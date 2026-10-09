#include "cache_allocation.hpp"
#include "config.hpp"
#include "control_api.hpp"
#include "coordinator.hpp"
#include "dataplane.hpp"
#include "hash_path.hpp"
#include "load_monitor.hpp"
#include "runtime.hpp"
#include "stats.hpp"

#include <rte_eal.h>
#include <rte_lcore.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace balancify::lb;

namespace {

std::atomic<bool>* g_shutdown = nullptr;

void on_signal(int) {
    if (g_shutdown != nullptr) g_shutdown->store(true);
}

struct AppOptions {
    std::string config_path;
    std::vector<std::pair<std::string, std::string>> overrides;
};

void usage(const char* prog) {
    std::cerr << "usage: " << prog << " [EAL options] -- --config <file> [--set key=value]...\n";
}

AppOptions parse_app_args(int argc, char** argv) {
    AppOptions o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if ((a == "--config" || a == "-c") && i + 1 < argc) {
            o.config_path = argv[++i];
        } else if (a == "--set" && i + 1 < argc) {
            const std::string kv = argv[++i];
            const auto eq = kv.find('=');
            if (eq == std::string::npos) throw std::runtime_error("--set expects key=value");
            o.overrides.emplace_back(kv.substr(0, eq), kv.substr(eq + 1));
        } else {
            throw std::runtime_error("unknown argument " + a);
        }
    }
    if (o.config_path.empty()) throw std::runtime_error("--config is required");
    return o;
}

void run_hook(const std::string& cmd, const char* what) {
    if (cmd.empty()) return;
    const int rc = std::system(cmd.c_str());
    std::cout << "[main] " << what << " hook exited with " << rc << std::endl;
}

} // namespace

int main(int argc, char** argv) {
    const int eal_args = rte_eal_init(argc, argv);
    if (eal_args < 0) {
        std::cerr << "EAL initialization failed" << std::endl;
        return EXIT_FAILURE;
    }
    argc -= eal_args;
    argv += eal_args;

    int exit_code = EXIT_SUCCESS;
    try {
        const AppOptions opts = parse_app_args(argc, argv);
        Config cfg = load_config(opts.config_path);
        for (const auto& [k, v] : opts.overrides) apply_setting(cfg, k, v);
        validate(cfg);

        Runtime rt(std::move(cfg));
        rt.init_workers_from_eal();
        rt.init_rcu();
        g_shutdown = &rt.shutdown_requested;
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);

        Dataplane dp(rt);
        dp.init();

        CacheAllocation cat;
        std::vector<unsigned> cpus;
        for (unsigned w = 0; w < rt.nworkers; ++w) cpus.push_back(rte_lcore_to_cpu_id(static_cast<int>(rt.worker_lcores[w])));
        const size_t reserved = cat.apply(rt.cfg.cat_partition_mb, rt.cfg.cat_cos, rt.cfg.cat_way_offset,
                                          rt.cfg.cat_isolate_others, rt.cfg.cat_interface, cpus);
        rt.compute_table_budget(reserved, CacheAllocation::llc_size_bytes());

        HashPathManager hpm(rt);
        std::atomic<Coordinator*> coordinator{nullptr};

        // Section III-B: a DIP that leaves the pool loses its table entries on
        // every core and is removed from the consistent-hashing set; a DIP that
        // joins only enters the hashing set.
        LoadMonitor monitor(rt, [&](const std::vector<uint16_t>& eligible, const std::vector<uint16_t>& removed,
                                    const std::vector<uint16_t>& added) {
            for (uint16_t d : removed) {
                CoreMsg m{};
                m.type = CoreMsgType::PurgeDip;
                m.dip = d;
                rt.broadcast(m);
                std::cout << "[pool] " << (*rt.pool)[d].name << " left the pool" << std::endl;
            }
            for (uint16_t d : added) {
                std::cout << "[pool] " << (*rt.pool)[d].name << " joined the pool" << std::endl;
            }
            Coordinator* c = coordinator.load();
            if (rt.cfg.hashing == HashingScheme::Maglev || c == nullptr) {
                hpm.rebuild(eligible);
            } else if (c->leader()) {
                hpm.rebuild(eligible);
                c->publish_buckets();
            }
            if (c != nullptr) c->on_dips_removed(removed);
        });
        monitor.prime();
        hpm.rebuild(monitor.eligible());

        rt.zk_enabled.store(!rt.cfg.zk_hosts.empty());
        dp.launch();
        monitor.start();

        std::unique_ptr<Coordinator> coord;
        if (rt.zk_enabled.load()) {
            coord = std::make_unique<Coordinator>(rt, hpm, [&monitor] { return monitor.eligible(); });
            if (!coord->start()) {
                throw std::runtime_error("cannot synchronize with the coordination store");
            }
            coordinator.store(coord.get());
        }

        StatsSources sources{rt, monitor, hpm, coordinator};
        ControlApi api(sources);
        api.start();
        StatsLogger logger(sources);
        logger.start();

        rt.ready.store(true);
        std::cout << "[main] balancify (" << to_string(rt.cfg.mode) << ", " << rt.nworkers << " cores, "
                  << rt.pool->size() << " DIPs, table budget " << rt.table_budget_bytes / 1024
                  << " KB/core) is ready" << std::endl;
        // Only now, with the local copy synchronized, is the VIP announced to ECMP.
        run_hook(rt.cfg.announce_cmd, "announce");

        while (!rt.shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        std::cout << "[main] shutting down" << std::endl;
        run_hook(rt.cfg.withdraw_cmd, "withdraw");
        std::this_thread::sleep_for(std::chrono::milliseconds(rt.cfg.shutdown_grace_ms));
        rt.ready.store(false);
        logger.stop();
        api.stop();
        dp.stop();
        coordinator.store(nullptr);
        if (coord) coord->stop();
        monitor.stop();
        cat.release();
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << std::endl;
        usage("balancify_lb");
        exit_code = EXIT_FAILURE;
    }
    rte_eal_cleanup();
    return exit_code;
}
