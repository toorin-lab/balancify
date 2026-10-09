#include "runtime.hpp"

#include <rte_common.h>
#include <rte_lcore.h>
#include <rte_pause.h>

#include <algorithm>
#include <cmath>
#include <new>
#include <stdexcept>

namespace balancify::lb {

Runtime::Runtime(Config c)
    : cfg(std::move(c)),
      threshold(cfg.threshold),
      sampling_interval_ms(static_cast<uint32_t>(std::lround(cfg.sampling_interval_s * 1000.0))),
      target_rule(static_cast<uint8_t>(cfg.target_rule)),
      stats(std::make_unique<std::array<WorkerStats, kMaxWorkers>>()) {
    rte_ether_addr gw{};
    std::copy(cfg.gateway_mac.begin(), cfg.gateway_mac.end(), gw.addr_bytes);
    pool = std::make_unique<DipPool>(gw);
    for (const auto& d : cfg.dips) {
        rte_ether_addr mac{};
        if (d.has_mac) std::copy(d.mac.begin(), d.mac.end(), mac.addr_bytes);
        pool->add(d.name, d.ip_be, d.has_mac ? &mac : nullptr, d.weight);
    }

    all_ports = cfg.vip_ports.empty();
    for (uint16_t p : cfg.vip_ports) {
        allowed_ports.set(p);
    }

    void* mem = rte_zmalloc("bfy_pending", sizeof(PendingLoad), RTE_CACHE_LINE_SIZE);
    if (mem == nullptr) {
        throw std::runtime_error("cannot allocate pending-load vector");
    }
    pending = new (mem) PendingLoad();
}

Runtime::~Runtime() {
    rte_free(hash_path.exchange(nullptr));
    rte_free(load.exchange(nullptr));
    if (pending != nullptr) {
        pending->~PendingLoad();
        rte_free(pending);
    }
    rte_free(qsbr);
}

void Runtime::init_workers_from_eal() {
    unsigned lcore;
    nworkers = 0;
    RTE_LCORE_FOREACH_WORKER(lcore) {
        if (nworkers == kMaxWorkers) break;
        if (cfg.workers != 0 && nworkers == cfg.workers) break;
        worker_lcores[nworkers++] = lcore;
    }
    if (nworkers == 0) {
        throw std::runtime_error("no worker lcores: pass at least two lcores to the EAL (-l)");
    }
}

void Runtime::init_rcu() {
    const size_t sz = rte_rcu_qsbr_get_memsize(nworkers);
    qsbr = static_cast<rte_rcu_qsbr*>(rte_zmalloc("bfy_qsbr", sz, RTE_CACHE_LINE_SIZE));
    if (qsbr == nullptr || rte_rcu_qsbr_init(qsbr, nworkers) != 0) {
        throw std::runtime_error("cannot initialize RCU");
    }
}

void Runtime::compute_table_budget(size_t reserved_bytes, size_t llc_bytes) {
    cache_reserved_bytes = reserved_bytes;
    size_t cache = reserved_bytes;
    if (cache == 0) {
        cache = cfg.table_budget_mb > 0.0 ? static_cast<size_t>(cfg.table_budget_mb * 1024.0 * 1024.0) : llc_bytes;
    }
    table_budget_bytes = static_cast<size_t>(static_cast<double>(cache) * cfg.table_cache_share) / nworkers;
}

bool Runtime::send_to_worker(unsigned worker, const CoreMsg& msg) {
    if (worker >= nworkers || ctrl_rings[worker] == nullptr) {
        return false;
    }
    for (unsigned attempt = 0; attempt < 100000; ++attempt) {
        if (rte_ring_mp_enqueue_elem(ctrl_rings[worker], &msg, sizeof(CoreMsg)) == 0) {
            return true;
        }
        if (stop.load(std::memory_order_relaxed)) {
            return false;
        }
        rte_pause();
    }
    return false;
}

void Runtime::send_to_owner(const CoreMsg& msg) {
    send_to_worker(owner_worker ? owner_worker(msg.tuple) : 0, msg);
}

void Runtime::broadcast(const CoreMsg& msg) {
    for (unsigned w = 0; w < nworkers; ++w) {
        send_to_worker(w, msg);
    }
}

} // namespace balancify::lb
