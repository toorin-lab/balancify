#include "stats.hpp"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>

namespace balancify::lb {

Totals collect(const Runtime& rt) {
    Totals t;
    for (unsigned w = 0; w < rt.nworkers; ++w) {
        const Counters c = (*rt.stats)[w].read();
#define BALANCIFY_SUM(name) t.c.name += c.name;
        BALANCIFY_WORKER_COUNTERS(BALANCIFY_SUM)
#undef BALANCIFY_SUM
    }
    t.cycles_per_packet = t.c.busy_pkts ? static_cast<double>(t.c.busy_cycles) / t.c.busy_pkts : 0.0;
    t.override_fraction = t.c.new_conns ? static_cast<double>(t.c.overrides) / t.c.new_conns : 0.0;
    const uint64_t hashed_queries = t.c.bloom_negatives + t.c.bloom_false_positives;
    t.bloom_fp_rate = hashed_queries ? static_cast<double>(t.c.bloom_false_positives) / hashed_queries : 0.0;
    return t;
}

// --- Json ----------------------------------------------------------------------

void Json::separate() {
    if (after_key_) {
        after_key_ = false;
        return;
    }
    if (!first_.empty()) {
        if (!first_.back()) out_.push_back(',');
        first_.back() = false;
    }
}

void Json::escape(std::string_view s) {
    out_.push_back('"');
    for (char ch : s) {
        switch (ch) {
        case '"': out_ += "\\\""; break;
        case '\\': out_ += "\\\\"; break;
        case '\n': out_ += "\\n"; break;
        case '\r': out_ += "\\r"; break;
        case '\t': out_ += "\\t"; break;
        default:
            if (static_cast<unsigned char>(ch) < 0x20) {
                char buf[8];
                std::snprintf(buf, sizeof(buf), "\\u%04x", ch);
                out_ += buf;
            } else {
                out_.push_back(ch);
            }
        }
    }
    out_.push_back('"');
}

Json& Json::begin_object() { separate(); out_.push_back('{'); first_.push_back(true); return *this; }
Json& Json::end_object() { out_.push_back('}'); first_.pop_back(); return *this; }
Json& Json::begin_array() { separate(); out_.push_back('['); first_.push_back(true); return *this; }
Json& Json::end_array() { out_.push_back(']'); first_.pop_back(); return *this; }

Json& Json::key(std::string_view k) {
    separate();
    escape(k);
    out_.push_back(':');
    after_key_ = true;
    return *this;
}

Json& Json::value(std::string_view v) { separate(); escape(v); return *this; }

Json& Json::value(double v) {
    separate();
    if (!std::isfinite(v)) {
        out_ += "null";
    } else {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.6g", v);
        out_ += buf;
    }
    return *this;
}

Json& Json::value(uint64_t v) { separate(); out_ += std::to_string(v); return *this; }
Json& Json::value(int64_t v) { separate(); out_ += std::to_string(v); return *this; }
Json& Json::value(bool v) { separate(); out_ += v ? "true" : "false"; return *this; }

// --- renderers -----------------------------------------------------------------

std::string render_stats(const StatsSources& src) {
    const Runtime& rt = src.rt;
    const Totals t = collect(rt);
    Json j;
    j.begin_object();
    j.field("service", rt.cfg.service);
    j.field("instance", rt.cfg.instance_id);
    j.field("mode", to_string(rt.cfg.mode));
    j.field("ready", rt.ready.load());
    j.field("workers", rt.nworkers);

    j.key("params").begin_object();
    j.field("threshold", rt.threshold.load());
    j.field("sampling_interval_s", rt.sampling_interval_ms.load() / 1000.0);
    j.field("target_rule", to_string(static_cast<TargetRule>(rt.target_rule.load())));
    j.field("load_signal", to_string(rt.cfg.load_signal));
    j.field("hashing", to_string(rt.cfg.hashing));
    j.field("bloom", to_string(rt.cfg.bloom));
    j.field("cache_reserved_bytes", static_cast<uint64_t>(rt.cache_reserved_bytes));
    j.field("table_budget_bytes_per_worker", static_cast<uint64_t>(rt.table_budget_bytes));
    j.end_object();

    j.key("dataplane").begin_object();
#define BALANCIFY_JSON(name) j.field(#name, t.c.name);
    BALANCIFY_WORKER_COUNTERS(BALANCIFY_JSON)
#undef BALANCIFY_JSON
    j.field("cycles_per_packet", t.cycles_per_packet);
    j.field("override_fraction", t.override_fraction);
    j.field("bloom_fp_rate", t.bloom_fp_rate);
    j.end_object();

    j.key("workers_detail").begin_array();
    for (unsigned w = 0; w < rt.nworkers; ++w) {
        const Counters c = (*rt.stats)[w].read();
        j.begin_object();
        j.field("worker", w);
        j.field("lcore", rt.worker_lcores[w]);
        j.field("rx_pkts", c.rx_pkts);
        j.field("table_entries", c.table_entries);
        j.field("table_bytes", c.table_bytes);
        j.field("bloom_active", c.bloom_active != 0);
        j.field("bloom_bytes", c.bloom_bytes);
        j.field("cycles_per_packet", c.busy_pkts ? static_cast<double>(c.busy_cycles) / c.busy_pkts : 0.0);
        j.end_object();
    }
    j.end_array();

    j.key("monitor").begin_object();
    j.field("snapshots", src.monitor.snapshots());
    j.field("reports", src.monitor.reports());
    j.field("oscillations", src.monitor.oscillations());
    j.field("avg_load", src.monitor.last_avg());
    j.field("load_stddev", src.monitor.last_stddev());
    j.field("mu", src.monitor.last_mu());
    j.end_object();

    if (Coordinator* c = src.coordinator.load()) {
        const auto s = c->stats();
        j.key("coordinator").begin_object();
        j.field("owner", s.owner_id);
        j.field("connected", s.connected);
        j.field("leader", s.leader);
        j.field("synced", s.synced);
        j.field("sync_ms", s.sync_ms);
        j.field("creates", s.creates);
        j.field("create_errors", s.create_errors);
        j.field("deletes", s.deletes);
        j.field("adoptions", s.adoptions);
        j.field("held", s.held);
        j.field("released", s.released);
        j.field("timeouts", s.timeouts);
        j.field("fail_open", s.fail_open);
        j.field("gc_deletes", s.gc_deletes);
        j.field("session_restarts", s.session_restarts);
        j.field("bucket_publishes", s.bucket_publishes);
        j.field("bindings", s.bindings);
        j.field("orphans", s.orphans);
        j.field("live_instances", s.live_instances);
        j.end_object();
    }
    j.end_object();
    return j.str();
}

std::string render_dips(const StatsSources& src) {
    const auto counts = src.hpm.bucket_counts();
    const auto [version, buckets] = src.hpm.current();
    Json j;
    j.begin_object();
    j.field("bucket_table_version", version);
    j.field("buckets", static_cast<uint64_t>(buckets.size()));
    j.key("dips").begin_array();
    for (const auto& v : src.monitor.view()) {
        const DipSlot& d = (*src.rt.pool)[v.index];
        j.begin_object();
        j.field("index", static_cast<unsigned>(v.index));
        j.field("name", std::string(d.name));
        j.field("ip", ip_to_string(d.ip_be));
        j.field("weight", d.weight);
        j.field("admin_enabled", v.admin_enabled);
        j.field("healthy", v.healthy);
        j.field("eligible", v.eligible);
        j.field("sampled_load", v.sampled_load);
        j.field("cpu", v.cpu);
        j.field("latency_ms", v.latency_ms);
        j.field("active_conns", v.active_conns);
        j.field("report_age_ms", v.report_age_ms);
        j.field("reports", v.reports);
        j.field("buckets", counts[v.index]);
        j.end_object();
    }
    j.end_array();
    j.end_object();
    return j.str();
}

// --- StatsLogger ---------------------------------------------------------------

StatsLogger::StatsLogger(StatsSources src) : src_(src) {}

StatsLogger::~StatsLogger() {
    stop();
}

void StatsLogger::start() {
    if (src_.rt.cfg.stats_log.empty() || running_.exchange(true)) return;
    thread_ = std::thread(&StatsLogger::run, this);
}

void StatsLogger::stop() {
    running_.store(false);
    if (thread_.joinable()) thread_.join();
}

void StatsLogger::run() {
    std::ofstream out(src_.rt.cfg.stats_log, std::ios::app);
    if (!out) {
        std::cerr << "[stats] cannot open " << src_.rt.cfg.stats_log << std::endl;
        return;
    }
    out << "unix_ms,rx_pps,tx_pps,new_conn_rate,override_rate,override_fraction,table_entries,table_bytes,"
           "bloom_active_workers,bloom_bytes,bloom_fp_rate,cycles_per_packet,store_create_rate,store_delete_rate,"
           "bindings,avg_load,load_stddev,oscillations\n";
    const auto interval = std::chrono::milliseconds(src_.rt.cfg.stats_log_interval_ms);
    Totals prev = collect(src_.rt);
    Coordinator::Stats prev_store{};
    if (Coordinator* c = src_.coordinator.load()) prev_store = c->stats();
    auto last = std::chrono::steady_clock::now();
    while (running_.load()) {
        std::this_thread::sleep_for(interval);
        const auto now = std::chrono::steady_clock::now();
        const double dt = std::chrono::duration<double>(now - last).count();
        last = now;
        const Totals t = collect(src_.rt);
        Coordinator::Stats store{};
        if (Coordinator* c = src_.coordinator.load()) store = c->stats();

        const uint64_t new_conns = t.c.new_conns - prev.c.new_conns;
        const uint64_t overrides = t.c.overrides - prev.c.overrides;
        const uint64_t busy_pkts = t.c.busy_pkts - prev.c.busy_pkts;
        const uint64_t busy_cycles = t.c.busy_cycles - prev.c.busy_cycles;
        const uint64_t fp_q = (t.c.bloom_negatives + t.c.bloom_false_positives) -
                              (prev.c.bloom_negatives + prev.c.bloom_false_positives);
        const uint64_t fp = t.c.bloom_false_positives - prev.c.bloom_false_positives;
        const auto unix_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        char line[512];
        std::snprintf(line, sizeof(line),
                      "%lld,%.0f,%.0f,%.1f,%.1f,%.4f,%llu,%llu,%llu,%llu,%.5f,%.1f,%.1f,%.1f,%llu,%.3f,%.3f,%llu\n",
                      static_cast<long long>(unix_ms), (t.c.rx_pkts - prev.c.rx_pkts) / dt,
                      (t.c.tx_pkts - prev.c.tx_pkts) / dt, new_conns / dt, overrides / dt,
                      new_conns ? static_cast<double>(overrides) / new_conns : 0.0,
                      static_cast<unsigned long long>(t.c.table_entries),
                      static_cast<unsigned long long>(t.c.table_bytes),
                      static_cast<unsigned long long>(t.c.bloom_active),
                      static_cast<unsigned long long>(t.c.bloom_bytes), fp_q ? static_cast<double>(fp) / fp_q : 0.0,
                      busy_pkts ? static_cast<double>(busy_cycles) / busy_pkts : 0.0,
                      (store.creates - prev_store.creates) / dt, (store.deletes - prev_store.deletes) / dt,
                      static_cast<unsigned long long>(store.bindings), src_.monitor.last_avg(),
                      src_.monitor.last_stddev(), static_cast<unsigned long long>(src_.monitor.oscillations()));
        out << line;
        out.flush();
        prev = t;
        prev_store = store;
    }
}

} // namespace balancify::lb
