#pragma once

#include "coordinator.hpp"
#include "hash_path.hpp"
#include "load_monitor.hpp"
#include "runtime.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace balancify::lb {

struct Totals {
    Counters c;
    double cycles_per_packet{0.0};
    double override_fraction{0.0};
    double bloom_fp_rate{0.0};
};

Totals collect(const Runtime& rt);

struct StatsSources {
    Runtime& rt;
    LoadMonitor& monitor;
    HashPathManager& hpm;
    std::atomic<Coordinator*>& coordinator;
};

std::string render_stats(const StatsSources& src);
std::string render_dips(const StatsSources& src);

// Minimal JSON writer for the control API.
class Json {
public:
    Json& begin_object();
    Json& end_object();
    Json& begin_array();
    Json& end_array();
    Json& key(std::string_view k);
    Json& value(std::string_view v);
    Json& value(const char* v) { return value(std::string_view(v)); }
    Json& value(const std::string& v) { return value(std::string_view(v)); }
    Json& value(double v);
    Json& value(uint64_t v);
    Json& value(int64_t v);
    Json& value(unsigned v) { return value(static_cast<uint64_t>(v)); }
    Json& value(int v) { return value(static_cast<int64_t>(v)); }
    Json& value(bool v);
    template <class T>
    Json& field(std::string_view k, const T& v) { return key(k).value(v); }
    const std::string& str() const { return out_; }

private:
    void separate();
    void escape(std::string_view s);
    std::string out_;
    std::vector<bool> first_;
    bool after_key_{false};
};

// Appends one CSV line per interval: the time series behind table-size,
// override-rate, coordination-write-rate and load-spread plots.
class StatsLogger {
public:
    explicit StatsLogger(StatsSources src);
    ~StatsLogger();
    void start();
    void stop();

private:
    void run();
    StatsSources src_;
    std::atomic<bool> running_{false};
    std::thread thread_;
};

} // namespace balancify::lb
