#pragma once

#include "common/bloom_filter.hpp"
#include "common/maglev.hpp"
#include "common/types.hpp"

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

namespace balancify::lb {

struct ConnectionInfo {
    std::chrono::steady_clock::time_point timestamp;
    const common::ServerEndpoint* server;
};

struct ConnectionStats {
    std::chrono::steady_clock::time_point timestamp;
    std::string server_id;
    bool was_stateful;
};

class BalancifyEngine {
public:
    explicit BalancifyEngine(common::LoadBalancerConfig cfg);
    ~BalancifyEngine();

    void update_server_metrics(const std::string& name, double cpu, double ram, bool healthy);
    common::RoutingDecision route(const common::ConnectionTuple& tuple);
    void end_connection(const std::string& key);

    [[nodiscard]] common::LoadBalancerConfig config() const { return config_; }
    [[nodiscard]] std::vector<common::ServerEndpoint> snapshot_servers() const;
    [[nodiscard]] size_t stateful_entry_count() const;
    [[nodiscard]] double avg_cpu() const { return avg_cpu_; }
    [[nodiscard]] double avg_ram() const { return avg_ram_; }
    [[nodiscard]] size_t stateless_routes() const { return stateless_routes_; }
    [[nodiscard]] size_t stateful_routes() const { return stateful_routes_; }
    [[nodiscard]] size_t total_connections() const;
    [[nodiscard]] size_t get_server_connection_count(const std::string& name) const;
    [[nodiscard]] std::vector<ConnectionStats> get_connection_stats() const;
    [[nodiscard]] size_t routing_changes() const { return routing_changes_; }

    void stop_monitoring();

private:
    void maybe_toggle_bloom();
    const common::ServerEndpoint* select_stateful(const std::string& key);
    const common::ServerEndpoint* find_least_loaded_server() const;
    void rebuild_maglev();
    void recompute_averages();
    void monitoring_thread();
    void cleanup_stale_connections();
    double get_load_score(const common::ServerEndpoint& ep) const;

    common::LoadBalancerConfig config_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, ConnectionInfo> connection_table_;
    std::unordered_map<std::string, common::ServerEndpoint> server_catalog_;
    std::unordered_map<std::string, size_t> server_connection_counts_;
    std::unordered_map<std::string, ConnectionStats> connection_stats_;  // Track connection statistics
    common::CountingBloomFilter bloom_;
    common::MaglevHash maglev_;

    double avg_cpu_{0.0};
    double avg_ram_{0.0};

    size_t stateless_routes_{0};
    size_t stateful_routes_{0};
    size_t routing_changes_{0};

    std::thread monitor_thread_;
    bool monitoring_active_{true};
};

} // namespace balancify::lb

