#include "balancify_engine.hpp"

#include "common/hash_utils.hpp"

#include <boost/json.hpp>
#include <shared_mutex>
#include <chrono>
#include <iostream>

namespace balancify::lb {

using namespace balancify::common;

BalancifyEngine::BalancifyEngine(LoadBalancerConfig cfg)
    : config_(std::move(cfg)) {
    for (const auto& ep : config_.endpoints) {
        server_catalog_.emplace(ep.name, ep);
        server_connection_counts_[ep.name] = 0;
    }
    bloom_.configure(config_.bloom_threshold, config_.bloom_error_rate);
    rebuild_maglev();
    recompute_averages();
    
    // Start monitoring thread
    monitor_thread_ = std::thread(&BalancifyEngine::monitoring_thread, this);
}

BalancifyEngine::~BalancifyEngine() {
    stop_monitoring();
}

void BalancifyEngine::update_server_metrics(const std::string& name, double cpu, double ram, bool healthy) {
    std::unique_lock lock(mutex_);
    if (auto it = server_catalog_.find(name); it != server_catalog_.end()) {
        it->second.cpu_utilization = cpu;
        it->second.ram_utilization = ram;
        it->second.healthy = healthy;
    }
    rebuild_maglev();
    recompute_averages();
}

RoutingDecision BalancifyEngine::route(const ConnectionTuple& tuple) {
    RoutingDecision decision;
    const auto key = tuple.to_string();
    bool was_stateful_before = false;
    std::string previous_server;
    const ServerEndpoint* found_server = nullptr;

    {
        std::shared_lock lock(mutex_);
        found_server = select_stateful(key);
        if (found_server) {
            decision.endpoint = found_server;
            decision.was_stateful = true;
            // Check previous stats while holding shared lock
            if (auto stats_it = connection_stats_.find(key); stats_it != connection_stats_.end()) {
                was_stateful_before = stats_it->second.was_stateful;
                previous_server = stats_it->second.server_id;
            }
        }
    }
    
    if (found_server) {
        // Upgrade to unique lock for updates
        std::unique_lock lock(mutex_);
        ++stateful_routes_;
        // Update connection timestamp
        if (auto it = connection_table_.find(key); it != connection_table_.end()) {
            it->second.timestamp = std::chrono::steady_clock::now();
        }
        // Update connection stats
        if (auto stats_it = connection_stats_.find(key); stats_it != connection_stats_.end()) {
            stats_it->second.timestamp = std::chrono::steady_clock::now();
            stats_it->second.server_id = found_server->name;
            stats_it->second.was_stateful = true;
        } else {
            ConnectionStats stats;
            stats.timestamp = std::chrono::steady_clock::now();
            stats.server_id = found_server->name;
            stats.was_stateful = true;
            connection_stats_[key] = stats;
        }
        // Track routing changes
        if (!was_stateful_before || previous_server != found_server->name) {
            ++routing_changes_;
        }
        return decision;
    }

    std::unique_lock lock(mutex_);
    found_server = select_stateful(key);
    if (found_server) {
        decision.endpoint = const_cast<ServerEndpoint*>(found_server);
        decision.was_stateful = true;
        ++stateful_routes_;
        if (auto it = connection_table_.find(key); it != connection_table_.end()) {
            it->second.timestamp = std::chrono::steady_clock::now();
        }
        // Update connection stats
        if (auto stats_it = connection_stats_.find(key); stats_it != connection_stats_.end()) {
            was_stateful_before = stats_it->second.was_stateful;
            previous_server = stats_it->second.server_id;
            stats_it->second.timestamp = std::chrono::steady_clock::now();
            stats_it->second.server_id = found_server->name;
            stats_it->second.was_stateful = true;
        } else {
            ConnectionStats stats;
            stats.timestamp = std::chrono::steady_clock::now();
            stats.server_id = found_server->name;
            stats.was_stateful = true;
            connection_stats_[key] = stats;
        }
        if (!was_stateful_before || previous_server != found_server->name) {
            ++routing_changes_;
        }
        return decision;
    }

    const auto hash = stable_hash(tuple.to_string());
    const auto* selected = maglev_.select(hash);
    if (!selected) {
        decision.endpoint = nullptr;
        ++stateless_routes_;
        return decision;
    }

    auto& catalog_entry = server_catalog_.at(selected->name);
    if (catalog_entry.cpu_utilization - avg_cpu_ > config_.cpu_threshold) {
        // Find the least loaded server (same as Python implementation)
        const auto* alternative = find_least_loaded_server();
        if (alternative && alternative != selected) {
            double alt_cpu = server_catalog_.at(alternative->name).cpu_utilization;
            if (alt_cpu < catalog_entry.cpu_utilization * 0.7) {
                if (connection_table_.size() >= config_.max_stateful_entries) {
                    // remove oldest entry
                    if (!connection_table_.empty()) {
                        auto oldest = connection_table_.begin();
                        for (auto it = connection_table_.begin(); it != connection_table_.end(); ++it) {
                            if (it->second.timestamp < oldest->second.timestamp) {
                                oldest = it;
                            }
                        }
                        bloom_.remove(oldest->first);
                        if (oldest->second.server) {
                            --server_connection_counts_[oldest->second.server->name];
                        }
                        connection_table_.erase(oldest);
                    }
                }
                ConnectionInfo info;
                info.timestamp = std::chrono::steady_clock::now();
                info.server = &server_catalog_.at(alternative->name);
                connection_table_[key] = info;
                bloom_.add(key);
                ++server_connection_counts_[alternative->name];
                decision.endpoint = &server_catalog_.at(alternative->name);
                decision.was_stateful = true;
                ++stateful_routes_;
                
                // Track connection stats - transition to stateful
                was_stateful_before = false;
                previous_server.clear();
                if (auto stats_it = connection_stats_.find(key); stats_it != connection_stats_.end()) {
                    was_stateful_before = stats_it->second.was_stateful;
                    previous_server = stats_it->second.server_id;
                    stats_it->second.timestamp = std::chrono::steady_clock::now();
                    stats_it->second.server_id = alternative->name;
                    stats_it->second.was_stateful = true;
                } else {
                    ConnectionStats stats;
                    stats.timestamp = std::chrono::steady_clock::now();
                    stats.server_id = alternative->name;
                    stats.was_stateful = true;
                    connection_stats_[key] = stats;
                }
                // Track routing changes (stateless -> stateful or server change)
                if (!was_stateful_before || previous_server != alternative->name) {
                    ++routing_changes_;
                }
                
                return decision;
            }
        }
    }

    decision.endpoint = &catalog_entry;
    ++stateless_routes_;
    ++server_connection_counts_[selected->name];
    
    // Track connection stats - stateless routing
    was_stateful_before = false;
    previous_server.clear();
    if (auto stats_it = connection_stats_.find(key); stats_it != connection_stats_.end()) {
        was_stateful_before = stats_it->second.was_stateful;
        previous_server = stats_it->second.server_id;
        stats_it->second.timestamp = std::chrono::steady_clock::now();
        stats_it->second.server_id = selected->name;
        stats_it->second.was_stateful = false;
    } else {
        ConnectionStats stats;
        stats.timestamp = std::chrono::steady_clock::now();
        stats.server_id = selected->name;
        stats.was_stateful = false;
        connection_stats_[key] = stats;
    }
    // Track routing changes (stateful -> stateless or server change)
    if (was_stateful_before || (!previous_server.empty() && previous_server != selected->name)) {
        ++routing_changes_;
    }
    
    return decision;
}

void BalancifyEngine::end_connection(const std::string& key) {
    std::unique_lock lock(mutex_);
    if (auto it = connection_table_.find(key); it != connection_table_.end()) {
        if (it->second.server) {
            --server_connection_counts_[it->second.server->name];
        }
        bloom_.remove(key);
        connection_table_.erase(it);
    }
    // Remove connection stats
    connection_stats_.erase(key);
}

std::vector<ServerEndpoint> BalancifyEngine::snapshot_servers() const {
    std::shared_lock lock(mutex_);
    std::vector<ServerEndpoint> servers;
    servers.reserve(server_catalog_.size());
    for (const auto& [_, ep] : server_catalog_) {
        servers.push_back(ep);
    }
    return servers;
}

size_t BalancifyEngine::stateful_entry_count() const {
    std::shared_lock lock(mutex_);
    return connection_table_.size();
}

const ServerEndpoint* BalancifyEngine::select_stateful(const std::string& key) {
    if (!bloom_.contains(key)) {
        return nullptr;
    }
    if (auto it = connection_table_.find(key); it != connection_table_.end()) {
        if (it->second.server && it->second.server->healthy) {
            return it->second.server;
        }
    }
    return nullptr;
}

const ServerEndpoint* BalancifyEngine::find_least_loaded_server() const {
    // Find the least loaded healthy server (by CPU utilization)
    // Same as Python's _find_least_loaded_server() implementation
    const ServerEndpoint* least_loaded = nullptr;
    double min_cpu = std::numeric_limits<double>::max();
    
    for (const auto& [_, ep] : server_catalog_) {
        if (ep.healthy && ep.cpu_utilization < min_cpu) {
            min_cpu = ep.cpu_utilization;
            least_loaded = &ep;
        }
    }
    
    return least_loaded;
}

void BalancifyEngine::maybe_toggle_bloom() {
    if (connection_table_.size() >= config_.bloom_threshold) {
        bloom_.configure(std::max(connection_table_.size(), config_.bloom_threshold), config_.bloom_error_rate);
    }
}

void BalancifyEngine::rebuild_maglev() {
    std::vector<ServerEndpoint> healthy;
    for (const auto& [_, ep] : server_catalog_) {
        if (ep.healthy) {
            healthy.push_back(ep);
        }
    }
    maglev_.rebuild(healthy);
}

void BalancifyEngine::recompute_averages() {
    if (server_catalog_.empty()) {
        avg_cpu_ = avg_ram_ = 0.0;
        return;
    }
    double total_cpu = 0.0;
    double total_ram = 0.0;
    size_t healthy = 0;
    for (const auto& [_, ep] : server_catalog_) {
        if (ep.healthy) {
            total_cpu += ep.cpu_utilization;
            total_ram += ep.ram_utilization;
            ++healthy;
        }
    }
    if (healthy == 0) {
        avg_cpu_ = avg_ram_ = 0.0;
    } else {
        avg_cpu_ = total_cpu / healthy;
        avg_ram_ = total_ram / healthy;
    }
}

size_t BalancifyEngine::total_connections() const {
    std::shared_lock lock(mutex_);
    size_t total = 0;
    for (const auto& [_, count] : server_connection_counts_) {
        total += count;
    }
    return total;
}

size_t BalancifyEngine::get_server_connection_count(const std::string& name) const {
    std::shared_lock lock(mutex_);
    if (auto it = server_connection_counts_.find(name); it != server_connection_counts_.end()) {
        return it->second;
    }
    return 0;
}

double BalancifyEngine::get_load_score(const ServerEndpoint& ep) const {
    const double cpu_weight = 0.4;
    const double ram_weight = 0.3;
    const double connection_weight = 0.3;
    
    const size_t capacity = 100; // Default capacity
    const double normalized_connections = static_cast<double>(get_server_connection_count(ep.name)) / std::max(1.0, static_cast<double>(capacity));
    const double normalized_cpu = ep.cpu_utilization / 100.0;
    const double normalized_ram = std::min(1.0, ep.ram_utilization / 16.0);
    
    return cpu_weight * normalized_cpu + ram_weight * normalized_ram + connection_weight * normalized_connections;
}

void BalancifyEngine::cleanup_stale_connections() {
    std::unique_lock lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    const auto stale_timeout = std::chrono::minutes(5);
    
    std::vector<std::string> to_remove;
    for (const auto& [key, info] : connection_table_) {
        if (now - info.timestamp > stale_timeout) {
            to_remove.push_back(key);
        }
    }
    
    for (const auto& key : to_remove) {
        if (auto it = connection_table_.find(key); it != connection_table_.end()) {
            if (it->second.server) {
                --server_connection_counts_[it->second.server->name];
            }
            bloom_.remove(key);
            connection_table_.erase(it);
        }
        // Remove connection stats
        connection_stats_.erase(key);
    }
}

void BalancifyEngine::monitoring_thread() {
    while (monitoring_active_) {
        cleanup_stale_connections();
        std::this_thread::sleep_for(std::chrono::seconds(config_.monitoring_interval));
    }
}

void BalancifyEngine::stop_monitoring() {
    monitoring_active_ = false;
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
}

std::vector<ConnectionStats> BalancifyEngine::get_connection_stats() const {
    std::shared_lock lock(mutex_);
    std::vector<ConnectionStats> stats;
    stats.reserve(connection_stats_.size());
    for (const auto& [_, stat] : connection_stats_) {
        stats.push_back(stat);
    }
    return stats;
}

} // namespace balancify::lb

