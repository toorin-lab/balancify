#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace balancify::common {

struct ConnectionTuple {
    std::array<uint8_t, 4> src_ip{};
    std::array<uint8_t, 4> dst_ip{};
    uint16_t src_port{0};
    uint16_t dst_port{0};
    uint8_t protocol{0}; // TCP=6, UDP=17

    [[nodiscard]] std::string to_string() const;
};

struct ServerEndpoint {
    std::string name;
    std::array<uint8_t, 6> mac{};
    std::array<uint8_t, 4> ip{};
    uint16_t port{0};
    double cpu_utilization{0.0};
    double ram_utilization{0.0};
    bool healthy{true};
};

struct LoadBalancerConfig {
    std::vector<ServerEndpoint> endpoints;
    double cpu_threshold{20.0};
    double ram_threshold{2.0};
    size_t max_stateful_entries{1000};
    size_t bloom_threshold{500};
    double bloom_error_rate{0.01};
    size_t monitoring_interval{10}; // seconds
};

struct RoutingDecision {
    const ServerEndpoint* endpoint{nullptr};
    bool was_stateful{false};
};

} // namespace balancify::common

