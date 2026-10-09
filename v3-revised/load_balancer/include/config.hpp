#pragma once

#include "types.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace balancify::lb {

struct DipConfig {
    std::string name;
    uint32_t ip_be{0};
    bool has_mac{false};
    std::array<uint8_t, 6> mac{};
    uint32_t weight{1};
};

struct Config {
    // Identity
    std::string service{"default"};
    std::string instance_id{"lb"};

    // Forwarding
    Mode mode{Mode::Balancify};
    uint32_t vip_be{0};
    std::vector<uint16_t> vip_ports;  // host order; empty means every port
    uint32_t local_ip_be{0};          // outer source address of IP-in-IP, answered in ARP
    std::array<uint8_t, 6> gateway_mac{};
    uint16_t port_id{0};
    unsigned workers{0};              // 0: one per EAL worker lcore
    unsigned rx_desc{1024};
    unsigned tx_desc{1024};
    unsigned mbufs_per_worker{16384};

    // Consistent hashing
    HashingScheme hashing{HashingScheme::Stable};
    uint32_t buckets{65536};          // stable hashing; Maglev uses maglev_table_size
    uint32_t maglev_table_size{65537};

    // Override rule and monitoring
    double threshold{20.0};           // percentage points (cpu) or milliseconds (latency)
    LoadSignal load_signal{LoadSignal::Cpu};
    double sampling_interval_s{10.0}; // tau
    TargetRule target_rule{TargetRule::IncrementTarget};
    double r1_increment{0.0};         // 0: estimate from agent reports
    bool load_mean_over_interval{true};
    uint32_t health_timeout_ms{1000};
    bool health_requires_agent{true};
    std::string agent_bind{"0.0.0.0"};
    uint16_t agent_port{7001};

    // Connection-to-DIP table
    uint32_t idle_timeout_s{300};
    uint32_t fin_linger_ms{2000};
    double table_cache_share{0.75};   // share of the cache budget given to the tables
    double table_budget_mb{0.0};      // budget without CAT; 0: LLC size

    // Intel CAT
    double cat_partition_mb{8.0};     // 0: no partition
    unsigned cat_cos{1};
    unsigned cat_way_offset{0};
    bool cat_isolate_others{false};
    std::string cat_interface{"os"};  // os (resctrl) or msr

    // Bloom filter
    BloomPolicy bloom{BloomPolicy::Auto};
    double bloom_fp_rate{0.01};

    // Coordination store
    std::string zk_hosts;             // empty: single instance, no sharing
    std::string zk_root{"/balancify"};
    uint32_t zk_session_timeout_ms{6000};
    uint32_t zk_write_timeout_ms{50};
    uint32_t zk_sync_timeout_ms{30000};
    uint32_t zk_orphan_timeout_s{0};  // 0: idle_timeout_s

    // Operations
    std::string announce_cmd;         // run once the local copy is synchronized
    std::string withdraw_cmd;         // run before shutdown
    uint32_t shutdown_grace_ms{2000};
    std::string control_bind{"127.0.0.1"};
    uint16_t control_port{8080};
    std::string stats_log;
    uint32_t stats_log_interval_ms{1000};

    std::vector<DipConfig> dips;
};

Config load_config(const std::string& path);
void apply_setting(Config& cfg, const std::string& key, const std::string& value);
void validate(const Config& cfg);

bool parse_mac(const std::string& text, std::array<uint8_t, 6>& mac);
bool parse_mode(const std::string& text, Mode& out);
bool parse_target_rule(const std::string& text, TargetRule& out);

} // namespace balancify::lb
