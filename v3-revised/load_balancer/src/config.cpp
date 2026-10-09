#include "config.hpp"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace balancify::lb {

namespace {

std::string trim(const std::string& s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    std::istringstream in(s);
    while (std::getline(in, cur, sep)) {
        cur = trim(cur);
        if (!cur.empty()) out.push_back(cur);
    }
    return out;
}

std::vector<std::string> split_ws(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream in(s);
    std::string tok;
    while (in >> tok) out.push_back(tok);
    return out;
}

[[noreturn]] void bad(const std::string& key, const std::string& value) {
    throw std::runtime_error("invalid value for '" + key + "': '" + value + "'");
}

double to_double(const std::string& key, const std::string& v) {
    try {
        size_t pos = 0;
        const double d = std::stod(v, &pos);
        if (pos != v.size()) bad(key, v);
        return d;
    } catch (const std::invalid_argument&) {
        bad(key, v);
    } catch (const std::out_of_range&) {
        bad(key, v);
    }
}

uint64_t to_uint(const std::string& key, const std::string& v) {
    try {
        size_t pos = 0;
        const unsigned long long u = std::stoull(v, &pos, 0);
        if (pos != v.size()) bad(key, v);
        return u;
    } catch (const std::invalid_argument&) {
        bad(key, v);
    } catch (const std::out_of_range&) {
        bad(key, v);
    }
}

bool to_bool(const std::string& key, const std::string& v) {
    const auto l = lower(v);
    if (l == "1" || l == "true" || l == "yes" || l == "on") return true;
    if (l == "0" || l == "false" || l == "no" || l == "off") return false;
    bad(key, v);
}

uint32_t to_ip(const std::string& key, const std::string& v) {
    uint32_t ip = 0;
    if (!parse_ipv4(v, ip)) bad(key, v);
    return ip;
}

DipConfig parse_dip(const std::string& value) {
    // dip = <name> <ip> [<mac>|-] [<weight>]
    const auto f = split_ws(value);
    if (f.size() < 2) bad("dip", value);
    DipConfig d;
    d.name = f[0];
    d.ip_be = to_ip("dip", f[1]);
    if (f.size() >= 3 && f[2] != "-") {
        if (!parse_mac(f[2], d.mac)) bad("dip", value);
        d.has_mac = true;
    }
    if (f.size() >= 4) {
        d.weight = static_cast<uint32_t>(to_uint("dip", f[3]));
    }
    return d;
}

} // namespace

bool parse_mac(const std::string& text, std::array<uint8_t, 6>& mac) {
    unsigned v[6];
    char tail = 0;
    if (std::sscanf(text.c_str(), "%x:%x:%x:%x:%x:%x%c", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &tail) != 6) {
        return false;
    }
    for (int i = 0; i < 6; ++i) {
        if (v[i] > 0xff) return false;
        mac[i] = static_cast<uint8_t>(v[i]);
    }
    return true;
}

bool parse_mode(const std::string& text, Mode& out) {
    const auto l = lower(text);
    if (l == "balancify") out = Mode::Balancify;
    else if (l == "all_stateful" || l == "allstateful" || l == "stateful") out = Mode::AllStateful;
    else if (l == "stateless") out = Mode::Stateless;
    else return false;
    return true;
}

bool parse_target_rule(const std::string& text, TargetRule& out) {
    const auto l = lower(text);
    if (l == "r0" || l == "argmin") out = TargetRule::ArgMin;
    else if (l == "r1" || l == "increment") out = TargetRule::IncrementTarget;
    else if (l == "r2" || l == "below_average") out = TargetRule::RandomBelowAverage;
    else return false;
    return true;
}

void apply_setting(Config& c, const std::string& key_in, const std::string& value) {
    const auto key = lower(trim(key_in));
    const auto v = trim(value);

    if (key == "service") c.service = v;
    else if (key == "instance_id") c.instance_id = v;
    else if (key == "mode") { if (!parse_mode(v, c.mode)) bad(key, v); }
    else if (key == "vip") c.vip_be = to_ip(key, v);
    else if (key == "vip_ports") {
        c.vip_ports.clear();
        for (const auto& p : split(v, ',')) {
            const auto port = to_uint(key, p);
            if (port == 0 || port > 65535) bad(key, v);
            c.vip_ports.push_back(static_cast<uint16_t>(port));
        }
    }
    else if (key == "local_ip") c.local_ip_be = to_ip(key, v);
    else if (key == "gateway_mac") { if (!parse_mac(v, c.gateway_mac)) bad(key, v); }
    else if (key == "port_id") c.port_id = static_cast<uint16_t>(to_uint(key, v));
    else if (key == "workers") c.workers = static_cast<unsigned>(to_uint(key, v));
    else if (key == "rx_desc") c.rx_desc = static_cast<unsigned>(to_uint(key, v));
    else if (key == "tx_desc") c.tx_desc = static_cast<unsigned>(to_uint(key, v));
    else if (key == "mbufs_per_worker") c.mbufs_per_worker = static_cast<unsigned>(to_uint(key, v));
    else if (key == "hashing") {
        const auto l = lower(v);
        if (l == "stable") c.hashing = HashingScheme::Stable;
        else if (l == "maglev") c.hashing = HashingScheme::Maglev;
        else bad(key, v);
    }
    else if (key == "buckets") c.buckets = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "maglev_table_size") c.maglev_table_size = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "threshold") c.threshold = to_double(key, v);
    else if (key == "load_signal") {
        const auto l = lower(v);
        if (l == "cpu") c.load_signal = LoadSignal::Cpu;
        else if (l == "latency") c.load_signal = LoadSignal::Latency;
        else bad(key, v);
    }
    else if (key == "sampling_interval") c.sampling_interval_s = to_double(key, v);
    else if (key == "target_rule") { if (!parse_target_rule(v, c.target_rule)) bad(key, v); }
    else if (key == "r1_increment") c.r1_increment = to_double(key, v);
    else if (key == "load_aggregation") {
        const auto l = lower(v);
        if (l == "mean") c.load_mean_over_interval = true;
        else if (l == "last") c.load_mean_over_interval = false;
        else bad(key, v);
    }
    else if (key == "health_timeout_ms") c.health_timeout_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "health_requires_agent") c.health_requires_agent = to_bool(key, v);
    else if (key == "agent_bind") c.agent_bind = v;
    else if (key == "agent_port") c.agent_port = static_cast<uint16_t>(to_uint(key, v));
    else if (key == "idle_timeout_s") c.idle_timeout_s = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "fin_linger_ms") c.fin_linger_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "table_cache_share") c.table_cache_share = to_double(key, v);
    else if (key == "table_budget_mb") c.table_budget_mb = to_double(key, v);
    else if (key == "cat_partition_mb") c.cat_partition_mb = to_double(key, v);
    else if (key == "cat_cos") c.cat_cos = static_cast<unsigned>(to_uint(key, v));
    else if (key == "cat_way_offset") c.cat_way_offset = static_cast<unsigned>(to_uint(key, v));
    else if (key == "cat_isolate_others") c.cat_isolate_others = to_bool(key, v);
    else if (key == "cat_interface") {
        const auto l = lower(v);
        if (l != "os" && l != "msr") bad(key, v);
        c.cat_interface = l;
    }
    else if (key == "bloom") {
        const auto l = lower(v);
        if (l == "auto") c.bloom = BloomPolicy::Auto;
        else if (l == "on") c.bloom = BloomPolicy::On;
        else if (l == "off") c.bloom = BloomPolicy::Off;
        else bad(key, v);
    }
    else if (key == "bloom_fp_rate") c.bloom_fp_rate = to_double(key, v);
    else if (key == "zk_hosts") c.zk_hosts = v;
    else if (key == "zk_root") c.zk_root = v;
    else if (key == "zk_session_timeout_ms") c.zk_session_timeout_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "zk_write_timeout_ms") c.zk_write_timeout_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "zk_sync_timeout_ms") c.zk_sync_timeout_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "zk_orphan_timeout_s") c.zk_orphan_timeout_s = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "announce_cmd") c.announce_cmd = v;
    else if (key == "withdraw_cmd") c.withdraw_cmd = v;
    else if (key == "shutdown_grace_ms") c.shutdown_grace_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "control_bind") c.control_bind = v;
    else if (key == "control_port") c.control_port = static_cast<uint16_t>(to_uint(key, v));
    else if (key == "stats_log") c.stats_log = v;
    else if (key == "stats_log_interval_ms") c.stats_log_interval_ms = static_cast<uint32_t>(to_uint(key, v));
    else if (key == "dip") c.dips.push_back(parse_dip(v));
    else throw std::runtime_error("unknown configuration key '" + key + "'");
}

Config load_config(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("cannot open configuration file " + path);
    }
    Config cfg;
    std::string line;
    unsigned lineno = 0;
    while (std::getline(in, line)) {
        ++lineno;
        if (const auto hash = line.find('#'); hash != std::string::npos) {
            line.erase(hash);
        }
        line = trim(line);
        if (line.empty()) continue;
        const auto eq = line.find('=');
        if (eq == std::string::npos) {
            throw std::runtime_error(path + ":" + std::to_string(lineno) + ": expected key = value");
        }
        try {
            apply_setting(cfg, line.substr(0, eq), line.substr(eq + 1));
        } catch (const std::exception& ex) {
            throw std::runtime_error(path + ":" + std::to_string(lineno) + ": " + ex.what());
        }
    }
    return cfg;
}

void validate(const Config& c) {
    auto fail = [](const std::string& m) { throw std::runtime_error("configuration: " + m); };
    if (c.vip_be == 0) fail("vip is required");
    if (c.local_ip_be == 0) fail("local_ip is required");
    if (c.dips.empty()) fail("at least one dip is required");
    if (c.dips.size() > kMaxDips) fail("too many dips");
    if (c.buckets == 0) fail("buckets must be positive");
    if (c.maglev_table_size < 3) fail("maglev_table_size too small");
    if (c.sampling_interval_s <= 0.0) fail("sampling_interval must be positive");
    if (c.threshold < 0.0) fail("threshold must be non-negative");
    if (c.bloom_fp_rate <= 0.0 || c.bloom_fp_rate >= 1.0) fail("bloom_fp_rate must be in (0,1)");
    if (c.table_cache_share <= 0.0 || c.table_cache_share > 1.0) fail("table_cache_share must be in (0,1]");
    if (c.idle_timeout_s == 0) fail("idle_timeout_s must be positive");
    for (char ch : c.instance_id) {
        if (ch == '/' || ch == '.' || std::isspace(static_cast<unsigned char>(ch))) {
            fail("instance_id must not contain '/', '.' or spaces");
        }
    }
}

} // namespace balancify::lb
