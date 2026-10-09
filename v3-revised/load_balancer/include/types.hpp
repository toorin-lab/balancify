#pragma once

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace balancify::lb {

inline constexpr uint16_t kMaxDips = 1024;
inline constexpr uint16_t kInvalidDip = 0xffff;
inline constexpr unsigned kMaxWorkers = 64;

// Balancify: hash, override when the hashed DIP exceeds the pool average by more than T.
// AllStateful: the override gate is forced open, so every connection is recorded.
// Stateless: the override gate is closed, so every connection is hashed.
enum class Mode : uint8_t { Balancify, AllStateful, Stateless };
enum class HashingScheme : uint8_t { Stable, Maglev };
// R0: argmin of the sampled load vector (Algorithm 2).
// R1: argmin, then add the per-connection load estimate to the target until the next sample.
// R2: uniform choice among the DIPs whose sampled load is below the average.
enum class TargetRule : uint8_t { ArgMin = 0, IncrementTarget = 1, RandomBelowAverage = 2 };
enum class LoadSignal : uint8_t { Cpu, Latency };
enum class BloomPolicy : uint8_t { Auto, On, Off };

inline const char* to_string(Mode m) {
    switch (m) {
    case Mode::Balancify: return "balancify";
    case Mode::AllStateful: return "all_stateful";
    case Mode::Stateless: return "stateless";
    }
    return "?";
}

inline const char* to_string(HashingScheme h) {
    return h == HashingScheme::Stable ? "stable" : "maglev";
}

inline const char* to_string(TargetRule r) {
    switch (r) {
    case TargetRule::ArgMin: return "r0";
    case TargetRule::IncrementTarget: return "r1";
    case TargetRule::RandomBelowAverage: return "r2";
    }
    return "?";
}

inline const char* to_string(LoadSignal s) {
    return s == LoadSignal::Cpu ? "cpu" : "latency";
}

inline const char* to_string(BloomPolicy b) {
    switch (b) {
    case BloomPolicy::Auto: return "auto";
    case BloomPolicy::On: return "on";
    case BloomPolicy::Off: return "off";
    }
    return "?";
}

// Connection identifier. Addresses and ports are kept in network byte order,
// exactly as they appear in the packet; the padding is always zero so the
// tuple can be compared and hashed as two 64-bit words.
struct alignas(16) FiveTuple {
    uint32_t src_ip{0};
    uint32_t dst_ip{0};
    uint16_t src_port{0};
    uint16_t dst_port{0};
    uint8_t proto{0};
    uint8_t pad[3]{0, 0, 0};

    bool operator==(const FiveTuple& o) const {
        return std::memcmp(this, &o, sizeof(FiveTuple)) == 0;
    }
};
static_assert(sizeof(FiveTuple) == 16);

// Independent hash families. kSeedStable selects the consistent-hashing bucket
// and must be identical on every instance; kSeedTable indexes the per-core
// connection table and supplies the fingerprint and the Bloom-filter input;
// kSeedShard spreads bindings over coordination-store shards.
inline constexpr uint64_t kSeedTable = 0x6a09e667f3bcc908ULL;
inline constexpr uint64_t kSeedStable = 0xbb67ae8584caa73bULL;
inline constexpr uint64_t kSeedShard = 0x3c6ef372fe94f82bULL;
inline constexpr uint64_t kSaltBloom = 0xa54ff53a5f1d36f1ULL;

inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

inline uint64_t hash_tuple(const FiveTuple& t, uint64_t seed) {
    uint64_t w0;
    uint64_t w1;
    std::memcpy(&w0, &t, sizeof(w0));
    std::memcpy(&w1, reinterpret_cast<const char*>(&t) + sizeof(w0), sizeof(w1));
    return mix64(mix64(w0 ^ seed) ^ (w1 + 0x9e3779b97f4a7c15ULL * (seed | 1)));
}

inline std::string ip_to_string(uint32_t ip_be) {
    char buf[INET_ADDRSTRLEN] = {};
    inet_ntop(AF_INET, &ip_be, buf, sizeof(buf));
    return buf;
}

inline bool parse_ipv4(std::string_view text, uint32_t& ip_be) {
    const std::string s(text);
    return inet_pton(AF_INET, s.c_str(), &ip_be) == 1;
}

// 26 hex characters: src ip, dst ip, src port, dst port, protocol.
inline std::string tuple_to_hex(const FiveTuple& t) {
    static constexpr char kHex[] = "0123456789abcdef";
    const auto* b = reinterpret_cast<const uint8_t*>(&t);
    std::string out;
    out.reserve(26);
    for (size_t i = 0; i < 13; ++i) {
        out.push_back(kHex[b[i] >> 4]);
        out.push_back(kHex[b[i] & 0xf]);
    }
    return out;
}

inline bool tuple_from_hex(std::string_view hex, FiveTuple& t) {
    if (hex.size() != 26) {
        return false;
    }
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    t = FiveTuple{};
    auto* b = reinterpret_cast<uint8_t*>(&t);
    for (size_t i = 0; i < 13; ++i) {
        const int hi = nibble(hex[2 * i]);
        const int lo = nibble(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            return false;
        }
        b[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

} // namespace balancify::lb
