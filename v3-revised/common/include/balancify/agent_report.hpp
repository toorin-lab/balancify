#pragma once

#include <arpa/inet.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace balancify {

// Load report sent by the agent on every DIP to every load-balancer instance.
// One UDP datagram per report; all integers are in network byte order.
inline constexpr uint32_t kReportMagic = 0x424c4659;  // "BLFY"
inline constexpr uint16_t kReportVersion = 1;
inline constexpr size_t kReportWireSize = 40;
inline constexpr uint16_t kReportHealthy = 0x1;

struct AgentReport {
    uint32_t dip_ip_be{0};     // DIP address, network byte order
    uint32_t seq{0};
    uint64_t timestamp_us{0};  // agent wall clock
    float cpu_pct{0.0f};       // CPU utilization averaged over the agent window
    float latency_ms{0.0f};    // mean request-response time over the agent window
    uint32_t active_conns{0};  // established connections on the service port
    uint32_t requests{0};      // requests completed in the agent window
    bool healthy{true};
};

namespace detail {

inline void put16(uint8_t*& p, uint16_t v) {
    v = htons(v);
    std::memcpy(p, &v, sizeof(v));
    p += sizeof(v);
}

inline void put32(uint8_t*& p, uint32_t v) {
    v = htonl(v);
    std::memcpy(p, &v, sizeof(v));
    p += sizeof(v);
}

inline void put64(uint8_t*& p, uint64_t v) {
    put32(p, static_cast<uint32_t>(v >> 32));
    put32(p, static_cast<uint32_t>(v));
}

inline uint16_t get16(const uint8_t*& p) {
    uint16_t v;
    std::memcpy(&v, p, sizeof(v));
    p += sizeof(v);
    return ntohs(v);
}

inline uint32_t get32(const uint8_t*& p) {
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    p += sizeof(v);
    return ntohl(v);
}

inline uint64_t get64(const uint8_t*& p) {
    const uint64_t hi = get32(p);
    return (hi << 32) | get32(p);
}

} // namespace detail

inline size_t encode_report(const AgentReport& r, uint8_t* buf) {
    using namespace detail;
    uint8_t* p = buf;
    put32(p, kReportMagic);
    put16(p, kReportVersion);
    put16(p, r.healthy ? kReportHealthy : 0);
    std::memcpy(p, &r.dip_ip_be, sizeof(r.dip_ip_be));
    p += sizeof(r.dip_ip_be);
    put32(p, r.seq);
    put64(p, r.timestamp_us);
    put32(p, std::bit_cast<uint32_t>(r.cpu_pct));
    put32(p, std::bit_cast<uint32_t>(r.latency_ms));
    put32(p, r.active_conns);
    put32(p, r.requests);
    return static_cast<size_t>(p - buf);
}

inline bool decode_report(const uint8_t* buf, size_t len, AgentReport& r) {
    using namespace detail;
    if (len < kReportWireSize) {
        return false;
    }
    const uint8_t* p = buf;
    if (get32(p) != kReportMagic || get16(p) != kReportVersion) {
        return false;
    }
    const uint16_t flags = get16(p);
    std::memcpy(&r.dip_ip_be, p, sizeof(r.dip_ip_be));
    p += sizeof(r.dip_ip_be);
    r.seq = get32(p);
    r.timestamp_us = get64(p);
    r.cpu_pct = std::bit_cast<float>(get32(p));
    r.latency_ms = std::bit_cast<float>(get32(p));
    r.active_conns = get32(p);
    r.requests = get32(p);
    r.healthy = (flags & kReportHealthy) != 0;
    return true;
}

} // namespace balancify
