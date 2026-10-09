#pragma once

#include "types.hpp"

#include <rte_ether.h>

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace balancify::lb {

// One backend server. Slots are written once, before the pool size is
// published, and are never reused, so the forwarding path reads them without
// locks. Health and administrative state change at run time.
struct DipSlot {
    char name[48]{};
    uint32_t ip_be{0};
    rte_ether_addr mac{};   // destination MAC of the outer frame (DIP or next hop)
    uint32_t weight{1};
    std::atomic<bool> admin_enabled{true};
    std::atomic<bool> eligible{false};  // healthy, enabled, and usable as a target
};

class DipPool {
public:
    explicit DipPool(const rte_ether_addr& gateway_mac);

    // Adds a DIP or returns the index of the existing DIP with the same address.
    uint16_t add(const std::string& name, uint32_t ip_be, const rte_ether_addr* mac, uint32_t weight);
    uint16_t find_ip(uint32_t ip_be) const;
    uint16_t find_name(const std::string& name) const;

    uint16_t size() const { return count_.load(std::memory_order_acquire); }
    DipSlot& operator[](uint16_t i) { return slots_[i]; }
    const DipSlot& operator[](uint16_t i) const { return slots_[i]; }

    std::vector<uint16_t> eligible() const;

private:
    rte_ether_addr gateway_mac_;
    mutable std::mutex mu_;
    std::unique_ptr<std::array<DipSlot, kMaxDips>> slots_holder_;
    std::array<DipSlot, kMaxDips>& slots_;
    std::atomic<uint16_t> count_{0};
    std::unordered_map<uint32_t, uint16_t> by_ip_;
    std::unordered_map<std::string, uint16_t> by_name_;
};

} // namespace balancify::lb
