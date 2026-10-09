#include "dip_pool.hpp"

#include <cstring>
#include <stdexcept>

namespace balancify::lb {

DipPool::DipPool(const rte_ether_addr& gateway_mac)
    : gateway_mac_(gateway_mac),
      slots_holder_(std::make_unique<std::array<DipSlot, kMaxDips>>()),
      slots_(*slots_holder_) {}

uint16_t DipPool::add(const std::string& name, uint32_t ip_be, const rte_ether_addr* mac, uint32_t weight) {
    std::lock_guard lk(mu_);
    if (auto it = by_ip_.find(ip_be); it != by_ip_.end()) {
        return it->second;
    }
    const uint16_t idx = count_.load(std::memory_order_relaxed);
    if (idx >= kMaxDips) {
        throw std::runtime_error("DIP pool is full");
    }
    DipSlot& s = slots_[idx];
    const std::string label = name.empty() ? ip_to_string(ip_be) : name;
    std::strncpy(s.name, label.c_str(), sizeof(s.name) - 1);
    s.ip_be = ip_be;
    s.mac = mac ? *mac : gateway_mac_;
    s.weight = weight == 0 ? 1 : weight;
    s.admin_enabled.store(true, std::memory_order_relaxed);
    s.eligible.store(false, std::memory_order_relaxed);
    by_ip_[ip_be] = idx;
    by_name_[label] = idx;
    count_.store(static_cast<uint16_t>(idx + 1), std::memory_order_release);
    return idx;
}

uint16_t DipPool::find_ip(uint32_t ip_be) const {
    std::lock_guard lk(mu_);
    const auto it = by_ip_.find(ip_be);
    return it == by_ip_.end() ? kInvalidDip : it->second;
}

uint16_t DipPool::find_name(const std::string& name) const {
    std::lock_guard lk(mu_);
    const auto it = by_name_.find(name);
    return it == by_name_.end() ? kInvalidDip : it->second;
}

std::vector<uint16_t> DipPool::eligible() const {
    std::vector<uint16_t> out;
    const uint16_t n = size();
    for (uint16_t i = 0; i < n; ++i) {
        if (slots_[i].eligible.load(std::memory_order_relaxed)) {
            out.push_back(i);
        }
    }
    return out;
}

} // namespace balancify::lb
