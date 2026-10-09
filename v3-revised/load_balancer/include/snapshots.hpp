#pragma once

#include "types.hpp"

#include <atomic>
#include <cmath>
#include <cstdint>

namespace balancify::lb {

// Bucket-to-DIP table of the hash path. Immutable once published; replaced
// as a whole under RCU when the DIP set changes. The DIP array follows the
// header in the same allocation.
struct HashPath {
    uint64_t version{0};
    uint32_t nbuckets{0};
    uint32_t mask{0};  // nbuckets - 1 when nbuckets is a power of two, otherwise 0
    uint16_t* dip{nullptr};

    uint16_t lookup(uint64_t stable_hash) const {
        const auto x = static_cast<uint32_t>(stable_hash);
        return dip[mask != 0 ? (x & mask) : (x % nbuckets)];
    }
};

// Load vector sampled by the monitoring thread every tau seconds
// (Algorithm 1), plus what the override path derives from it.
struct LoadSnapshot {
    uint64_t generation{0};
    uint16_t ndips{0};
    uint16_t n_eligible{0};
    uint16_t argmin{kInvalidDip};
    uint16_t argmax{kInvalidDip};
    uint16_t n_below{0};
    double avg{0.0};
    double stddev{0.0};
    double mu{0.0};  // estimated load of one connection (R1 increment)
    float load[kMaxDips]{};
    uint8_t eligible[kMaxDips]{};
    uint16_t below[kMaxDips]{};
};

// Load the R1 rule has added to override targets since the last sample,
// shared by all forwarding cores. Fixed point, millionths of a load unit.
struct alignas(64) PendingLoad {
    std::atomic<int64_t> micro[kMaxDips];
    std::atomic<int64_t> total;

    PendingLoad() { reset(); }

    void reset() {
        for (auto& m : micro) m.store(0, std::memory_order_relaxed);
        total.store(0, std::memory_order_relaxed);
    }
    double get(uint16_t d) const {
        return static_cast<double>(micro[d].load(std::memory_order_relaxed)) * 1e-6;
    }
    double total_value() const {
        return static_cast<double>(total.load(std::memory_order_relaxed)) * 1e-6;
    }
    void add(uint16_t d, double v) {
        const auto inc = static_cast<int64_t>(std::llround(v * 1e6));
        micro[d].fetch_add(inc, std::memory_order_relaxed);
        total.fetch_add(inc, std::memory_order_relaxed);
    }
};

} // namespace balancify::lb
