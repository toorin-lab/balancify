#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace balancify::lb {

// Reserves a partition of the last-level cache for the forwarding cores with
// Intel Cache Allocation Technology (through libpqos). The partition is shared
// by all forwarding cores, which matches the shared LLC: tables, Bloom filters
// and the bucket table of every core live in the same ways.
class CacheAllocation {
public:
    CacheAllocation() = default;
    ~CacheAllocation();
    CacheAllocation(const CacheAllocation&) = delete;
    CacheAllocation& operator=(const CacheAllocation&) = delete;

    // Returns the number of bytes actually reserved (a whole number of ways),
    // or 0 when megabytes is 0 or CAT is unavailable.
    size_t apply(double megabytes, unsigned cos, unsigned way_offset, bool isolate_others,
                 const std::string& interface, const std::vector<unsigned>& cpus);
    void release();

    static size_t llc_size_bytes();

private:
    bool active_{false};
    unsigned cos_{0};
    std::vector<unsigned> cpus_;
    std::vector<unsigned> l3_ids_;
    uint64_t full_mask_{0};
    bool isolated_{false};
};

} // namespace balancify::lb
