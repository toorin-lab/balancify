#pragma once

#include "runtime.hpp"
#include "snapshots.hpp"

#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

namespace balancify::lb {

// Stable hashing: a fixed number of buckets mapped to DIPs. When the member
// set changes, only the buckets of departed DIPs and the excess of DIPs above
// their (weighted) share are reassigned, so a DIP addition moves about 1/N of
// the buckets and a removal moves only the buckets of the removed DIP.
std::vector<uint16_t> stable_assign(const std::vector<uint16_t>& previous, uint32_t nbuckets,
                                    const std::vector<uint16_t>& members, const DipPool& pool);

// Maglev lookup table, computed independently on every instance from the member set.
std::vector<uint16_t> maglev_populate(const std::vector<uint16_t>& members, const DipPool& pool,
                                      uint32_t table_size);

class HashPathManager {
public:
    explicit HashPathManager(Runtime& rt);

    // Recomputes the bucket table from the eligible DIP set and publishes it.
    uint64_t rebuild(const std::vector<uint16_t>& members);
    // Publishes a bucket table received from the coordination store.
    void install(uint64_t version, std::vector<uint16_t> buckets);

    std::pair<uint64_t, std::vector<uint16_t>> current() const;
    std::vector<uint32_t> bucket_counts() const;

private:
    void publish_locked();

    Runtime& rt_;
    mutable std::mutex mu_;
    uint64_t version_{0};
    std::vector<uint16_t> buckets_;
};

} // namespace balancify::lb
