#include "counting_bloom.hpp"

#include <rte_common.h>
#include <rte_malloc.h>

#include <algorithm>
#include <cmath>

namespace balancify::lb {

bool CountingBloom::init(size_t design_entries, double fp_rate, int socket) {
    destroy();
    const double n = static_cast<double>(std::max<size_t>(design_entries, 1024));
    const double ln2 = std::log(2.0);
    const double m = std::ceil(-n * std::log(fp_rate) / (ln2 * ln2));
    nblocks_ = static_cast<uint64_t>(std::ceil(m / kCountersPerBlock));
    k_ = static_cast<unsigned>(std::clamp(std::lround(m / n * ln2), 1L, 16L));
    blocks_ = static_cast<uint8_t*>(rte_zmalloc_socket("bfy_bloom", bytes(), RTE_CACHE_LINE_SIZE, socket));
    if (blocks_ == nullptr) {
        nblocks_ = 0;
        return false;
    }
    design_ = static_cast<size_t>(n);
    count_ = 0;
    overflow_.clear();
    return true;
}

void CountingBloom::destroy() {
    rte_free(blocks_);
    blocks_ = nullptr;
    nblocks_ = 0;
    design_ = 0;
    count_ = 0;
    overflow_.clear();
}

void CountingBloom::add(uint64_t h) {
    if (blocks_ == nullptr) return;
    const uint64_t x = mix64(h + kSaltBloom);
    uint8_t* blk = block(x);
    const unsigned a = static_cast<unsigned>(x >> 32) & (kCountersPerBlock - 1);
    const unsigned step = (static_cast<unsigned>(x >> 40) | 1u) & (kCountersPerBlock - 1);
    for (unsigned i = 0; i < k_; ++i) {
        const unsigned pos = (a + i * step) & (kCountersPerBlock - 1);
        const unsigned v = nibble(blk, pos);
        if (v + 1 < kSaturated) {
            set_nibble(blk, pos, v + 1);
        } else if (v + 1 == kSaturated) {
            set_nibble(blk, pos, kSaturated);
            overflow_[counter_index(blk, pos)] = kSaturated;
        } else {
            ++overflow_[counter_index(blk, pos)];
        }
    }
    ++count_;
}

void CountingBloom::remove(uint64_t h) {
    if (blocks_ == nullptr) return;
    const uint64_t x = mix64(h + kSaltBloom);
    uint8_t* blk = block(x);
    const unsigned a = static_cast<unsigned>(x >> 32) & (kCountersPerBlock - 1);
    const unsigned step = (static_cast<unsigned>(x >> 40) | 1u) & (kCountersPerBlock - 1);
    for (unsigned i = 0; i < k_; ++i) {
        const unsigned pos = (a + i * step) & (kCountersPerBlock - 1);
        const unsigned v = nibble(blk, pos);
        if (v == 0) {
            continue;
        }
        if (v < kSaturated) {
            set_nibble(blk, pos, v - 1);
            continue;
        }
        const auto it = overflow_.find(counter_index(blk, pos));
        if (it == overflow_.end() || it->second <= kSaturated) {
            set_nibble(blk, pos, kSaturated - 1);
            if (it != overflow_.end()) overflow_.erase(it);
        } else {
            --it->second;
        }
    }
    if (count_ > 0) --count_;
}

} // namespace balancify::lb
