#pragma once

#include "types.hpp"

#include <cstddef>
#include <cstdint>
#include <unordered_map>

namespace balancify::lb {

// Cache-blocked counting Bloom filter over the identifiers in the
// connection-to-DIP table. All k counters of a key sit in one 64-byte block,
// so a query is a single cache-line access. Counters are 4 bits wide; a
// counter that reaches 15 is saturated and its exact value is kept in a small
// auxiliary table, so increments and decrements stay exact indefinitely.
class CountingBloom {
public:
    static constexpr unsigned kCountersPerBlock = 128;
    static constexpr unsigned kSaturated = 15;

    CountingBloom() = default;
    ~CountingBloom() { destroy(); }
    CountingBloom(const CountingBloom&) = delete;
    CountingBloom& operator=(const CountingBloom&) = delete;

    bool init(size_t design_entries, double fp_rate, int socket);
    void destroy();
    bool active() const { return blocks_ != nullptr; }

    // h is the table hash of the 5-tuple (hash_tuple(t, kSeedTable)).
    bool may_contain(uint64_t h) const {
        const uint64_t x = mix64(h + kSaltBloom);
        const uint8_t* blk = block(x);
        const unsigned a = static_cast<unsigned>(x >> 32) & (kCountersPerBlock - 1);
        const unsigned step = (static_cast<unsigned>(x >> 40) | 1u) & (kCountersPerBlock - 1);
        for (unsigned i = 0; i < k_; ++i) {
            if (nibble(blk, (a + i * step) & (kCountersPerBlock - 1)) == 0) {
                return false;
            }
        }
        return true;
    }

    void add(uint64_t h);
    void remove(uint64_t h);

    size_t bytes() const { return static_cast<size_t>(nblocks_) * 64; }
    size_t design_entries() const { return design_; }
    size_t count() const { return count_; }
    unsigned hashes() const { return k_; }
    size_t saturated_counters() const { return overflow_.size(); }

private:
    uint8_t* block(uint64_t x) const {
        return blocks_ + (((x & 0xffffffffULL) * nblocks_) >> 32) * 64;
    }
    static unsigned nibble(const uint8_t* blk, unsigned pos) {
        const uint8_t byte = blk[pos >> 1];
        return (pos & 1) ? (byte >> 4) : (byte & 0xf);
    }
    static void set_nibble(uint8_t* blk, unsigned pos, unsigned v) {
        uint8_t& byte = blk[pos >> 1];
        byte = (pos & 1) ? static_cast<uint8_t>((byte & 0x0f) | (v << 4))
                         : static_cast<uint8_t>((byte & 0xf0) | (v & 0xf));
    }
    uint64_t counter_index(const uint8_t* blk, unsigned pos) const {
        return static_cast<uint64_t>(blk - blocks_) * 2 + pos;
    }

    uint8_t* blocks_{nullptr};
    uint64_t nblocks_{0};
    unsigned k_{1};
    size_t design_{0};
    size_t count_{0};
    std::unordered_map<uint64_t, uint32_t> overflow_;
};

} // namespace balancify::lb
