#pragma once

#include "types.hpp"

#include <rte_malloc.h>

#include <emmintrin.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>

namespace balancify::lb {

// Per-core connection-to-DIP table holding only overridden connections.
//
// Open addressing over 64-byte buckets of eight slots. A bucket keeps a 16-bit
// fingerprint, the DIP index and 32 bits of metadata per slot, so a probe
// touches one cache line; full 5-tuples live in a parallel array and are read
// only when a fingerprint matches. The table grows (or shrinks) by allocating
// a second array and migrating a bounded number of buckets per packet; during
// the migration lookups consult both arrays and insertions go to the new one.
class ConnTable {
public:
    static constexpr unsigned kSlots = 8;
    static constexpr unsigned kMaxProbe = 16;
    static constexpr uint16_t kFpEmpty = 0;
    static constexpr uint16_t kFpTomb = 1;

    // meta: bits 0..27 last-seen time in milliseconds (wrapping), bits 28..31 flags.
    static constexpr uint32_t kTimeMask = 0x0fffffffu;
    static constexpr uint32_t kFlagFin = 1u << 28;     // FIN observed; reclaimed after the linger time
    static constexpr uint32_t kFlagRemote = 1u << 29;  // binding owned by another LB instance

    struct alignas(64) Bucket {
        uint16_t fp[kSlots];
        uint16_t dip[kSlots];
        uint32_t meta[kSlots];
    };
    static_assert(sizeof(Bucket) == 64);
    static constexpr size_t kBytesPerBucket = sizeof(Bucket) + kSlots * sizeof(FiveTuple);

    struct Entry {
        Bucket* bucket{nullptr};
        FiveTuple* key{nullptr};
        unsigned slot{0};

        explicit operator bool() const { return bucket != nullptr; }
        uint16_t dip() const { return bucket->dip[slot]; }
        void set_dip(uint16_t d) const { bucket->dip[slot] = d; }
        uint32_t& meta() const { return bucket->meta[slot]; }
    };

    ConnTable() = default;
    ~ConnTable();
    ConnTable(const ConnTable&) = delete;
    ConnTable& operator=(const ConnTable&) = delete;

    // budget_bytes: cache share of this core; the initial array is the
    // largest power-of-two bucket count that fits in it.
    bool init(size_t budget_bytes, int socket);

    static uint16_t fingerprint(uint64_t h) {
        const auto fp = static_cast<uint16_t>(h >> 48);
        return fp < 2 ? static_cast<uint16_t>(fp + 2) : fp;
    }

    Entry find(const FiveTuple& t, uint64_t h) {
        const uint16_t fp = fingerprint(h);
        Entry e = find_in(cur_, t, h, fp);
        if (!e && old_.buckets != nullptr) {
            e = find_in(old_, t, h, fp);
        }
        return e;
    }

    // The caller guarantees the key is absent (a find() miss precedes it).
    bool insert(const FiveTuple& t, uint64_t h, uint16_t dip, uint32_t meta);
    void erase(const Entry& e);

    // Advances an ongoing resize by up to `nbuckets` old buckets, or starts
    // one when the load factor calls for it.
    void migrate(unsigned nbuckets);

    // Visits up to `nbuckets` buckets of the current array, cyclically.
    // fn(const FiveTuple&, uint16_t dip, uint32_t meta) returns true to remove.
    template <class Fn>
    void sweep(unsigned nbuckets, Fn&& fn) {
        if (cur_.buckets == nullptr) return;
        while (nbuckets--) {
            if (sweep_pos_ >= cur_.nbuckets) sweep_pos_ = 0;
            Bucket& b = cur_.buckets[sweep_pos_];
            for (unsigned s = 0; s < kSlots; ++s) {
                if (b.fp[s] > kFpTomb &&
                    fn(cur_.keys[size_t(sweep_pos_) * kSlots + s], b.dip[s], b.meta[s])) {
                    tombstone(cur_, b, s);
                }
            }
            ++sweep_pos_;
        }
    }

    // Visits every entry in both arrays. fn(const Entry&) returns true to remove.
    template <class Fn>
    void for_each(Fn&& fn) {
        for (Array* a : {&cur_, &old_}) {
            if (a->buckets == nullptr) continue;
            for (uint32_t i = 0; i < a->nbuckets; ++i) {
                Bucket& b = a->buckets[i];
                for (unsigned s = 0; s < kSlots; ++s) {
                    if (b.fp[s] <= kFpTomb) continue;
                    Entry e{&b, &a->keys[size_t(i) * kSlots + s], s};
                    if (fn(e)) tombstone(*a, b, s);
                }
            }
        }
    }

    size_t size() const { return entries_; }
    size_t bytes() const { return (size_t(cur_.nbuckets) + old_.nbuckets) * kBytesPerBucket; }
    size_t budget_bytes() const { return budget_; }
    bool beyond_budget() const { return size_t(cur_.nbuckets) * kBytesPerBucket > budget_; }
    bool migrating() const { return old_.buckets != nullptr; }
    uint64_t resizes() const { return resizes_; }
    uint64_t lost_in_migration() const { return lost_; }

private:
    struct Array {
        Bucket* buckets{nullptr};
        FiveTuple* keys{nullptr};
        uint32_t nbuckets{0};
        uint32_t mask{0};
        size_t used{0};
        size_t tombs{0};
    };

    Entry find_in(Array& a, const FiveTuple& t, uint64_t h, uint16_t fp) {
        const __m128i needle = _mm_set1_epi16(static_cast<short>(fp));
        const __m128i empty = _mm_setzero_si128();
        uint32_t idx = static_cast<uint32_t>(h) & a.mask;
        for (unsigned probe = 0; probe < kMaxProbe; ++probe) {
            Bucket* b = &a.buckets[idx];
            const __m128i fps = _mm_load_si128(reinterpret_cast<const __m128i*>(b->fp));
            unsigned hits = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi16(fps, needle)));
            while (hits != 0) {
                const unsigned slot = static_cast<unsigned>(__builtin_ctz(hits)) >> 1;
                FiveTuple* key = &a.keys[size_t(idx) * kSlots + slot];
                if (*key == t) {
                    return Entry{b, key, slot};
                }
                hits &= ~(3u << (slot * 2));
            }
            if (_mm_movemask_epi8(_mm_cmpeq_epi16(fps, empty)) != 0) {
                return Entry{};
            }
            idx = (idx + 1) & a.mask;
        }
        return Entry{};
    }

    bool insert_in(Array& a, const FiveTuple& t, uint64_t h, uint16_t fp, uint16_t dip, uint32_t meta);
    void tombstone(Array& a, Bucket& b, unsigned slot);
    bool alloc(Array& a, uint32_t nbuckets);
    void release(Array& a);
    void maybe_resize();
    bool start_resize(uint32_t nbuckets);
    void finish_migration();
    Array& array_of(const Entry& e);

    Array cur_;
    Array old_;
    uint32_t migrate_pos_{0};
    uint32_t sweep_pos_{0};
    uint32_t initial_nbuckets_{0};
    size_t entries_{0};
    size_t budget_{0};
    int socket_{0};
    uint64_t resizes_{0};
    uint64_t lost_{0};
};

} // namespace balancify::lb
