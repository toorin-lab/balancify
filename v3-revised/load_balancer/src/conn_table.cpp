#include "conn_table.hpp"

#include <rte_common.h>

#include <algorithm>

namespace balancify::lb {

namespace {

uint32_t pow2_floor(size_t x) {
    if (x < 1) return 1;
    uint32_t p = 1;
    while (size_t(p) * 2 <= x && p < (1u << 30)) p <<= 1;
    return p;
}

} // namespace

ConnTable::~ConnTable() {
    release(cur_);
    release(old_);
}

bool ConnTable::init(size_t budget_bytes, int socket) {
    budget_ = budget_bytes;
    socket_ = socket;
    initial_nbuckets_ = std::max<uint32_t>(64, pow2_floor(budget_bytes / kBytesPerBucket));
    return alloc(cur_, initial_nbuckets_);
}

bool ConnTable::alloc(Array& a, uint32_t nbuckets) {
    Array fresh;
    fresh.buckets = static_cast<Bucket*>(
        rte_zmalloc_socket("bfy_ct_buckets", size_t(nbuckets) * sizeof(Bucket), RTE_CACHE_LINE_SIZE, socket_));
    fresh.keys = static_cast<FiveTuple*>(
        rte_zmalloc_socket("bfy_ct_keys", size_t(nbuckets) * kSlots * sizeof(FiveTuple), RTE_CACHE_LINE_SIZE, socket_));
    if (fresh.buckets == nullptr || fresh.keys == nullptr) {
        rte_free(fresh.buckets);
        rte_free(fresh.keys);
        return false;
    }
    fresh.nbuckets = nbuckets;
    fresh.mask = nbuckets - 1;
    a = fresh;
    return true;
}

void ConnTable::release(Array& a) {
    rte_free(a.buckets);
    rte_free(a.keys);
    a = Array{};
}

ConnTable::Array& ConnTable::array_of(const Entry& e) {
    if (old_.buckets != nullptr && e.bucket >= old_.buckets && e.bucket < old_.buckets + old_.nbuckets) {
        return old_;
    }
    return cur_;
}

bool ConnTable::insert_in(Array& a, const FiveTuple& t, uint64_t h, uint16_t fp, uint16_t dip, uint32_t meta) {
    uint32_t idx = static_cast<uint32_t>(h) & a.mask;
    for (unsigned probe = 0; probe < kMaxProbe; ++probe) {
        Bucket& b = a.buckets[idx];
        for (unsigned s = 0; s < kSlots; ++s) {
            if (b.fp[s] <= kFpTomb) {
                if (b.fp[s] == kFpTomb) --a.tombs;
                a.keys[size_t(idx) * kSlots + s] = t;
                b.dip[s] = dip;
                b.meta[s] = meta;
                b.fp[s] = fp;
                ++a.used;
                return true;
            }
        }
        idx = (idx + 1) & a.mask;
    }
    return false;
}

bool ConnTable::insert(const FiveTuple& t, uint64_t h, uint16_t dip, uint32_t meta) {
    const uint16_t fp = fingerprint(h);
    if (!insert_in(cur_, t, h, fp, dip, meta)) {
        // Probe window exhausted: complete any pending migration and grow now.
        if (old_.buckets != nullptr) finish_migration();
        if (!start_resize(cur_.nbuckets * 2)) return false;
        if (!insert_in(cur_, t, h, fp, dip, meta)) return false;
    }
    ++entries_;
    maybe_resize();
    return true;
}

void ConnTable::tombstone(Array& a, Bucket& b, unsigned slot) {
    b.fp[slot] = kFpTomb;
    --a.used;
    ++a.tombs;
    --entries_;
}

void ConnTable::erase(const Entry& e) {
    tombstone(array_of(e), *e.bucket, e.slot);
}

void ConnTable::maybe_resize() {
    if (old_.buckets != nullptr) return;
    const size_t slots = size_t(cur_.nbuckets) * kSlots;
    if ((cur_.used + cur_.tombs) * 4 > slots * 3) {
        // Grow when live entries dominate, otherwise rebuild in place to drop tombstones.
        start_resize(cur_.used * 2 > slots ? cur_.nbuckets * 2 : cur_.nbuckets);
    } else if (cur_.nbuckets > initial_nbuckets_ && cur_.used * 8 < slots) {
        start_resize(cur_.nbuckets / 2);
    }
}

bool ConnTable::start_resize(uint32_t nbuckets) {
    Array fresh;
    if (!alloc(fresh, nbuckets)) return false;
    old_ = cur_;
    cur_ = fresh;
    migrate_pos_ = 0;
    sweep_pos_ = 0;
    ++resizes_;
    return true;
}

void ConnTable::migrate(unsigned nbuckets) {
    if (old_.buckets == nullptr) {
        maybe_resize();
        return;
    }
    while (nbuckets-- && migrate_pos_ < old_.nbuckets) {
        Bucket& b = old_.buckets[migrate_pos_];
        for (unsigned s = 0; s < kSlots; ++s) {
            if (b.fp[s] <= kFpTomb) continue;
            const FiveTuple& key = old_.keys[size_t(migrate_pos_) * kSlots + s];
            if (!insert_in(cur_, key, hash_tuple(key, kSeedTable), b.fp[s], b.dip[s], b.meta[s])) {
                --entries_;
                ++lost_;
            }
            // A tombstone, not an empty slot, so probes through this bucket keep going.
            b.fp[s] = kFpTomb;
            --old_.used;
        }
        ++migrate_pos_;
    }
    if (migrate_pos_ >= old_.nbuckets) {
        finish_migration();
    }
}

void ConnTable::finish_migration() {
    while (migrate_pos_ < old_.nbuckets) {
        migrate(old_.nbuckets - migrate_pos_);
        if (old_.buckets == nullptr) return;
    }
    release(old_);
    migrate_pos_ = 0;
}

} // namespace balancify::lb
