#include "hash_path.hpp"

#include <rte_common.h>
#include <rte_malloc.h>

#include <algorithm>
#include <cmath>
#include <new>
#include <stdexcept>

namespace balancify::lb {

namespace {

std::vector<uint16_t> sorted_by_ip(const std::vector<uint16_t>& members, const DipPool& pool) {
    std::vector<uint16_t> order(members);
    std::sort(order.begin(), order.end(), [&](uint16_t a, uint16_t b) {
        return ntohl(pool[a].ip_be) < ntohl(pool[b].ip_be);
    });
    order.erase(std::unique(order.begin(), order.end()), order.end());
    return order;
}

} // namespace

std::vector<uint16_t> stable_assign(const std::vector<uint16_t>& previous, uint32_t nbuckets,
                                    const std::vector<uint16_t>& members, const DipPool& pool) {
    std::vector<uint16_t> out(nbuckets, kInvalidDip);
    if (members.empty()) {
        return out;
    }
    if (previous.size() == nbuckets) {
        out = previous;
    }
    const auto order = sorted_by_ip(members, pool);

    // Target bucket count per member, by largest remainder over the weights.
    std::vector<uint32_t> target(kMaxDips, 0);
    std::vector<uint8_t> is_member(kMaxDips, 0);
    double wsum = 0.0;
    for (uint16_t d : order) {
        is_member[d] = 1;
        wsum += std::max<uint32_t>(pool[d].weight, 1);
    }
    uint32_t assigned = 0;
    std::vector<std::pair<double, size_t>> remainders;
    for (size_t i = 0; i < order.size(); ++i) {
        const uint16_t d = order[i];
        const double exact = static_cast<double>(nbuckets) * std::max<uint32_t>(pool[d].weight, 1) / wsum;
        const auto whole = static_cast<uint32_t>(std::floor(exact));
        target[d] = whole;
        assigned += whole;
        remainders.emplace_back(exact - whole, i);
    }
    std::stable_sort(remainders.begin(), remainders.end(),
                     [](const auto& a, const auto& b) { return a.first > b.first; });
    for (size_t i = 0; assigned < nbuckets && i < remainders.size(); ++i, ++assigned) {
        ++target[order[remainders[i].second]];
    }

    // Keep every bucket whose DIP is still a member.
    std::vector<uint32_t> count(kMaxDips, 0);
    for (uint32_t b = 0; b < nbuckets; ++b) {
        const uint16_t d = out[b];
        if (d < kMaxDips && is_member[d]) {
            ++count[d];
        } else {
            out[b] = kInvalidDip;
        }
    }
    // Release the excess of members above their share.
    for (uint32_t b = nbuckets; b-- > 0;) {
        const uint16_t d = out[b];
        if (d != kInvalidDip && count[d] > target[d]) {
            out[b] = kInvalidDip;
            --count[d];
        }
    }
    // Hand free buckets to members below their share, round robin.
    size_t cursor = 0;
    for (uint32_t b = 0; b < nbuckets; ++b) {
        if (out[b] != kInvalidDip) continue;
        for (size_t tries = 0; tries < order.size(); ++tries) {
            const uint16_t d = order[cursor];
            cursor = (cursor + 1) % order.size();
            if (count[d] < target[d]) {
                out[b] = d;
                ++count[d];
                break;
            }
        }
    }
    return out;
}

std::vector<uint16_t> maglev_populate(const std::vector<uint16_t>& members, const DipPool& pool,
                                      uint32_t table_size) {
    std::vector<uint16_t> table(table_size, kInvalidDip);
    if (members.empty()) {
        return table;
    }
    const auto order = sorted_by_ip(members, pool);
    const size_t n = order.size();
    std::vector<uint64_t> offset(n);
    std::vector<uint64_t> skip(n);
    std::vector<uint64_t> next(n, 0);
    for (size_t i = 0; i < n; ++i) {
        const uint64_t ip = ntohl(pool[order[i]].ip_be);
        offset[i] = mix64(ip ^ 0x5bd1e9955bd1e995ULL) % table_size;
        skip[i] = mix64(ip ^ 0x27d4eb2f165667c5ULL) % (table_size - 1) + 1;
    }
    uint32_t filled = 0;
    while (true) {
        for (size_t i = 0; i < n; ++i) {
            uint64_t c = (offset[i] + next[i] * skip[i]) % table_size;
            while (table[c] != kInvalidDip) {
                ++next[i];
                c = (offset[i] + next[i] * skip[i]) % table_size;
            }
            table[c] = order[i];
            ++next[i];
            if (++filled == table_size) {
                return table;
            }
        }
    }
}

HashPathManager::HashPathManager(Runtime& rt) : rt_(rt) {}

uint64_t HashPathManager::rebuild(const std::vector<uint16_t>& members) {
    std::lock_guard lk(mu_);
    if (rt_.cfg.hashing == HashingScheme::Stable) {
        buckets_ = stable_assign(buckets_, rt_.cfg.buckets, members, *rt_.pool);
    } else {
        buckets_ = maglev_populate(members, *rt_.pool, rt_.cfg.maglev_table_size);
    }
    ++version_;
    publish_locked();
    return version_;
}

void HashPathManager::install(uint64_t version, std::vector<uint16_t> buckets) {
    std::lock_guard lk(mu_);
    if (buckets.empty()) {
        return;
    }
    buckets_ = std::move(buckets);
    version_ = std::max(version_, version);
    publish_locked();
}

std::pair<uint64_t, std::vector<uint16_t>> HashPathManager::current() const {
    std::lock_guard lk(mu_);
    return {version_, buckets_};
}

std::vector<uint32_t> HashPathManager::bucket_counts() const {
    std::lock_guard lk(mu_);
    std::vector<uint32_t> counts(kMaxDips, 0);
    for (uint16_t d : buckets_) {
        if (d < kMaxDips) ++counts[d];
    }
    return counts;
}

void HashPathManager::publish_locked() {
    const auto n = static_cast<uint32_t>(buckets_.size());
    const size_t bytes = sizeof(HashPath) + size_t(n) * sizeof(uint16_t);
    void* mem = rte_zmalloc("bfy_hash_path", bytes, RTE_CACHE_LINE_SIZE);
    if (mem == nullptr) {
        throw std::runtime_error("cannot allocate the hash path");
    }
    auto* hp = new (mem) HashPath();
    hp->version = version_;
    hp->nbuckets = n;
    hp->mask = (n != 0 && (n & (n - 1)) == 0) ? n - 1 : 0;
    hp->dip = reinterpret_cast<uint16_t*>(hp + 1);
    std::copy(buckets_.begin(), buckets_.end(), hp->dip);
    rcu_replace(rt_, rt_.hash_path, hp);
}

} // namespace balancify::lb
