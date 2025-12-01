#pragma once

#include "hash_utils.hpp"
#include "types.hpp"
#include <optional>
#include <vector>

namespace balancify::common {

class MaglevHash {
public:
    explicit MaglevHash(size_t table_size = 65537) : table_size_(table_size) {
        lookup_.resize(table_size_);
    }

    void rebuild(const std::vector<ServerEndpoint>& endpoints) {
        endpoints_ = endpoints;
        std::fill(lookup_.begin(), lookup_.end(), std::nullopt);

        const size_t n = endpoints_.size();
        if (n == 0) {
            return;
        }

        std::vector<std::vector<size_t>> permutations(n, std::vector<size_t>(table_size_));
        for (size_t i = 0; i < n; ++i) {
            auto key = endpoints_[i].name + std::to_string(i);
            const size_t offset = stable_hash(key + ":0") % table_size_;
            const size_t skip = (stable_hash(key + ":1") % (table_size_ - 1)) + 1;
            for (size_t j = 0; j < table_size_; ++j) {
                permutations[i][j] = (offset + j * skip) % table_size_;
            }
        }

        std::vector<size_t> next(n, 0);
        size_t filled = 0;
        while (filled < table_size_) {
            for (size_t i = 0; i < n; ++i) {
                auto c = permutations[i][next[i]];
                while (lookup_[c].has_value()) {
                    ++next[i];
                    c = permutations[i][next[i]];
                }
                lookup_[c] = i;
                ++next[i];
                if (++filled >= table_size_) {
                    break;
                }
            }
        }
    }

    [[nodiscard]] const ServerEndpoint* select(uint64_t hash) const {
        if (endpoints_.empty()) {
            return nullptr;
        }
        auto idx = hash % table_size_;
        auto entry = lookup_[idx];
        if (!entry.has_value() || entry.value() >= endpoints_.size()) {
            return nullptr;
        }
        return &endpoints_[entry.value()];
    }

private:
    size_t table_size_;
    std::vector<std::optional<size_t>> lookup_;
    std::vector<ServerEndpoint> endpoints_;
};

} // namespace balancify::common

