#pragma once

#include <cmath>
#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

#include "hash_utils.hpp"

namespace balancify::common {

class CountingBloomFilter {
public:
    CountingBloomFilter() = default;

    CountingBloomFilter(size_t capacity, double error_rate) {
        configure(capacity, error_rate);
    }

    void configure(size_t capacity, double error_rate) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (capacity == 0) {
            capacity = 1;
        }
        double num_bits = -static_cast<double>(capacity) * std::log(error_rate) / (std::log(2) * std::log(2));
        bits_.assign(static_cast<size_t>(std::ceil(num_bits)), 0);
        num_hashes_ = std::max<size_t>(1, static_cast<size_t>(std::round((bits_.size() / static_cast<double>(capacity)) * std::log(2))));
    }

    void add(const std::string& key) {
        std::lock_guard<std::mutex> lk(mutex_);
        for (size_t i = 0; i < num_hashes_; ++i) {
            auto position = position_for(key, i);
            ++bits_[position];
        }
    }

    void remove(const std::string& key) {
        std::lock_guard<std::mutex> lk(mutex_);
        for (size_t i = 0; i < num_hashes_; ++i) {
            auto position = position_for(key, i);
            if (bits_[position] > 0) {
                --bits_[position];
            }
        }
    }

    [[nodiscard]] bool contains(const std::string& key) const {
        std::lock_guard<std::mutex> lk(mutex_);
        for (size_t i = 0; i < num_hashes_; ++i) {
            auto position = position_for(key, i);
            if (bits_[position] == 0) {
                return false;
            }
        }
        return true;
    }

private:
    size_t position_for(const std::string& key, size_t seed) const {
        auto hash = stable_hash(key + std::to_string(seed));
        return hash % bits_.size();
    }

    mutable std::mutex mutex_;
    std::vector<uint16_t> bits_;
    size_t num_hashes_{1};
};

} // namespace balancify::common

