#pragma once

#include "types.hpp"
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <openssl/sha.h>
#include <sstream>
#include <string>

namespace balancify::common {

inline std::string ConnectionTuple::to_string() const {
    std::ostringstream oss;
    oss << static_cast<int>(src_ip[0]) << "." << static_cast<int>(src_ip[1]) << "."
        << static_cast<int>(src_ip[2]) << "." << static_cast<int>(src_ip[3])
        << ":" << src_port << ">";
    oss << static_cast<int>(dst_ip[0]) << "." << static_cast<int>(dst_ip[1]) << "."
        << static_cast<int>(dst_ip[2]) << "." << static_cast<int>(dst_ip[3])
        << ":" << dst_port << "#" << static_cast<int>(protocol);
    return oss.str();
}

inline uint64_t stable_hash(const std::string& value) {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
    SHA256(reinterpret_cast<const unsigned char*>(value.data()), value.size(), digest.data());
    uint64_t hash = 0;
    std::memcpy(&hash, digest.data(), sizeof(uint64_t));
    return hash;
}

} // namespace balancify::common

