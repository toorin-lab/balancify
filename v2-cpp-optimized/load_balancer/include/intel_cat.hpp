#pragma once

#include <cstdint>
#include <string>

namespace balancify::lb {

class IntelCatManager {
public:
    IntelCatManager() = default;
    ~IntelCatManager();

    void configure(uint32_t mask, unsigned lcore);
    void release();

private:
    bool initialized_{false};
};

} // namespace balancify::lb

