#include "cache_allocation.hpp"

#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#ifdef BALANCIFY_HAS_PQOS
extern "C" {
#include <pqos.h>
}
#endif

namespace balancify::lb {

size_t CacheAllocation::llc_size_bytes() {
    // The highest cache index of cpu0 is the LLC; sysfs reports e.g. "16896K".
    for (int idx = 4; idx >= 0; --idx) {
        std::ifstream in("/sys/devices/system/cpu/cpu0/cache/index" + std::to_string(idx) + "/size");
        std::string s;
        if (!(in >> s) || s.empty()) continue;
        size_t mult = 1;
        if (s.back() == 'K') mult = 1024;
        else if (s.back() == 'M') mult = 1024 * 1024;
        if (mult != 1) s.pop_back();
        return static_cast<size_t>(std::strtoull(s.c_str(), nullptr, 10)) * mult;
    }
    return 8u * 1024 * 1024;
}

CacheAllocation::~CacheAllocation() {
    release();
}

#ifdef BALANCIFY_HAS_PQOS

size_t CacheAllocation::apply(double megabytes, unsigned cos, unsigned way_offset, bool isolate_others,
                              const std::string& interface, const std::vector<unsigned>& cpus) {
    if (megabytes <= 0.0) {
        return 0;
    }
    pqos_config cfg{};
    cfg.fd_log = STDERR_FILENO;
    cfg.verbose = 0;
    cfg.interface = interface == "msr" ? PQOS_INTER_MSR : PQOS_INTER_OS;
    if (pqos_init(&cfg) != PQOS_RETVAL_OK) {
        std::cerr << "[cat] pqos_init failed; continuing without a cache partition" << std::endl;
        return 0;
    }
    const pqos_cap* cap = nullptr;
    const pqos_cpuinfo* cpu = nullptr;
    const pqos_capability* l3 = nullptr;
    if (pqos_cap_get(&cap, &cpu) != PQOS_RETVAL_OK ||
        pqos_cap_get_type(cap, PQOS_CAP_TYPE_L3CA, &l3) != PQOS_RETVAL_OK || l3 == nullptr) {
        std::cerr << "[cat] L3 cache allocation is not supported on this processor" << std::endl;
        pqos_fini();
        return 0;
    }
    const unsigned num_ways = l3->u.l3ca->num_ways;
    const unsigned way_size = l3->u.l3ca->way_size;
    unsigned ways = static_cast<unsigned>(std::ceil(megabytes * 1024.0 * 1024.0 / way_size));
    ways = std::max(1u, std::min(ways, num_ways));
    if (way_offset + ways > num_ways) {
        way_offset = num_ways - ways;
    }
    full_mask_ = (num_ways >= 64) ? ~0ULL : ((1ULL << num_ways) - 1);
    const uint64_t mask = ((1ULL << ways) - 1) << way_offset;

    unsigned count = 0;
    unsigned* ids = pqos_cpu_get_sockets(cpu, &count);
    if (ids == nullptr || count == 0) {
        std::cerr << "[cat] cannot enumerate sockets" << std::endl;
        std::free(ids);
        pqos_fini();
        return 0;
    }
    l3_ids_.assign(ids, ids + count);
    std::free(ids);

    for (unsigned id : l3_ids_) {
        pqos_l3ca ca{};
        ca.class_id = cos;
        ca.cdp = 0;
        ca.u.ways_mask = mask;
        if (pqos_l3ca_set(id, 1, &ca) != PQOS_RETVAL_OK) {
            std::cerr << "[cat] cannot program class " << cos << std::endl;
            pqos_fini();
            return 0;
        }
        if (isolate_others && mask != full_mask_) {
            // Keep every other workload (class 0) out of the reserved ways.
            pqos_l3ca def{};
            def.class_id = 0;
            def.u.ways_mask = full_mask_ & ~mask;
            if (pqos_l3ca_set(id, 1, &def) == PQOS_RETVAL_OK) isolated_ = true;
        }
    }
    for (unsigned c : cpus) {
        if (pqos_alloc_assoc_set(c, cos) != PQOS_RETVAL_OK) {
            std::cerr << "[cat] cannot associate core " << c << " with class " << cos << std::endl;
        }
    }
    cpus_ = cpus;
    cos_ = cos;
    active_ = true;
    const size_t reserved = size_t(ways) * way_size;
    std::cout << "[cat] reserved " << ways << " of " << num_ways << " ways (" << reserved / 1024
              << " KB, mask 0x" << std::hex << mask << std::dec << ") in class " << cos << std::endl;
    return reserved;
}

void CacheAllocation::release() {
    if (!active_) return;
    for (unsigned c : cpus_) {
        pqos_alloc_assoc_set(c, 0);
    }
    for (unsigned id : l3_ids_) {
        pqos_l3ca ca{};
        ca.class_id = cos_;
        ca.u.ways_mask = full_mask_;
        pqos_l3ca_set(id, 1, &ca);
        if (isolated_) {
            pqos_l3ca def{};
            def.class_id = 0;
            def.u.ways_mask = full_mask_;
            pqos_l3ca_set(id, 1, &def);
        }
    }
    pqos_fini();
    active_ = false;
}

#else

size_t CacheAllocation::apply(double megabytes, unsigned, unsigned, bool, const std::string&,
                              const std::vector<unsigned>&) {
    if (megabytes > 0.0) {
        std::cerr << "[cat] built without libpqos; no cache partition is reserved" << std::endl;
    }
    return 0;
}

void CacheAllocation::release() {
    active_ = false;
}

#endif

} // namespace balancify::lb
