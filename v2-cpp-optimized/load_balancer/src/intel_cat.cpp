#include "intel_cat.hpp"

#include <stdexcept>
#include <unistd.h>

#ifdef BALANCIFY_HAS_PQOS
extern "C" {
#include <pqos.h>
}
#else
#include <iostream>
#endif

namespace balancify::lb {

IntelCatManager::~IntelCatManager() {
    release();
}

void IntelCatManager::configure(uint32_t mask, unsigned lcore) {
#ifdef BALANCIFY_HAS_PQOS
    struct pqos_cfg cfg = {};
    cfg.fd_log = STDOUT_FILENO;
    cfg.verbose = 0;
    cfg.interface = PQOS_INTER_MSR;

    if (pqos_init(&cfg) != PQOS_RETVAL_OK) {
        throw std::runtime_error("PQOS init failed");
    }

    uint32_t num_sockets = 0;
    unsigned* sockets = nullptr;
    if (pqos_cap_get_sockets(&num_sockets, &sockets) != PQOS_RETVAL_OK || num_sockets == 0) {
        pqos_fini();
        throw std::runtime_error("PQOS get sockets failed");
    }

    struct pqos_l3ca ca = {};
    ca.class_id = static_cast<unsigned>(lcore);
    ca.cdp = 0;
    ca.u.ways_mask = mask;

    if (pqos_l3ca_set(sockets[0], 1, &ca) != PQOS_RETVAL_OK) {
        pqos_fini();
        throw std::runtime_error("PQOS L3CA set failed");
    }

    initialized_ = true;
#else
    (void)mask;
    (void)lcore;
    std::cerr << "[IntelCatManager] libpqos not available; Intel CAT support disabled" << std::endl;
#endif
}

void IntelCatManager::release() {
#ifdef BALANCIFY_HAS_PQOS
    if (initialized_) {
        pqos_fini();
        initialized_ = false;
    }
#else
    initialized_ = false;
#endif
}

} // namespace balancify::lb

