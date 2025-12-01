#pragma once

#include "balancify_engine.hpp"
#include <atomic>
#include <thread>

extern "C" {
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_tcp.h>
#include <rte_udp.h>
}

namespace balancify::lb {

class DpdkContext {
public:
    DpdkContext();
    ~DpdkContext();

    void initialize(int argc, char** argv, uint16_t port_id);
    void run(BalancifyEngine& engine);
    void stop();

private:
    void rx_loop(BalancifyEngine& engine);
    static bool parse_tuple(struct rte_mbuf* mbuf, common::ConnectionTuple& tuple);
    static void rewrite_packet(struct rte_mbuf* mbuf, const common::ServerEndpoint& endpoint);

    uint16_t port_id_{0};
    std::atomic<bool> running_{false};
    std::thread worker_;
};

} // namespace balancify::lb

