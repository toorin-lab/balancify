#pragma once

#include "conn_table.hpp"
#include "counting_bloom.hpp"
#include "runtime.hpp"

#include <rte_ethdev.h>
#include <rte_mbuf.h>

#include <cstdint>
#include <vector>

namespace balancify::lb {

// One forwarding core: polls its RSS queue, runs Algorithm 2 on every packet,
// and owns a private connection-to-DIP table and Bloom filter, so the fast
// path needs no cross-core synchronization.
class Worker {
public:
    static constexpr uint16_t kBurst = 32;

    Worker(Runtime& rt, unsigned index, uint16_t port, uint16_t queue);
    ~Worker();
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    static int entry(void* self) { return static_cast<Worker*>(self)->run(); }

private:
    int run();
    void process(rte_mbuf* m, const HashPath* hp, const LoadSnapshot* ls, double threshold, TargetRule rule);
    void forward(rte_mbuf* m, uint16_t dip);
    bool encapsulate(rte_mbuf* m, uint16_t dip);
    void reply_arp(rte_mbuf* m);
    void drop(rte_mbuf* m);
    void hold_for_store(rte_mbuf* m, const FiveTuple& t, uint16_t dip);
    void request_store(ZkReqType type, const FiveTuple& t, uint16_t dip);
    void remove_entry(const ConnTable::Entry& e, uint64_t h, uint64_t& reason);
    void expire(unsigned nbuckets);
    void drain_control();
    void drain_released();
    void flush_store_retries();
    void update_bloom_policy();
    void rebuild_bloom();
    void publish();

    Runtime& rt_;
    const Config& cfg_;
    const unsigned idx_;
    const uint16_t port_;
    const uint16_t queue_;
    int socket_{0};

    ConnTable table_;
    CountingBloom bloom_;
    rte_eth_dev_tx_buffer* txb_{nullptr};
    uint64_t tx_buffer_drops_{0};

    Counters c_;
    uint64_t tsc_per_ms_{1};
    uint32_t now_ms_{0};
    uint32_t idle_timeout_ms_{0};
    uint32_t fin_linger_ms_{0};
    bool store_{false};
    std::vector<ZkReq> store_retry_;
};

} // namespace balancify::lb
