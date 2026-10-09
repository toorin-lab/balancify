#pragma once

#include "config.hpp"
#include "dip_pool.hpp"
#include "snapshots.hpp"
#include "types.hpp"

#include <rte_ether.h>
#include <rte_malloc.h>
#include <rte_rcu_qsbr.h>
#include <rte_ring.h>

#include <array>
#include <atomic>
#include <bitset>
#include <functional>
#include <memory>

struct rte_mbuf;

namespace balancify::lb {

#define BALANCIFY_WORKER_COUNTERS(X)                                  \
    X(rx_pkts) X(tx_pkts) X(tx_dropped) X(dropped) X(non_vip)         \
    X(arp_replies) X(new_conns) X(overrides) X(hashed_new)            \
    X(table_hits) X(bloom_negatives) X(bloom_positives)               \
    X(bloom_false_positives) X(held_syn) X(released_syn)              \
    X(zk_fail_open) X(adopted) X(reclaimed_fin) X(reclaimed_rst)      \
    X(reclaimed_idle) X(purged) X(remote_bound) X(remote_unbound)     \
    X(insert_failures) X(busy_cycles) X(busy_pkts)                    \
    X(table_entries) X(table_bytes) X(table_resizes) X(table_lost)    \
    X(bloom_active) X(bloom_bytes) X(bloom_rebuilds)

// Plain counters owned by one forwarding core.
struct Counters {
#define BALANCIFY_DECLARE_PLAIN(name) uint64_t name = 0;
    BALANCIFY_WORKER_COUNTERS(BALANCIFY_DECLARE_PLAIN)
#undef BALANCIFY_DECLARE_PLAIN
};

// Published copy of a core's counters, read by the control threads.
struct alignas(64) WorkerStats {
#define BALANCIFY_DECLARE_ATOMIC(name) std::atomic<uint64_t> name{0};
    BALANCIFY_WORKER_COUNTERS(BALANCIFY_DECLARE_ATOMIC)
#undef BALANCIFY_DECLARE_ATOMIC

    void publish(const Counters& c) {
#define BALANCIFY_STORE(name) name.store(c.name, std::memory_order_relaxed);
        BALANCIFY_WORKER_COUNTERS(BALANCIFY_STORE)
#undef BALANCIFY_STORE
    }

    Counters read() const {
        Counters c;
#define BALANCIFY_LOAD(name) c.name = name.load(std::memory_order_relaxed);
        BALANCIFY_WORKER_COUNTERS(BALANCIFY_LOAD)
#undef BALANCIFY_LOAD
        return c;
    }
};

// Control messages delivered to the core that owns a connection.
enum class CoreMsgType : uint8_t {
    BindRemote,    // install or update a binding owned by another instance
    UnbindRemote,  // drop a binding owned by another instance
    PurgeDip,      // drop every entry that points to a DIP leaving the pool
    DisownAll,     // the coordination session was lost: local bindings are no longer ours
};

struct CoreMsg {
    FiveTuple tuple;
    uint16_t dip{kInvalidDip};
    CoreMsgType type{CoreMsgType::BindRemote};
    uint8_t pad[13]{};
};
static_assert(sizeof(CoreMsg) == 32);

// Requests from the forwarding cores to the coordination thread.
enum class ZkReqType : uint8_t {
    Create,  // new override; `held` is the first packet, forwarded once the write completes
    Delete,  // an owned binding ended (FIN, RST, inactivity, or DIP removal)
    Adopt,   // traffic of a remote binding reached this instance; take ownership
};

struct ZkReq {
    FiveTuple tuple;
    rte_mbuf* held{nullptr};
    uint32_t dip_ip_be{0};
    uint16_t worker{0};
    ZkReqType type{ZkReqType::Create};
    uint8_t pad{0};
};
static_assert(sizeof(ZkReq) == 32);

// State shared by the forwarding cores and the control threads.
struct Runtime {
    explicit Runtime(Config c);
    ~Runtime();
    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    Config cfg;
    std::unique_ptr<DipPool> pool;

    rte_rcu_qsbr* qsbr{nullptr};
    std::atomic<HashPath*> hash_path{nullptr};
    std::atomic<LoadSnapshot*> load{nullptr};
    PendingLoad* pending{nullptr};

    // Parameters adjustable at run time through the control API.
    std::atomic<double> threshold;
    std::atomic<uint32_t> sampling_interval_ms;
    std::atomic<uint8_t> target_rule;

    unsigned nworkers{0};
    std::array<unsigned, kMaxWorkers> worker_lcores{};
    std::array<rte_ring*, kMaxWorkers> ctrl_rings{};
    std::array<rte_ring*, kMaxWorkers> release_rings{};
    rte_ring* zk_requests{nullptr};
    std::unique_ptr<std::array<WorkerStats, kMaxWorkers>> stats;

    std::bitset<65536> allowed_ports;
    bool all_ports{true};
    rte_ether_addr port_mac{};
    bool tx_ip_cksum_offload{false};
    size_t cache_reserved_bytes{0};
    size_t table_budget_bytes{0};

    std::atomic<bool> zk_enabled{false};
    std::atomic<bool> ready{false};
    std::atomic<bool> shutdown_requested{false};
    std::atomic<bool> stop{false};

    // Maps a 5-tuple to the forwarding core whose RSS queue receives it.
    std::function<unsigned(const FiveTuple&)> owner_worker;

    void init_workers_from_eal();
    void init_rcu();
    void compute_table_budget(size_t reserved_bytes, size_t llc_bytes);

    bool send_to_worker(unsigned worker, const CoreMsg& msg);
    void send_to_owner(const CoreMsg& msg);
    void broadcast(const CoreMsg& msg);
};

// Swaps an RCU-protected pointer and frees the previous object once every
// forwarding core has passed a quiescent state.
template <class T>
void rcu_replace(Runtime& rt, std::atomic<T*>& slot, T* fresh) {
    T* old = slot.exchange(fresh, std::memory_order_acq_rel);
    if (old != nullptr) {
        rte_rcu_qsbr_synchronize(rt.qsbr, RTE_QSBR_THRID_INVALID);
        rte_free(old);
    }
}

} // namespace balancify::lb
