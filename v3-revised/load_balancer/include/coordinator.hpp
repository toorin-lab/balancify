#pragma once

#include "hash_path.hpp"
#include "runtime.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace balancify::lb {

// Shares state among the load-balancer instances of one VIP through ZooKeeper:
//
//   <root>/<service>/instances/<owner>         ephemeral, one per live instance
//   <root>/<service>/election/n_<seq>          ephemeral sequential; lowest is the leader
//   <root>/<service>/buckets                   stable-hashing bucket table (leader writes)
//   <root>/<service>/bindings/<xx>/<tuple>.<dip>.<owner>
//                                              one znode per override, 256 shards
//
// Every override is written before the first packet of its connection is
// forwarded. All instances watch the shards and keep a local, cache-resident
// copy of every binding in their forwarding cores' tables. A binding is
// removed by its owner on FIN, RST or inactivity. When traffic of a binding
// shows up at another instance (ECMP remap), that instance adopts it; bindings
// of a dead owner that nobody adopts are collected by the leader.
class Coordinator {
public:
    struct Stats {
        bool connected{false};
        bool leader{false};
        bool synced{false};
        std::string owner_id;
        double sync_ms{0.0};
        uint64_t creates{0};
        uint64_t create_errors{0};
        uint64_t deletes{0};
        uint64_t adoptions{0};
        uint64_t held{0};
        uint64_t released{0};
        uint64_t timeouts{0};
        uint64_t fail_open{0};
        uint64_t gc_deletes{0};
        uint64_t session_restarts{0};
        uint64_t bucket_publishes{0};
        uint64_t bindings{0};
        uint64_t orphans{0};
        uint64_t live_instances{0};
    };

    Coordinator(Runtime& rt, HashPathManager& hpm, std::function<std::vector<uint16_t>()> eligible);
    ~Coordinator();
    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

    // Connects, registers, and blocks until the local copy of every binding
    // and of the bucket table has been loaded into the forwarding cores.
    bool start();
    void stop();

    bool leader() const;
    // Writes the current bucket table to the store (leader only).
    void publish_buckets();
    // Lets the leader collect the bindings of dead owners that point to removed DIPs.
    void on_dips_removed(const std::vector<uint16_t>& removed);

    Stats stats() const;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

} // namespace balancify::lb
