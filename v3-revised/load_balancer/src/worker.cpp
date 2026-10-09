#include "worker.hpp"

#include "override_policy.hpp"

#include <rte_arp.h>
#include <rte_cycles.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_malloc.h>
#include <rte_prefetch.h>
#include <rte_random.h>
#include <rte_tcp.h>
#include <rte_udp.h>

#include <netinet/in.h>

#include <algorithm>
#include <iostream>

namespace balancify::lb {

namespace {

constexpr unsigned kPrefetchAhead = 4;
constexpr unsigned kControlEvery = 16;   // loop iterations between control-ring polls
constexpr unsigned kPublishEvery = 1024; // loop iterations between counter publications
constexpr size_t kMaxStoreRetries = 65536;

} // namespace

Worker::Worker(Runtime& rt, unsigned index, uint16_t port, uint16_t queue)
    : rt_(rt), cfg_(rt.cfg), idx_(index), port_(port), queue_(queue) {
    idle_timeout_ms_ = cfg_.idle_timeout_s * 1000u;
    fin_linger_ms_ = cfg_.fin_linger_ms;
}

Worker::~Worker() {
    rte_free(txb_);
}

void Worker::drop(rte_mbuf* m) {
    ++c_.dropped;
    rte_pktmbuf_free(m);
}

bool Worker::encapsulate(rte_mbuf* m, uint16_t dip) {
    const DipSlot& d = (*rt_.pool)[dip];
    const auto* inner = rte_pktmbuf_mtod_offset(m, const rte_ipv4_hdr*, sizeof(rte_ether_hdr));
    const uint16_t inner_len = rte_be_to_cpu_16(inner->total_length);
    const uint8_t tos = inner->type_of_service;

    auto* eth = reinterpret_cast<rte_ether_hdr*>(rte_pktmbuf_prepend(m, sizeof(rte_ipv4_hdr)));
    if (eth == nullptr) {
        return false;
    }
    auto* outer = reinterpret_cast<rte_ipv4_hdr*>(eth + 1);
    rte_ether_addr_copy(&d.mac, &eth->dst_addr);
    rte_ether_addr_copy(&rt_.port_mac, &eth->src_addr);
    eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

    outer->version_ihl = RTE_IPV4_VHL_DEF;
    outer->type_of_service = tos;
    outer->total_length = rte_cpu_to_be_16(static_cast<uint16_t>(inner_len + sizeof(rte_ipv4_hdr)));
    outer->packet_id = 0;
    outer->fragment_offset = 0;
    outer->time_to_live = 64;
    outer->next_proto_id = IPPROTO_IPIP;
    outer->src_addr = cfg_.local_ip_be;
    outer->dst_addr = d.ip_be;
    outer->hdr_checksum = 0;
    if (rt_.tx_ip_cksum_offload) {
        m->l2_len = sizeof(rte_ether_hdr);
        m->l3_len = sizeof(rte_ipv4_hdr);
        m->ol_flags |= RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM;
    } else {
        outer->hdr_checksum = rte_ipv4_cksum(outer);
    }
    return true;
}

void Worker::forward(rte_mbuf* m, uint16_t dip) {
    if (dip >= rt_.pool->size() || !encapsulate(m, dip)) {
        drop(m);
        return;
    }
    ++c_.tx_pkts;
    rte_eth_tx_buffer(port_, queue_, txb_, m);
}

void Worker::reply_arp(rte_mbuf* m) {
    if (rte_pktmbuf_data_len(m) < sizeof(rte_ether_hdr) + sizeof(rte_arp_hdr)) {
        drop(m);
        return;
    }
    auto* eth = rte_pktmbuf_mtod(m, rte_ether_hdr*);
    auto* arp = reinterpret_cast<rte_arp_hdr*>(eth + 1);
    if (arp->arp_opcode != rte_cpu_to_be_16(RTE_ARP_OP_REQUEST) || arp->arp_data.arp_tip != cfg_.local_ip_be) {
        drop(m);
        return;
    }
    arp->arp_opcode = rte_cpu_to_be_16(RTE_ARP_OP_REPLY);
    rte_ether_addr_copy(&arp->arp_data.arp_sha, &arp->arp_data.arp_tha);
    arp->arp_data.arp_tip = arp->arp_data.arp_sip;
    rte_ether_addr_copy(&rt_.port_mac, &arp->arp_data.arp_sha);
    arp->arp_data.arp_sip = cfg_.local_ip_be;
    rte_ether_addr_copy(&eth->src_addr, &eth->dst_addr);
    rte_ether_addr_copy(&rt_.port_mac, &eth->src_addr);
    ++c_.arp_replies;
    rte_eth_tx_buffer(port_, queue_, txb_, m);
}

void Worker::request_store(ZkReqType type, const FiveTuple& t, uint16_t dip) {
    if (!store_) return;
    ZkReq r{};
    r.tuple = t;
    r.dip_ip_be = dip < rt_.pool->size() ? (*rt_.pool)[dip].ip_be : 0;
    r.worker = static_cast<uint16_t>(idx_);
    r.type = type;
    if (rte_ring_mp_enqueue_elem(rt_.zk_requests, &r, sizeof(ZkReq)) != 0 && store_retry_.size() < kMaxStoreRetries) {
        store_retry_.push_back(r);
    }
}

void Worker::hold_for_store(rte_mbuf* m, const FiveTuple& t, uint16_t dip) {
    // The override is written to the coordination store before the first
    // packet leaves; the coordination thread hands the packet back through
    // this core's release ring when the write completes.
    ZkReq r{};
    r.tuple = t;
    r.held = m;
    r.dip_ip_be = (*rt_.pool)[dip].ip_be;
    r.worker = static_cast<uint16_t>(idx_);
    r.type = ZkReqType::Create;
    if (rte_ring_mp_enqueue_elem(rt_.zk_requests, &r, sizeof(ZkReq)) == 0) {
        ++c_.held_syn;
        return;
    }
    ++c_.zk_fail_open;
    r.held = nullptr;
    if (store_retry_.size() < kMaxStoreRetries) store_retry_.push_back(r);
    ++c_.tx_pkts;
    rte_eth_tx_buffer(port_, queue_, txb_, m);
}

void Worker::flush_store_retries() {
    size_t done = 0;
    for (; done < store_retry_.size(); ++done) {
        if (rte_ring_mp_enqueue_elem(rt_.zk_requests, &store_retry_[done], sizeof(ZkReq)) != 0) break;
    }
    store_retry_.erase(store_retry_.begin(), store_retry_.begin() + static_cast<long>(done));
}

void Worker::remove_entry(const ConnTable::Entry& e, uint64_t h, uint64_t& reason) {
    const uint32_t meta = e.meta();
    if (!(meta & ConnTable::kFlagRemote)) {
        request_store(ZkReqType::Delete, *e.key, e.dip());
    }
    if (bloom_.active()) bloom_.remove(h);
    table_.erase(e);
    ++reason;
}

void Worker::process(rte_mbuf* m, const HashPath* hp, const LoadSnapshot* ls, double threshold, TargetRule rule) {
    const uint32_t len = rte_pktmbuf_data_len(m);
    if (len < sizeof(rte_ether_hdr) + sizeof(rte_ipv4_hdr)) {
        drop(m);
        return;
    }
    auto* eth = rte_pktmbuf_mtod(m, rte_ether_hdr*);
    if (eth->ether_type == rte_cpu_to_be_16(RTE_ETHER_TYPE_ARP)) {
        reply_arp(m);
        return;
    }
    if (eth->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4)) {
        drop(m);
        return;
    }
    auto* ip = reinterpret_cast<rte_ipv4_hdr*>(eth + 1);
    if (ip->dst_addr != cfg_.vip_be) {
        ++c_.non_vip;
        rte_pktmbuf_free(m);
        return;
    }

    FiveTuple t{};
    t.src_ip = ip->src_addr;
    t.dst_ip = ip->dst_addr;
    t.proto = ip->next_proto_id;
    const unsigned ihl = (ip->version_ihl & RTE_IPV4_HDR_IHL_MASK) * RTE_IPV4_IHL_MULTIPLIER;
    const bool first_fragment = (rte_be_to_cpu_16(ip->fragment_offset) & RTE_IPV4_HDR_OFFSET_MASK) == 0;
    const size_t l4_off = sizeof(rte_ether_hdr) + ihl;
    uint8_t tcp_flags = 0;
    bool tcp = false;
    if (first_fragment) {
        if (t.proto == IPPROTO_TCP && len >= l4_off + sizeof(rte_tcp_hdr)) {
            const auto* th = rte_pktmbuf_mtod_offset(m, const rte_tcp_hdr*, l4_off);
            t.src_port = th->src_port;
            t.dst_port = th->dst_port;
            tcp_flags = th->tcp_flags;
            tcp = true;
        } else if (t.proto == IPPROTO_UDP && len >= l4_off + sizeof(rte_udp_hdr)) {
            const auto* uh = rte_pktmbuf_mtod_offset(m, const rte_udp_hdr*, l4_off);
            t.src_port = uh->src_port;
            t.dst_port = uh->dst_port;
        }
    }
    if (!rt_.all_ports && !rt_.allowed_ports.test(rte_be_to_cpu_16(t.dst_port))) {
        drop(m);
        return;
    }

    // Algorithm 2: the connection-to-DIP table first, consistent hashing on a miss.
    const uint64_t h = hash_tuple(t, kSeedTable);
    uint16_t dip = kInvalidDip;
    bool probe = cfg_.mode != Mode::Stateless;
    if (probe && bloom_.active()) {
        if (bloom_.may_contain(h)) {
            ++c_.bloom_positives;
        } else {
            ++c_.bloom_negatives;
            probe = false;
        }
    }
    if (probe) {
        const ConnTable::Entry e = table_.find(t, h);
        if (e) {
            ++c_.table_hits;
            dip = e.dip();
            uint32_t flags = e.meta() & ~ConnTable::kTimeMask;
            if (flags & ConnTable::kFlagRemote) {
                // Traffic for a binding made elsewhere reached this instance: ownership follows it.
                flags &= ~ConnTable::kFlagRemote;
                request_store(ZkReqType::Adopt, t, dip);
                ++c_.adopted;
            }
            if (tcp_flags & RTE_TCP_FIN_FLAG) {
                flags |= ConnTable::kFlagFin;
            }
            e.meta() = (now_ms_ & ConnTable::kTimeMask) | flags;
            if (tcp_flags & RTE_TCP_RST_FLAG) {
                remove_entry(e, h, c_.reclaimed_rst);
            }
        } else if (bloom_.active()) {
            ++c_.bloom_false_positives;
        }
    }

    if (dip == kInvalidDip) {
        const uint16_t hashed = hp != nullptr ? hp->lookup(hash_tuple(t, kSeedStable)) : kInvalidDip;
        dip = hashed;
        const bool syn = tcp && (tcp_flags & (RTE_TCP_SYN_FLAG | RTE_TCP_ACK_FLAG)) == RTE_TCP_SYN_FLAG;
        if (syn && cfg_.mode != Mode::Stateless) {
            ++c_.new_conns;
            const OverrideDecision d = decide(hashed, ls, cfg_.mode, threshold, rule, *rt_.pending, rte_rand());
            if (!d.pinned) {
                ++c_.hashed_new;
            } else if (table_.insert(t, h, d.dip, now_ms_ & ConnTable::kTimeMask)) {
                if (bloom_.active()) bloom_.add(h);
                ++c_.overrides;
                dip = d.dip;
                if (store_) {
                    if (dip >= rt_.pool->size() || !encapsulate(m, dip)) {
                        drop(m);
                        return;
                    }
                    hold_for_store(m, t, dip);
                    return;
                }
            } else {
                ++c_.insert_failures;
            }
        }
    }
    forward(m, dip);
}

void Worker::expire(unsigned nbuckets) {
    table_.sweep(nbuckets, [&](const FiveTuple& key, uint16_t dip, uint32_t meta) {
        const uint32_t age = (now_ms_ - (meta & ConnTable::kTimeMask)) & ConnTable::kTimeMask;
        const bool fin = (meta & ConnTable::kFlagFin) != 0;
        if (!(fin && age >= fin_linger_ms_) && age < idle_timeout_ms_) {
            return false;
        }
        if (!(meta & ConnTable::kFlagRemote)) {
            request_store(ZkReqType::Delete, key, dip);
        }
        if (bloom_.active()) bloom_.remove(hash_tuple(key, kSeedTable));
        ++(fin ? c_.reclaimed_fin : c_.reclaimed_idle);
        return true;
    });
}

void Worker::drain_control() {
    CoreMsg msgs[32];
    const unsigned n = rte_ring_sc_dequeue_burst_elem(rt_.ctrl_rings[idx_], msgs, sizeof(CoreMsg), 32, nullptr);
    for (unsigned i = 0; i < n; ++i) {
        const CoreMsg& msg = msgs[i];
        switch (msg.type) {
        case CoreMsgType::BindRemote: {
            const uint64_t h = hash_tuple(msg.tuple, kSeedTable);
            const ConnTable::Entry e = table_.find(msg.tuple, h);
            if (e) {
                e.set_dip(msg.dip);
                e.meta() |= ConnTable::kFlagRemote;
            } else if (table_.insert(msg.tuple, h, msg.dip, (now_ms_ & ConnTable::kTimeMask) | ConnTable::kFlagRemote)) {
                if (bloom_.active()) bloom_.add(h);
            } else {
                ++c_.insert_failures;
                break;
            }
            ++c_.remote_bound;
            break;
        }
        case CoreMsgType::UnbindRemote: {
            const uint64_t h = hash_tuple(msg.tuple, kSeedTable);
            const ConnTable::Entry e = table_.find(msg.tuple, h);
            if (e && (e.meta() & ConnTable::kFlagRemote)) {
                if (bloom_.active()) bloom_.remove(h);
                table_.erase(e);
                ++c_.remote_unbound;
            }
            break;
        }
        case CoreMsgType::PurgeDip:
            table_.for_each([&](const ConnTable::Entry& e) {
                if (e.dip() != msg.dip) return false;
                if (!(e.meta() & ConnTable::kFlagRemote)) {
                    request_store(ZkReqType::Delete, *e.key, e.dip());
                }
                if (bloom_.active()) bloom_.remove(hash_tuple(*e.key, kSeedTable));
                ++c_.purged;
                return true;
            });
            break;
        case CoreMsgType::DisownAll:
            table_.for_each([&](const ConnTable::Entry& e) {
                e.meta() |= ConnTable::kFlagRemote;
                return false;
            });
            break;
        }
    }
}

void Worker::drain_released() {
    rte_mbuf* pkts[kBurst];
    const unsigned n = rte_ring_sc_dequeue_burst(rt_.release_rings[idx_], reinterpret_cast<void**>(pkts), kBurst, nullptr);
    for (unsigned i = 0; i < n; ++i) {
        ++c_.released_syn;
        ++c_.tx_pkts;
        rte_eth_tx_buffer(port_, queue_, txb_, pkts[i]);
    }
}

void Worker::rebuild_bloom() {
    const size_t design = std::max<size_t>(table_.size() * 2, 4096);
    if (!bloom_.init(design, cfg_.bloom_fp_rate, socket_)) {
        std::cerr << "[worker " << idx_ << "] cannot allocate the Bloom filter" << std::endl;
        return;
    }
    table_.for_each([&](const ConnTable::Entry& e) {
        bloom_.add(hash_tuple(*e.key, kSeedTable));
        return false;
    });
    ++c_.bloom_rebuilds;
}

void Worker::update_bloom_policy() {
    switch (cfg_.bloom) {
    case BloomPolicy::Off:
        if (bloom_.active()) bloom_.destroy();
        return;
    case BloomPolicy::On:
        if (!bloom_.active()) rebuild_bloom();
        break;
    case BloomPolicy::Auto:
        // The filter is built once the table no longer fits in this core's
        // cache budget, and dropped when the table shrinks back into it.
        if (!bloom_.active() && table_.beyond_budget()) {
            rebuild_bloom();
        } else if (bloom_.active() && !table_.beyond_budget() && !table_.migrating()) {
            bloom_.destroy();
        }
        break;
    }
    if (bloom_.active() && table_.size() > bloom_.design_entries()) {
        rebuild_bloom();
    }
}

void Worker::publish() {
    c_.tx_dropped = tx_buffer_drops_;
    c_.table_entries = table_.size();
    c_.table_bytes = table_.bytes();
    c_.table_resizes = table_.resizes();
    c_.table_lost = table_.lost_in_migration();
    c_.bloom_active = bloom_.active() ? 1 : 0;
    c_.bloom_bytes = bloom_.bytes();
    (*rt_.stats)[idx_].publish(c_);
}

int Worker::run() {
    socket_ = static_cast<int>(rte_socket_id());
    tsc_per_ms_ = std::max<uint64_t>(1, rte_get_tsc_hz() / 1000);
    store_ = rt_.zk_enabled.load();

    if (!table_.init(rt_.table_budget_bytes, socket_)) {
        std::cerr << "[worker " << idx_ << "] cannot allocate the connection table" << std::endl;
        return -1;
    }
    txb_ = static_cast<rte_eth_dev_tx_buffer*>(
        rte_zmalloc_socket("bfy_txb", RTE_ETH_TX_BUFFER_SIZE(kBurst * 2), 0, socket_));
    if (txb_ == nullptr) {
        std::cerr << "[worker " << idx_ << "] cannot allocate the TX buffer" << std::endl;
        return -1;
    }
    rte_eth_tx_buffer_init(txb_, kBurst * 2);
    rte_eth_tx_buffer_set_err_callback(txb_, rte_eth_tx_buffer_count_callback, &tx_buffer_drops_);

    rte_rcu_qsbr_thread_register(rt_.qsbr, idx_);
    rte_rcu_qsbr_thread_online(rt_.qsbr, idx_);
    update_bloom_policy();

    rte_mbuf* pkts[kBurst];
    uint64_t loops = 0;
    while (!rt_.stop.load(std::memory_order_relaxed)) {
        const uint64_t t0 = rte_rdtsc();
        now_ms_ = static_cast<uint32_t>(t0 / tsc_per_ms_);
        const uint16_t n = rte_eth_rx_burst(port_, queue_, pkts, kBurst);
        if (n != 0) {
            const HashPath* hp = rt_.hash_path.load(std::memory_order_acquire);
            const LoadSnapshot* ls = rt_.load.load(std::memory_order_acquire);
            const double threshold = rt_.threshold.load(std::memory_order_relaxed);
            const auto rule = static_cast<TargetRule>(rt_.target_rule.load(std::memory_order_relaxed));
            for (unsigned i = 0; i < n && i < kPrefetchAhead; ++i) {
                rte_prefetch0(rte_pktmbuf_mtod(pkts[i], void*));
            }
            for (unsigned i = 0; i < n; ++i) {
                if (i + kPrefetchAhead < n) {
                    rte_prefetch0(rte_pktmbuf_mtod(pkts[i + kPrefetchAhead], void*));
                }
                process(pkts[i], hp, ls, threshold, rule);
            }
            // A bounded number of buckets migrated per packet during a resize.
            table_.migrate(n);
            expire(n / 4 + 1);
            c_.rx_pkts += n;
            c_.busy_pkts += n;
            c_.busy_cycles += rte_rdtsc() - t0;
        } else {
            expire(4);
        }
        rte_eth_tx_buffer_flush(port_, queue_, txb_);

        ++loops;
        if (store_) drain_released();
        if (loops % kControlEvery == 0) {
            drain_control();
            if (!store_retry_.empty()) flush_store_retries();
            update_bloom_policy();
        }
        if (loops % kPublishEvery == 0) publish();
        rte_rcu_qsbr_quiescent(rt_.qsbr, idx_);
    }

    if (store_) drain_released();
    rte_eth_tx_buffer_flush(port_, queue_, txb_);
    publish();
    rte_rcu_qsbr_thread_offline(rt_.qsbr, idx_);
    rte_rcu_qsbr_thread_unregister(rt_.qsbr, idx_);
    return 0;
}

} // namespace balancify::lb
