#include "dataplane.hpp"

#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ring.h>
#include <rte_thash.h>

#include <netinet/in.h>

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <string>

namespace balancify::lb {

namespace {

constexpr unsigned kCtrlRingSize = 8192;
constexpr unsigned kReleaseRingSize = 4096;
constexpr unsigned kStoreRingSize = 65536;
constexpr unsigned kMempoolCache = 256;

} // namespace

Dataplane::Dataplane(Runtime& rt) : rt_(rt) {}

Dataplane::~Dataplane() {
    stop();
}

void Dataplane::create_rings() {
    for (unsigned w = 0; w < rt_.nworkers; ++w) {
        const int socket = static_cast<int>(rte_lcore_to_socket_id(rt_.worker_lcores[w]));
        const std::string ctrl = "bfy_ctrl_" + std::to_string(w);
        rt_.ctrl_rings[w] = rte_ring_create_elem(ctrl.c_str(), sizeof(CoreMsg), kCtrlRingSize, socket, RING_F_SC_DEQ);
        const std::string rel = "bfy_rel_" + std::to_string(w);
        rt_.release_rings[w] = rte_ring_create(rel.c_str(), kReleaseRingSize, socket, RING_F_SC_DEQ);
        if (rt_.ctrl_rings[w] == nullptr || rt_.release_rings[w] == nullptr) {
            throw std::runtime_error("cannot create worker rings");
        }
    }
    rt_.zk_requests = rte_ring_create_elem("bfy_store_req", sizeof(ZkReq), kStoreRingSize,
                                           static_cast<int>(rte_socket_id()), RING_F_SC_DEQ);
    if (rt_.zk_requests == nullptr) {
        throw std::runtime_error("cannot create the coordination request ring");
    }
}

void Dataplane::init() {
    const uint16_t port = rt_.cfg.port_id;
    if (!rte_eth_dev_is_valid_port(port)) {
        throw std::runtime_error("invalid DPDK port " + std::to_string(port));
    }
    rte_eth_dev_info info{};
    if (rte_eth_dev_info_get(port, &info) != 0) {
        throw std::runtime_error("rte_eth_dev_info_get failed");
    }
    const uint16_t nq = static_cast<uint16_t>(rt_.nworkers);
    if (nq > info.max_rx_queues || nq > info.max_tx_queues) {
        throw std::runtime_error("port supports fewer queues than forwarding cores");
    }

    rte_eth_conf conf{};
    rss_key_.assign(info.hash_key_size != 0 ? info.hash_key_size : 40, 0);
    for (size_t i = 0; i < rss_key_.size(); ++i) {
        rss_key_[i] = (i % 2 == 0) ? 0x6d : 0x5a;
    }
    conf.rxmode.mq_mode = nq > 1 ? RTE_ETH_MQ_RX_RSS : RTE_ETH_MQ_RX_NONE;
    conf.rx_adv_conf.rss_conf.rss_key = rss_key_.data();
    conf.rx_adv_conf.rss_conf.rss_key_len = static_cast<uint8_t>(rss_key_.size());
    conf.rx_adv_conf.rss_conf.rss_hf =
        (RTE_ETH_RSS_IPV4 | RTE_ETH_RSS_NONFRAG_IPV4_TCP | RTE_ETH_RSS_NONFRAG_IPV4_UDP) & info.flow_type_rss_offloads;
    if (info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_IPV4_CKSUM) {
        conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_IPV4_CKSUM;
        rt_.tx_ip_cksum_offload = true;
    }
    if (info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE) {
        conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;
    }
    if (rte_eth_dev_configure(port, nq, nq, &conf) != 0) {
        throw std::runtime_error("rte_eth_dev_configure failed");
    }
    uint16_t nb_rxd = static_cast<uint16_t>(rt_.cfg.rx_desc);
    uint16_t nb_txd = static_cast<uint16_t>(rt_.cfg.tx_desc);
    rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);

    int port_socket = rte_eth_dev_socket_id(port);
    for (unsigned w = 0; w < rt_.nworkers; ++w) {
        const int socket = static_cast<int>(rte_lcore_to_socket_id(rt_.worker_lcores[w]));
        if (port_socket >= 0 && port_socket != socket) {
            std::cerr << "[dataplane] worker " << w << " runs on socket " << socket
                      << " but the port is attached to socket " << port_socket << std::endl;
        }
        const std::string name = "bfy_mbuf_" + std::to_string(w);
        const unsigned nmbufs = std::max<unsigned>(rt_.cfg.mbufs_per_worker, nb_rxd + nb_txd + 4 * kMempoolCache);
        rte_mempool* mp = rte_pktmbuf_pool_create(name.c_str(), nmbufs, kMempoolCache, 0,
                                                  RTE_MBUF_DEFAULT_BUF_SIZE, socket);
        if (mp == nullptr) {
            throw std::runtime_error("cannot create mbuf pool " + name);
        }
        pools_.push_back(mp);

        rte_eth_rxconf rxconf = info.default_rxconf;
        rte_eth_txconf txconf = info.default_txconf;
        txconf.offloads = conf.txmode.offloads;
        if (rte_eth_rx_queue_setup(port, static_cast<uint16_t>(w), nb_rxd, socket, &rxconf, mp) != 0 ||
            rte_eth_tx_queue_setup(port, static_cast<uint16_t>(w), nb_txd, socket, &txconf) != 0) {
            throw std::runtime_error("queue setup failed for queue " + std::to_string(w));
        }
    }

    if (rte_eth_dev_start(port) != 0) {
        throw std::runtime_error("rte_eth_dev_start failed");
    }
    started_ = true;
    rte_eth_macaddr_get(port, &rt_.port_mac);
    program_reta();
    create_rings();

    for (unsigned w = 0; w < rt_.nworkers; ++w) {
        workers_.push_back(std::make_unique<Worker>(rt_, w, port, static_cast<uint16_t>(w)));
    }
    rt_.owner_worker = [this](const FiveTuple& t) { return owner_worker(t); };
}

void Dataplane::program_reta() {
    const uint16_t port = rt_.cfg.port_id;
    rte_eth_dev_info info{};
    rte_eth_dev_info_get(port, &info);
    if (rt_.nworkers <= 1 || info.reta_size == 0) {
        reta_.assign(1, 0);
        return;
    }
    const uint16_t size = info.reta_size;
    std::vector<rte_eth_rss_reta_entry64> conf((size + RTE_ETH_RETA_GROUP_SIZE - 1) / RTE_ETH_RETA_GROUP_SIZE);
    reta_.resize(size);
    for (uint16_t i = 0; i < size; ++i) {
        const uint16_t q = static_cast<uint16_t>(i % rt_.nworkers);
        reta_[i] = q;
        conf[i / RTE_ETH_RETA_GROUP_SIZE].mask |= 1ULL << (i % RTE_ETH_RETA_GROUP_SIZE);
        conf[i / RTE_ETH_RETA_GROUP_SIZE].reta[i % RTE_ETH_RETA_GROUP_SIZE] = q;
    }
    if (rte_eth_dev_rss_reta_update(port, conf.data(), size) != 0) {
        // Fall back to querying what the driver installed.
        for (auto& c : conf) c.mask = ~0ULL;
        if (rte_eth_dev_rss_reta_query(port, conf.data(), size) == 0) {
            for (uint16_t i = 0; i < size; ++i) {
                reta_[i] = conf[i / RTE_ETH_RETA_GROUP_SIZE].reta[i % RTE_ETH_RETA_GROUP_SIZE];
            }
        }
    }
}

unsigned Dataplane::owner_worker(const FiveTuple& t) const {
    if (rt_.nworkers <= 1 || reta_.size() <= 1) {
        return 0;
    }
    rte_ipv4_tuple tuple{};
    tuple.src_addr = rte_be_to_cpu_32(t.src_ip);
    tuple.dst_addr = rte_be_to_cpu_32(t.dst_ip);
    tuple.sport = rte_be_to_cpu_16(t.src_port);
    tuple.dport = rte_be_to_cpu_16(t.dst_port);
    const uint32_t len = (t.proto == IPPROTO_TCP || t.proto == IPPROTO_UDP) ? RTE_THASH_V4_L4_LEN : RTE_THASH_V4_L3_LEN;
    const uint32_t h = rte_softrss(reinterpret_cast<uint32_t*>(&tuple), len, rss_key_.data());
    const unsigned q = reta_[h % reta_.size()];
    return q < rt_.nworkers ? q : 0;
}

void Dataplane::launch() {
    for (unsigned w = 0; w < rt_.nworkers; ++w) {
        if (rte_eal_remote_launch(&Worker::entry, workers_[w].get(), rt_.worker_lcores[w]) != 0) {
            throw std::runtime_error("cannot launch worker " + std::to_string(w));
        }
    }
    launched_ = true;
}

void Dataplane::stop() {
    if (launched_) {
        rt_.stop.store(true);
        for (unsigned w = 0; w < rt_.nworkers; ++w) {
            rte_eal_wait_lcore(rt_.worker_lcores[w]);
        }
        launched_ = false;
    }
    if (started_) {
        rte_eth_dev_stop(rt_.cfg.port_id);
        rte_eth_dev_close(rt_.cfg.port_id);
        started_ = false;
    }
}

} // namespace balancify::lb
