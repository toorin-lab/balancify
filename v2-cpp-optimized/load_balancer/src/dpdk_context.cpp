#include "dpdk_context.hpp"

#include <cstring>
#include <iostream>

namespace balancify::lb {

namespace {
constexpr uint16_t RX_RING_SIZE = 1024;
constexpr uint16_t TX_RING_SIZE = 1024;
constexpr uint16_t BURST_SIZE = 32;
} // namespace

DpdkContext::DpdkContext() = default;

DpdkContext::~DpdkContext() {
    stop();
}

void DpdkContext::initialize(int argc, char** argv, uint16_t port_id) {
    port_id_ = port_id;
    if (rte_eal_init(argc, argv) < 0) {
        throw std::runtime_error("Failed to initialize DPDK EAL");
    }

    const uint16_t nb_ports = rte_eth_dev_count_avail();
    if (port_id_ >= nb_ports) {
        throw std::runtime_error("Invalid DPDK port id");
    }

    rte_eth_conf port_conf{};
    port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE;
    port_conf.txmode.mq_mode = RTE_ETH_MQ_TX_NONE;

    if (rte_eth_dev_configure(port_id_, 1, 1, &port_conf) < 0) {
        throw std::runtime_error("rte_eth_dev_configure failed");
    }
    auto* mbuf_pool = rte_pktmbuf_pool_create("RX_POOL", 8192, 256, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    if (!mbuf_pool) {
        throw std::runtime_error("failed to create mbuf pool");
    }
    if (rte_eth_rx_queue_setup(port_id_, 0, RX_RING_SIZE, rte_eth_dev_socket_id(port_id_), nullptr, mbuf_pool) < 0) {
        throw std::runtime_error("rx queue setup failed");
    }
    if (rte_eth_tx_queue_setup(port_id_, 0, TX_RING_SIZE, rte_eth_dev_socket_id(port_id_), nullptr) < 0) {
        throw std::runtime_error("tx queue setup failed");
    }
    if (rte_eth_dev_start(port_id_) < 0) {
        throw std::runtime_error("rte_eth_dev_start failed");
    }
}

void DpdkContext::run(BalancifyEngine& engine) {
    running_ = true;
    worker_ = std::thread(&DpdkContext::rx_loop, this, std::ref(engine));
}

void DpdkContext::stop() {
    running_ = false;
    if (worker_.joinable()) {
        worker_.join();
    }
    if (rte_eth_dev_is_valid_port(port_id_)) {
        rte_eth_dev_stop(port_id_);
        rte_eth_dev_close(port_id_);
    }
}

void DpdkContext::rx_loop(BalancifyEngine& engine) {
    rte_mbuf* bufs[BURST_SIZE];

    while (running_) {
        const uint16_t nb_rx = rte_eth_rx_burst(port_id_, 0, bufs, BURST_SIZE);
        if (nb_rx == 0) {
            continue;
        }

        for (uint16_t i = 0; i < nb_rx; ++i) {
            auto* mbuf = bufs[i];
            common::ConnectionTuple tuple{};
            if (parse_tuple(mbuf, tuple)) {
                auto decision = engine.route(tuple);
                if (decision.endpoint) {
                    rewrite_packet(mbuf, *decision.endpoint);
                }
            }
            rte_eth_tx_burst(port_id_, 0, &mbuf, 1);
        }
    }
}

bool DpdkContext::parse_tuple(struct rte_mbuf* mbuf, common::ConnectionTuple& tuple) {
    auto* eth = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr*);
    if (eth->ether_type != rte_be_to_cpu_16(RTE_ETHER_TYPE_IPV4)) {
        return false;
    }

    auto* ipv4 = reinterpret_cast<struct rte_ipv4_hdr*>(eth + 1);
    tuple.src_ip = {
        static_cast<uint8_t>(ipv4->src_addr & 0xFF),
        static_cast<uint8_t>((ipv4->src_addr >> 8) & 0xFF),
        static_cast<uint8_t>((ipv4->src_addr >> 16) & 0xFF),
        static_cast<uint8_t>((ipv4->src_addr >> 24) & 0xFF)};

    tuple.dst_ip = {
        static_cast<uint8_t>(ipv4->dst_addr & 0xFF),
        static_cast<uint8_t>((ipv4->dst_addr >> 8) & 0xFF),
        static_cast<uint8_t>((ipv4->dst_addr >> 16) & 0xFF),
        static_cast<uint8_t>((ipv4->dst_addr >> 24) & 0xFF)};

    tuple.protocol = ipv4->next_proto_id;

    if (tuple.protocol == IPPROTO_TCP) {
        auto* tcp = reinterpret_cast<struct rte_tcp_hdr*>(reinterpret_cast<uint8_t*>(ipv4) + sizeof(*ipv4));
        tuple.src_port = rte_be_to_cpu_16(tcp->src_port);
        tuple.dst_port = rte_be_to_cpu_16(tcp->dst_port);
    } else if (tuple.protocol == IPPROTO_UDP) {
        auto* udp = reinterpret_cast<struct rte_udp_hdr*>(reinterpret_cast<uint8_t*>(ipv4) + sizeof(*ipv4));
        tuple.src_port = rte_be_to_cpu_16(udp->src_port);
        tuple.dst_port = rte_be_to_cpu_16(udp->dst_port);
    } else {
        return false;
    }

    return true;
}

void DpdkContext::rewrite_packet(struct rte_mbuf* mbuf, const common::ServerEndpoint& endpoint) {
    auto* eth = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr*);
    std::memcpy(&eth->dst_addr, endpoint.mac.data(), RTE_ETHER_ADDR_LEN);

    auto* ipv4 = reinterpret_cast<struct rte_ipv4_hdr*>(eth + 1);
    uint32_t new_ip = (endpoint.ip[3] << 24) | (endpoint.ip[2] << 16) | (endpoint.ip[1] << 8) | endpoint.ip[0];
    ipv4->dst_addr = rte_cpu_to_be_32(new_ip);
}

} // namespace balancify::lb

