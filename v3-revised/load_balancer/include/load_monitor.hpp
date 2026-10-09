#pragma once

#include "runtime.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace balancify::lb {

// Monitoring thread (Algorithm 1). Receives the agents' reports, declares a
// DIP unavailable when its reports stop for health_timeout_ms, and every tau
// seconds samples the load vector, computes the pool average and publishes a
// LoadSnapshot for the forwarding cores.
class LoadMonitor {
public:
    // eligible: the new member set; removed/added: the changes since the last call.
    using MembershipCallback = std::function<void(const std::vector<uint16_t>& eligible,
                                                  const std::vector<uint16_t>& removed,
                                                  const std::vector<uint16_t>& added)>;

    struct DipView {
        uint16_t index{0};
        bool healthy{false};
        bool eligible{false};
        bool admin_enabled{true};
        double sampled_load{0.0};
        double cpu{0.0};
        double latency_ms{0.0};
        uint32_t active_conns{0};
        double report_age_ms{-1.0};
        uint64_t reports{0};
    };

    LoadMonitor(Runtime& rt, MembershipCallback cb);
    ~LoadMonitor();

    // Publishes the initial snapshot and marks the configured DIPs eligible.
    void prime();
    void start();
    void stop();

    void request_reevaluation() { reevaluate_.store(true, std::memory_order_relaxed); }
    std::vector<uint16_t> eligible() const;
    std::vector<DipView> view() const;

    uint64_t snapshots() const { return snapshots_.load(std::memory_order_relaxed); }
    uint64_t oscillations() const { return oscillations_.load(std::memory_order_relaxed); }
    uint64_t reports() const { return reports_.load(std::memory_order_relaxed); }
    double last_avg() const { return last_avg_.load(std::memory_order_relaxed); }
    double last_stddev() const { return last_stddev_.load(std::memory_order_relaxed); }
    double last_mu() const { return last_mu_.load(std::memory_order_relaxed); }

private:
    struct DipState {
        uint64_t last_report_ns{0};
        bool reported_healthy{true};
        bool healthy{true};
        double cpu{0.0};
        double latency_ms{0.0};
        uint32_t active_conns{0};
        double sum{0.0};
        uint32_t n{0};
        double sampled{0.0};
        uint64_t reports{0};
    };

    void run();
    void receive(int fd, uint64_t now_ns);
    void sync_pool_size(uint64_t now_ns);
    bool health_tick(uint64_t now_ns, std::vector<uint16_t>& removed, std::vector<uint16_t>& added);
    void publish_snapshot(bool sample);
    double signal_of(const DipState& s) const;

    Runtime& rt_;
    MembershipCallback cb_;
    mutable std::mutex mu_;
    std::vector<DipState> dips_;
    std::vector<uint8_t> eligible_;
    uint16_t known_{0};
    uint16_t prev_argmin_{kInvalidDip};
    uint64_t generation_{0};

    std::atomic<uint64_t> snapshots_{0};
    std::atomic<uint64_t> oscillations_{0};
    std::atomic<uint64_t> reports_{0};
    std::atomic<double> last_avg_{0.0};
    std::atomic<double> last_stddev_{0.0};
    std::atomic<double> last_mu_{0.0};

    std::atomic<bool> running_{false};
    std::atomic<bool> reevaluate_{false};
    std::thread thread_;
};

} // namespace balancify::lb
