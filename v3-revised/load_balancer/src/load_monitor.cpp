#include "load_monitor.hpp"

#include "balancify/agent_report.hpp"

#include <rte_common.h>
#include <rte_malloc.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <stdexcept>

namespace balancify::lb {

namespace {

uint64_t now_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

constexpr double kDefaultMu = 1e-3;
constexpr uint64_t kHealthTickNs = 50'000'000;  // 50 ms

} // namespace

LoadMonitor::LoadMonitor(Runtime& rt, MembershipCallback cb)
    : rt_(rt), cb_(std::move(cb)), dips_(kMaxDips), eligible_(kMaxDips, 0) {}

LoadMonitor::~LoadMonitor() {
    stop();
}

double LoadMonitor::signal_of(const DipState& s) const {
    return rt_.cfg.load_signal == LoadSignal::Cpu ? s.cpu : s.latency_ms;
}

void LoadMonitor::sync_pool_size(uint64_t now) {
    const uint16_t n = rt_.pool->size();
    for (uint16_t i = known_; i < n; ++i) {
        // A DIP that has never reported gets one health timeout of grace.
        dips_[i] = DipState{};
        dips_[i].last_report_ns = now;
    }
    known_ = n;
}

void LoadMonitor::prime() {
    std::lock_guard lk(mu_);
    sync_pool_size(now_ns());
    for (uint16_t i = 0; i < known_; ++i) {
        const bool e = (*rt_.pool)[i].admin_enabled.load(std::memory_order_relaxed);
        eligible_[i] = e ? 1 : 0;
        (*rt_.pool)[i].eligible.store(e, std::memory_order_relaxed);
    }
    publish_snapshot(false);
}

std::vector<uint16_t> LoadMonitor::eligible() const {
    std::lock_guard lk(mu_);
    std::vector<uint16_t> out;
    for (uint16_t i = 0; i < known_; ++i) {
        if (eligible_[i]) out.push_back(i);
    }
    return out;
}

std::vector<LoadMonitor::DipView> LoadMonitor::view() const {
    std::lock_guard lk(mu_);
    const uint64_t now = now_ns();
    std::vector<DipView> out;
    out.reserve(known_);
    for (uint16_t i = 0; i < known_; ++i) {
        const DipState& s = dips_[i];
        DipView v;
        v.index = i;
        v.healthy = s.healthy;
        v.eligible = eligible_[i] != 0;
        v.admin_enabled = (*rt_.pool)[i].admin_enabled.load(std::memory_order_relaxed);
        v.sampled_load = s.sampled;
        v.cpu = s.cpu;
        v.latency_ms = s.latency_ms;
        v.active_conns = s.active_conns;
        v.reports = s.reports;
        v.report_age_ms = s.reports != 0 ? static_cast<double>(now - s.last_report_ns) / 1e6 : -1.0;
        out.push_back(v);
    }
    return out;
}

void LoadMonitor::start() {
    if (running_.exchange(true)) return;
    thread_ = std::thread(&LoadMonitor::run, this);
}

void LoadMonitor::stop() {
    running_.store(false);
    if (thread_.joinable()) thread_.join();
}

void LoadMonitor::receive(int fd, uint64_t now) {
    uint8_t buf[256];
    while (true) {
        const ssize_t len = ::recv(fd, buf, sizeof(buf), 0);
        if (len <= 0) {
            return;
        }
        AgentReport r;
        if (!decode_report(buf, static_cast<size_t>(len), r)) {
            continue;
        }
        const uint16_t idx = rt_.pool->find_ip(r.dip_ip_be);
        if (idx == kInvalidDip) {
            continue;
        }
        std::lock_guard lk(mu_);
        sync_pool_size(now);
        DipState& s = dips_[idx];
        s.last_report_ns = now;
        s.reported_healthy = r.healthy;
        s.cpu = r.cpu_pct;
        s.latency_ms = r.latency_ms;
        s.active_conns = r.active_conns;
        s.sum += signal_of(s);
        ++s.n;
        ++s.reports;
        reports_.fetch_add(1, std::memory_order_relaxed);
    }
}

bool LoadMonitor::health_tick(uint64_t now, std::vector<uint16_t>& removed, std::vector<uint16_t>& added) {
    const uint64_t timeout = static_cast<uint64_t>(rt_.cfg.health_timeout_ms) * 1'000'000ULL;
    bool changed = false;
    for (uint16_t i = 0; i < known_; ++i) {
        DipState& s = dips_[i];
        if (!rt_.cfg.health_requires_agent && s.reports == 0) {
            s.healthy = true;
        } else {
            s.healthy = s.reported_healthy && (now - s.last_report_ns) <= timeout;
        }
        const bool e = s.healthy && (*rt_.pool)[i].admin_enabled.load(std::memory_order_relaxed);
        if (e != (eligible_[i] != 0)) {
            eligible_[i] = e ? 1 : 0;
            (*rt_.pool)[i].eligible.store(e, std::memory_order_relaxed);
            (e ? added : removed).push_back(i);
            changed = true;
        }
    }
    return changed;
}

void LoadMonitor::publish_snapshot(bool sample) {
    void* mem = rte_zmalloc("bfy_load", sizeof(LoadSnapshot), RTE_CACHE_LINE_SIZE);
    if (mem == nullptr) {
        std::cerr << "[monitor] cannot allocate a load snapshot" << std::endl;
        return;
    }
    auto* s = new (mem) LoadSnapshot();
    s->generation = ++generation_;
    s->ndips = known_;

    double sum = 0.0;
    double sumsq = 0.0;
    double conns = 0.0;
    uint16_t n = 0;
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::lowest();
    for (uint16_t i = 0; i < known_; ++i) {
        DipState& d = dips_[i];
        if (sample) {
            d.sampled = (rt_.cfg.load_mean_over_interval && d.n != 0) ? d.sum / d.n : signal_of(d);
            d.sum = 0.0;
            d.n = 0;
        }
        s->load[i] = static_cast<float>(d.sampled);
        s->eligible[i] = eligible_[i];
        if (!eligible_[i]) continue;
        sum += d.sampled;
        sumsq += d.sampled * d.sampled;
        conns += d.active_conns;
        ++n;
        if (d.sampled < min) {
            min = d.sampled;
            s->argmin = i;
        }
        if (d.sampled > max) {
            max = d.sampled;
            s->argmax = i;
        }
    }
    s->n_eligible = n;
    s->avg = n != 0 ? sum / n : 0.0;
    s->stddev = n != 0 ? std::sqrt(std::max(0.0, sumsq / n - s->avg * s->avg)) : 0.0;
    for (uint16_t i = 0; i < known_; ++i) {
        if (eligible_[i] && s->load[i] < s->avg) {
            s->below[s->n_below++] = i;
        }
    }
    if (rt_.cfg.r1_increment > 0.0) {
        s->mu = rt_.cfg.r1_increment;
    } else {
        s->mu = conns > 0.0 ? sum / conns : kDefaultMu;
    }

    if (sample) {
        // Oscillation: the DIP that was least loaded at the previous sample is now the most loaded.
        if (prev_argmin_ != kInvalidDip && prev_argmin_ == s->argmax && n > 1) {
            oscillations_.fetch_add(1, std::memory_order_relaxed);
        }
        prev_argmin_ = s->argmin;
        snapshots_.fetch_add(1, std::memory_order_relaxed);
    }
    last_avg_.store(s->avg, std::memory_order_relaxed);
    last_stddev_.store(s->stddev, std::memory_order_relaxed);
    last_mu_.store(s->mu, std::memory_order_relaxed);

    rcu_replace(rt_, rt_.load, s);
    rt_.pending->reset();
}

void LoadMonitor::run() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        std::cerr << "[monitor] socket: " << std::strerror(errno) << std::endl;
        return;
    }
    const int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(rt_.cfg.agent_port);
    if (inet_pton(AF_INET, rt_.cfg.agent_bind.c_str(), &addr.sin_addr) != 1 ||
        ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "[monitor] cannot bind " << rt_.cfg.agent_bind << ":" << rt_.cfg.agent_port << std::endl;
        ::close(fd);
        return;
    }
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    uint64_t next_sample = now_ns() + uint64_t(rt_.sampling_interval_ms.load()) * 1'000'000ULL;
    uint64_t next_health = now_ns();
    std::vector<uint16_t> removed;
    std::vector<uint16_t> added;

    while (running_.load(std::memory_order_relaxed)) {
        pollfd p{fd, POLLIN, 0};
        ::poll(&p, 1, 20);
        uint64_t now = now_ns();
        if (p.revents & POLLIN) {
            receive(fd, now);
        }

        now = now_ns();
        if (now >= next_health || reevaluate_.exchange(false)) {
            next_health = now + kHealthTickNs;
            removed.clear();
            added.clear();
            std::vector<uint16_t> members;
            bool changed;
            {
                std::lock_guard lk(mu_);
                sync_pool_size(now);
                changed = health_tick(now, removed, added);
                if (changed) {
                    // Overrides must stop targeting a departed DIP immediately, not at the next sample.
                    publish_snapshot(false);
                    for (uint16_t i = 0; i < known_; ++i) {
                        if (eligible_[i]) members.push_back(i);
                    }
                }
            }
            if (changed && cb_) {
                cb_(members, removed, added);
            }
        }

        if (now >= next_sample) {
            std::lock_guard lk(mu_);
            publish_snapshot(true);
            next_sample = now + uint64_t(rt_.sampling_interval_ms.load()) * 1'000'000ULL;
        }
    }
    ::close(fd);
}

} // namespace balancify::lb
