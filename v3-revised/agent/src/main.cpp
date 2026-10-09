// Lightweight agent installed on every DIP. Every interval it sends one load
// report to each load-balancer instance: CPU utilization averaged over a
// sliding window, the mean request-response time taken from the service's
// access log, the number of established connections on the service port,
// and a local health verdict.

#include "balancify/agent_report.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

std::atomic<bool> g_stop{false};

struct Options {
    uint32_t dip_ip_be{0};
    std::vector<sockaddr_in> lbs;
    unsigned interval_ms{200};
    unsigned window_ms{1000};
    unsigned service_port{0};
    unsigned check_port{0};
    std::string access_log;
    int latency_field{-1};          // whitespace-separated field; negative counts from the end
    double latency_to_ms{1000.0};   // multiplier from the log unit to milliseconds
};

void usage(const char* prog) {
    std::cerr << "usage: " << prog
              << " --dip-ip A.B.C.D --lb HOST:PORT [--lb HOST:PORT]... [--interval-ms 200]\n"
                 "       [--window-ms 1000] [--service-port P] [--check-port P]\n"
                 "       [--access-log FILE] [--latency-field N] [--latency-unit s|ms|us]\n";
}

bool parse_endpoint(const std::string& s, sockaddr_in& out) {
    const auto colon = s.rfind(':');
    if (colon == std::string::npos) return false;
    out = sockaddr_in{};
    out.sin_family = AF_INET;
    out.sin_port = htons(static_cast<uint16_t>(std::stoi(s.substr(colon + 1))));
    return inet_pton(AF_INET, s.substr(0, colon).c_str(), &out.sin_addr) == 1;
}

Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
            return argv[++i];
        };
        if (a == "--dip-ip") {
            if (inet_pton(AF_INET, next().c_str(), &o.dip_ip_be) != 1) throw std::runtime_error("bad --dip-ip");
        } else if (a == "--lb") {
            sockaddr_in sa{};
            if (!parse_endpoint(next(), sa)) throw std::runtime_error("bad --lb");
            o.lbs.push_back(sa);
        } else if (a == "--interval-ms") {
            o.interval_ms = static_cast<unsigned>(std::stoul(next()));
        } else if (a == "--window-ms") {
            o.window_ms = static_cast<unsigned>(std::stoul(next()));
        } else if (a == "--service-port") {
            o.service_port = static_cast<unsigned>(std::stoul(next()));
        } else if (a == "--check-port") {
            o.check_port = static_cast<unsigned>(std::stoul(next()));
        } else if (a == "--access-log") {
            o.access_log = next();
        } else if (a == "--latency-field") {
            o.latency_field = std::stoi(next());
        } else if (a == "--latency-unit") {
            const std::string u = next();
            if (u == "s") o.latency_to_ms = 1000.0;
            else if (u == "ms") o.latency_to_ms = 1.0;
            else if (u == "us") o.latency_to_ms = 0.001;
            else throw std::runtime_error("bad --latency-unit");
        } else {
            throw std::runtime_error("unknown argument " + a);
        }
    }
    if (o.dip_ip_be == 0 || o.lbs.empty()) throw std::runtime_error("--dip-ip and --lb are required");
    if (o.interval_ms == 0) throw std::runtime_error("--interval-ms must be positive");
    return o;
}

// --- CPU ---------------------------------------------------------------------

struct CpuSample {
    Clock::time_point t;
    uint64_t total{0};
    uint64_t idle{0};
};

bool read_cpu(CpuSample& s) {
    std::ifstream in("/proc/stat");
    std::string cpu;
    uint64_t v[10] = {};
    if (!(in >> cpu) || cpu != "cpu") return false;
    for (auto& x : v) in >> x;
    // user nice system idle iowait irq softirq steal guest guest_nice
    s.idle = v[3] + v[4];
    s.total = v[0] + v[1] + v[2] + v[3] + v[4] + v[5] + v[6] + v[7];
    s.t = Clock::now();
    return true;
}

class CpuWindow {
public:
    explicit CpuWindow(unsigned window_ms) : window_(std::chrono::milliseconds(window_ms)) {}

    double sample() {
        CpuSample s;
        if (!read_cpu(s)) return 0.0;
        samples_.push_back(s);
        while (samples_.size() > 2 && s.t - samples_[1].t >= window_) samples_.pop_front();
        const CpuSample& first = samples_.front();
        const uint64_t dt = s.total - first.total;
        if (dt == 0) return last_;
        last_ = 100.0 * (1.0 - static_cast<double>(s.idle - first.idle) / static_cast<double>(dt));
        return last_;
    }

private:
    Clock::duration window_;
    std::deque<CpuSample> samples_;
    double last_{0.0};
};

// --- connections ---------------------------------------------------------------

uint32_t established_on_port(unsigned port) {
    if (port == 0) return 0;
    uint32_t n = 0;
    for (const char* path : {"/proc/net/tcp", "/proc/net/tcp6"}) {
        std::ifstream in(path);
        std::string line;
        std::getline(in, line);  // header
        while (std::getline(in, line)) {
            std::istringstream ls(line);
            std::string sl, local, remote, st;
            if (!(ls >> sl >> local >> remote >> st)) continue;
            const auto colon = local.rfind(':');
            if (colon == std::string::npos) continue;
            if (std::stoul(local.substr(colon + 1), nullptr, 16) == port && st == "01") ++n;
        }
    }
    return n;
}

bool port_accepts(unsigned port) {
    if (port == 0) return true;
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) return false;
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(port));
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bool ok = false;
    if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0) {
        ok = true;
    } else if (errno == EINPROGRESS) {
        pollfd p{fd, POLLOUT, 0};
        if (::poll(&p, 1, 200) == 1) {
            int err = 0;
            socklen_t len = sizeof(err);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
            ok = err == 0;
        }
    }
    ::close(fd);
    return ok;
}

// --- access log ----------------------------------------------------------------

class AccessLogTail {
public:
    AccessLogTail(std::string path, int field, double to_ms, unsigned window_ms)
        : path_(std::move(path)), field_(field), to_ms_(to_ms), window_(std::chrono::milliseconds(window_ms)) {
        open(true);
    }

    // Reads the lines appended since the last call and returns the mean
    // latency (ms) and the number of requests over the window.
    std::pair<double, uint32_t> poll() {
        if (path_.empty()) return {0.0, 0};
        reopen_if_rotated();
        double sum = 0.0;
        uint32_t count = 0;
        if (fd_ >= 0) {
            char buf[65536];
            ssize_t n;
            while ((n = ::read(fd_, buf, sizeof(buf))) > 0) {
                partial_.append(buf, static_cast<size_t>(n));
                pos_ += n;
            }
            size_t start = 0;
            size_t nl;
            while ((nl = partial_.find('\n', start)) != std::string::npos) {
                double v;
                if (parse_line(partial_.substr(start, nl - start), v)) {
                    sum += v;
                    ++count;
                }
                start = nl + 1;
            }
            partial_.erase(0, start);
        }
        const auto now = Clock::now();
        ticks_.push_back({now, sum, count});
        while (!ticks_.empty() && now - ticks_.front().t > window_) ticks_.pop_front();
        double wsum = 0.0;
        uint32_t wcount = 0;
        for (const auto& t : ticks_) {
            wsum += t.sum;
            wcount += t.count;
        }
        if (wcount != 0) last_ = wsum / wcount;
        return {last_, wcount};
    }

private:
    struct Tick {
        Clock::time_point t;
        double sum;
        uint32_t count;
    };

    void open(bool at_end) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = ::open(path_.c_str(), O_RDONLY | O_NONBLOCK);
        if (fd_ < 0) return;
        struct stat st {};
        ::fstat(fd_, &st);
        inode_ = st.st_ino;
        pos_ = at_end ? ::lseek(fd_, 0, SEEK_END) : 0;
        partial_.clear();
    }

    void reopen_if_rotated() {
        struct stat st {};
        if (::stat(path_.c_str(), &st) != 0) return;
        if (fd_ < 0 || st.st_ino != inode_ || st.st_size < pos_) open(false);
    }

    bool parse_line(const std::string& line, double& ms) const {
        std::vector<std::string> f;
        std::istringstream in(line);
        std::string tok;
        while (in >> tok) f.push_back(tok);
        if (f.empty()) return false;
        const long idx = field_ >= 0 ? field_ : static_cast<long>(f.size()) + field_;
        if (idx < 0 || idx >= static_cast<long>(f.size())) return false;
        char* end = nullptr;
        const double v = std::strtod(f[static_cast<size_t>(idx)].c_str(), &end);
        if (end == f[static_cast<size_t>(idx)].c_str()) return false;
        ms = v * to_ms_;
        return true;
    }

    std::string path_;
    int field_;
    double to_ms_;
    Clock::duration window_;
    int fd_{-1};
    ino_t inode_{0};
    off_t pos_{0};
    std::string partial_;
    std::deque<Tick> ticks_;
    double last_{0.0};
};

} // namespace

int main(int argc, char** argv) {
    Options opt;
    try {
        opt = parse(argc, argv);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    std::signal(SIGINT, [](int) { g_stop = true; });
    std::signal(SIGTERM, [](int) { g_stop = true; });

    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        std::perror("socket");
        return EXIT_FAILURE;
    }

    CpuWindow cpu(opt.window_ms);
    AccessLogTail log(opt.access_log, opt.latency_field, opt.latency_to_ms, opt.window_ms);
    balancify::AgentReport r;
    r.dip_ip_be = opt.dip_ip_be;
    uint8_t buf[balancify::kReportWireSize];
    auto next = Clock::now();

    while (!g_stop.load()) {
        r.cpu_pct = static_cast<float>(cpu.sample());
        const auto [latency, requests] = log.poll();
        r.latency_ms = static_cast<float>(latency);
        r.requests = requests;
        r.active_conns = established_on_port(opt.service_port);
        r.healthy = port_accepts(opt.check_port);
        r.timestamp_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                   std::chrono::system_clock::now().time_since_epoch())
                                                   .count());
        ++r.seq;
        const size_t len = balancify::encode_report(r, buf);
        for (const auto& lb : opt.lbs) {
            ::sendto(fd, buf, len, 0, reinterpret_cast<const sockaddr*>(&lb), sizeof(lb));
        }
        next += std::chrono::milliseconds(opt.interval_ms);
        std::this_thread::sleep_until(next);
    }
    ::close(fd);
    return EXIT_SUCCESS;
}
