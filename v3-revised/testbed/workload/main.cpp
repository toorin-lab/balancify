// Closed-loop workload generator for end-to-end latency. Each of C client
// threads repeatedly opens a connection to the VIP, draws a per-connection
// cost from a log-normal distribution with the requested mean and coefficient
// of variation c, issues requests of that cost for an exponentially
// distributed lifetime, and closes the connection. The per-request latency
// percentiles are printed at the end (and optionally written as CSV).

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;
using Clock = std::chrono::steady_clock;

namespace {

struct Options {
    std::string host{"10.0.0.100"};
    std::string port{"80"};
    unsigned connections{64};
    double duration_s{60.0};
    double mean_cpu_us{2000.0};
    double cv{1.0};
    double lifetime_s{5.0};
    double think_ms{0.0};
    size_t bytes{1024};
    std::string csv;
};

Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
            return argv[++i];
        };
        if (a == "--target") {
            const std::string t = next();
            const auto colon = t.rfind(':');
            o.host = t.substr(0, colon);
            if (colon != std::string::npos) o.port = t.substr(colon + 1);
        } else if (a == "--connections") o.connections = static_cast<unsigned>(std::stoul(next()));
        else if (a == "--duration-s") o.duration_s = std::stod(next());
        else if (a == "--mean-cpu-us") o.mean_cpu_us = std::stod(next());
        else if (a == "--cv") o.cv = std::stod(next());
        else if (a == "--lifetime-s") o.lifetime_s = std::stod(next());
        else if (a == "--think-ms") o.think_ms = std::stod(next());
        else if (a == "--bytes") o.bytes = std::stoul(next());
        else if (a == "--csv") o.csv = next();
        else throw std::runtime_error("unknown argument " + a);
    }
    return o;
}

double percentile(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    const size_t idx = std::min(sorted.size() - 1, static_cast<size_t>(p * static_cast<double>(sorted.size())));
    return sorted[idx];
}

} // namespace

int main(int argc, char** argv) {
    Options o;
    try {
        o = parse(argc, argv);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\nusage: " << argv[0]
                  << " --target HOST:PORT [--connections 64] [--duration-s 60] [--mean-cpu-us 2000]"
                     " [--cv 1.0] [--lifetime-s 5] [--think-ms 0] [--bytes 1024] [--csv FILE]" << std::endl;
        return EXIT_FAILURE;
    }

    // Log-normal with mean m and coefficient of variation c.
    const double sigma2 = std::log(1.0 + o.cv * o.cv);
    const double mu = std::log(o.mean_cpu_us) - sigma2 / 2.0;
    const double sigma = std::sqrt(sigma2);

    std::mutex mu_all;
    std::vector<double> all;
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> conns{0};
    const auto deadline = Clock::now() + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(o.duration_s));

    std::vector<std::thread> threads;
    for (unsigned c = 0; c < o.connections; ++c) {
        threads.emplace_back([&, c] {
            std::mt19937_64 rng(std::random_device{}() ^ (uint64_t(c) << 32));
            std::lognormal_distribution<double> cost(mu, sigma);
            std::exponential_distribution<double> life(1.0 / o.lifetime_s);
            std::vector<double> lat;
            net::io_context ioc;
            tcp::resolver resolver(ioc);
            while (Clock::now() < deadline) {
                try {
                    beast::tcp_stream stream(ioc);
                    stream.connect(resolver.resolve(o.host, o.port));
                    conns.fetch_add(1);
                    const double cpu_us = cost(rng);
                    const auto end = std::min(deadline, Clock::now() + std::chrono::duration_cast<Clock::duration>(
                                                                         std::chrono::duration<double>(life(rng))));
                    const std::string target = "/work?cpu_us=" + std::to_string(static_cast<uint64_t>(cpu_us)) +
                                               "&bytes=" + std::to_string(o.bytes);
                    beast::flat_buffer buffer;
                    do {
                        http::request<http::empty_body> req{http::verb::get, target, 11};
                        req.set(http::field::host, o.host);
                        req.keep_alive(true);
                        const auto t0 = Clock::now();
                        http::write(stream, req);
                        http::response<http::string_body> res;
                        http::read(stream, buffer, res);
                        lat.push_back(std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
                        if (o.think_ms > 0) {
                            std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(o.think_ms));
                        }
                    } while (Clock::now() < end);
                    beast::error_code ec;
                    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                } catch (const std::exception&) {
                    errors.fetch_add(1);
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
            std::lock_guard lk(mu_all);
            all.insert(all.end(), lat.begin(), lat.end());
        });
    }
    for (auto& t : threads) t.join();

    std::sort(all.begin(), all.end());
    std::cout << "requests " << all.size() << " connections " << conns.load() << " errors " << errors.load()
              << " throughput_rps " << all.size() / o.duration_s << "\n"
              << "latency_ms p50 " << percentile(all, 0.50) << " p90 " << percentile(all, 0.90) << " p99 "
              << percentile(all, 0.99) << " p99.9 " << percentile(all, 0.999) << std::endl;
    if (!o.csv.empty()) {
        std::ofstream out(o.csv);
        out << "latency_ms\n";
        for (double v : all) out << v << "\n";
    }
    return EXIT_SUCCESS;
}
