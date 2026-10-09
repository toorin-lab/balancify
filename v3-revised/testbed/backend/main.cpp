// Backend service for the testbed. Each request burns the CPU time it asks
// for (GET /work?cpu_us=N&bytes=M), so the client controls the per-connection
// cost distribution and hence the request heterogeneity c. Every request is
// written to an access log whose last field is the request time in seconds,
// which the agent turns into the latency load signal.

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace {

std::mutex g_log_mu;
std::ofstream g_log;

std::map<std::string, std::string> query_of(const std::string& target) {
    std::map<std::string, std::string> q;
    const auto pos = target.find('?');
    if (pos == std::string::npos) return q;
    std::string rest = target.substr(pos + 1);
    size_t start = 0;
    while (start < rest.size()) {
        size_t amp = rest.find('&', start);
        if (amp == std::string::npos) amp = rest.size();
        const std::string kv = rest.substr(start, amp - start);
        const auto eq = kv.find('=');
        if (eq != std::string::npos) q[kv.substr(0, eq)] = kv.substr(eq + 1);
        start = amp + 1;
    }
    return q;
}

double thread_cpu_us() {
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

void burn(double cpu_us) {
    const double start = thread_cpu_us();
    volatile uint64_t x = 0;
    while (thread_cpu_us() - start < cpu_us) {
        for (int i = 0; i < 1000; ++i) x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    }
}

void serve(tcp::socket socket) {
    beast::error_code ec;
    beast::flat_buffer buffer;
    const std::string peer = socket.remote_endpoint(ec).address().to_string();
    while (true) {
        http::request<http::string_body> req;
        http::read(socket, buffer, req, ec);
        if (ec) break;
        const auto t0 = std::chrono::steady_clock::now();
        const std::string target(req.target());
        http::response<http::string_body> res{http::status::ok, req.version()};
        res.set(http::field::content_type, "application/octet-stream");
        res.keep_alive(req.keep_alive());
        if (target.rfind("/work", 0) == 0) {
            const auto q = query_of(target);
            const double cpu_us = q.count("cpu_us") ? std::stod(q.at("cpu_us")) : 0.0;
            const size_t bytes = q.count("bytes") ? std::stoul(q.at("bytes")) : 64;
            burn(cpu_us);
            res.body().assign(bytes, 'x');
        } else if (target == "/health") {
            res.body() = "ok";
        } else {
            res.result(http::status::not_found);
        }
        res.prepare_payload();
        http::write(socket, res, ec);
        const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        {
            std::lock_guard lk(g_log_mu);
            if (g_log.is_open()) {
                char line[512];
                std::snprintf(line, sizeof(line), "%s %s %s %u %zu %.6f\n", peer.c_str(),
                              std::string(req.method_string()).c_str(), target.c_str(), res.result_int(),
                              res.body().size(), secs);
                g_log << line;
                g_log.flush();
            }
        }
        if (ec || !res.keep_alive()) break;
    }
    socket.shutdown(tcp::socket::shutdown_both, ec);
}

} // namespace

int main(int argc, char** argv) {
    unsigned short port = 80;
    std::string log_path;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) port = static_cast<unsigned short>(std::stoi(argv[++i]));
        else if (a == "--access-log" && i + 1 < argc) log_path = argv[++i];
        else {
            std::cerr << "usage: " << argv[0] << " [--port 80] [--access-log FILE]" << std::endl;
            return EXIT_FAILURE;
        }
    }
    if (!log_path.empty()) g_log.open(log_path, std::ios::app);

    try {
        net::io_context ioc{1};
        tcp::acceptor acceptor(ioc, {tcp::v4(), port});
        acceptor.set_option(net::socket_base::reuse_address(true));
        std::cout << "backend listening on port " << port << std::endl;
        while (true) {
            tcp::socket socket(ioc);
            acceptor.accept(socket);
            std::thread(serve, std::move(socket)).detach();
        }
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
}
