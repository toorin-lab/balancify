// Boost before DPDK: DPDK defines function-like macros (likely/unlikely)
// that must not be visible while Boost headers are parsed.
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "control_api.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <iostream>

namespace balancify::lb {

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace {

std::string url_decode(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '+') {
            out.push_back(' ');
        } else if (s[i] == '%' && i + 2 < s.size()) {
            out.push_back(static_cast<char>(std::stoi(s.substr(i + 1, 2), nullptr, 16)));
            i += 2;
        } else {
            out.push_back(s[i]);
        }
    }
    return out;
}

std::map<std::string, std::string> parse_query(const std::string& q) {
    std::map<std::string, std::string> out;
    size_t pos = 0;
    while (pos < q.size()) {
        size_t amp = q.find('&', pos);
        if (amp == std::string::npos) amp = q.size();
        const std::string kv = q.substr(pos, amp - pos);
        const size_t eq = kv.find('=');
        if (eq != std::string::npos) {
            out[url_decode(kv.substr(0, eq))] = url_decode(kv.substr(eq + 1));
        } else if (!kv.empty()) {
            out[url_decode(kv)] = "";
        }
        pos = amp + 1;
    }
    return out;
}

std::string error(const std::string& msg) {
    Json j;
    j.begin_object().field("error", msg).end_object();
    return j.str();
}

std::string ok() {
    Json j;
    j.begin_object().field("status", "ok").end_object();
    return j.str();
}

} // namespace

ControlApi::ControlApi(StatsSources src) : src_(src) {}

ControlApi::~ControlApi() {
    stop();
}

void ControlApi::start() {
    if (running_.exchange(true)) return;
    thread_ = std::thread(&ControlApi::run, this);
}

void ControlApi::stop() {
    if (!running_.exchange(false)) return;
    // Wake the blocking accept with a local connection.
    try {
        net::io_context ioc;
        tcp::socket s(ioc);
        s.connect({net::ip::make_address(src_.rt.cfg.control_bind == "0.0.0.0" ? "127.0.0.1" : src_.rt.cfg.control_bind),
                   src_.rt.cfg.control_port});
    } catch (const std::exception&) {
    }
    if (thread_.joinable()) thread_.join();
}

std::string ControlApi::handle(bool post, const std::string& target, int& status) {
    Runtime& rt = src_.rt;
    const size_t qpos = target.find('?');
    const std::string path = target.substr(0, qpos);
    const auto q = parse_query(qpos == std::string::npos ? "" : target.substr(qpos + 1));
    status = 200;

    if (!post && path == "/stats") return render_stats(src_);
    if (!post && path == "/dips") return render_dips(src_);
    if (!post && path == "/ready") {
        status = rt.ready.load() ? 200 : 503;
        Json j;
        j.begin_object().field("ready", rt.ready.load()).end_object();
        return j.str();
    }
    if (post && path == "/params") {
        if (const auto it = q.find("threshold"); it != q.end()) {
            const double t = std::stod(it->second);
            if (t < 0.0) { status = 400; return error("threshold must be non-negative"); }
            rt.threshold.store(t);
        }
        if (const auto it = q.find("interval"); it != q.end()) {
            const double s = std::stod(it->second);
            if (s <= 0.0) { status = 400; return error("interval must be positive"); }
            rt.sampling_interval_ms.store(static_cast<uint32_t>(std::lround(s * 1000.0)));
        }
        if (const auto it = q.find("rule"); it != q.end()) {
            TargetRule r;
            if (!parse_target_rule(it->second, r)) { status = 400; return error("rule must be r0, r1 or r2"); }
            rt.target_rule.store(static_cast<uint8_t>(r));
        }
        return ok();
    }
    if (post && path == "/dips/add") {
        uint32_t ip = 0;
        if (!q.count("ip") || !parse_ipv4(q.at("ip"), ip)) { status = 400; return error("ip is required"); }
        rte_ether_addr mac{};
        bool has_mac = false;
        if (q.count("mac")) {
            std::array<uint8_t, 6> m{};
            if (!parse_mac(q.at("mac"), m)) { status = 400; return error("invalid mac"); }
            std::copy(m.begin(), m.end(), mac.addr_bytes);
            has_mac = true;
        }
        const uint32_t weight = q.count("weight") ? static_cast<uint32_t>(std::stoul(q.at("weight"))) : 1;
        const uint16_t idx = rt.pool->add(q.count("name") ? q.at("name") : "", ip, has_mac ? &mac : nullptr, weight);
        (*rt.pool)[idx].admin_enabled.store(true);
        src_.monitor.request_reevaluation();
        Json j;
        j.begin_object().field("status", "ok").field("index", static_cast<unsigned>(idx)).end_object();
        return j.str();
    }
    if (post && (path == "/dips/drain" || path == "/dips/enable")) {
        const auto it = q.find("name");
        const uint16_t idx = it == q.end() ? kInvalidDip : rt.pool->find_name(it->second);
        if (idx == kInvalidDip) { status = 404; return error("unknown dip"); }
        (*rt.pool)[idx].admin_enabled.store(path == "/dips/enable");
        src_.monitor.request_reevaluation();
        return ok();
    }
    status = 404;
    return error("unknown endpoint");
}

void ControlApi::run() {
    try {
        net::io_context ioc{1};
        tcp::acceptor acceptor(ioc, {net::ip::make_address(src_.rt.cfg.control_bind), src_.rt.cfg.control_port});
        while (running_.load()) {
            tcp::socket socket(ioc);
            beast::error_code ec;
            acceptor.accept(socket, ec);
            if (ec || !running_.load()) continue;
            try {
                beast::flat_buffer buffer;
                http::request<http::string_body> req;
                http::read(socket, buffer, req);
                int status = 200;
                std::string body;
                const bool post = req.method() == http::verb::post || req.method() == http::verb::put;
                try {
                    body = handle(post, std::string(req.target()), status);
                } catch (const std::exception& ex) {
                    status = 400;
                    body = error(ex.what());
                }
                http::response<http::string_body> res{static_cast<http::status>(status), req.version()};
                res.set(http::field::content_type, "application/json");
                res.keep_alive(false);
                res.body() = std::move(body);
                res.prepare_payload();
                http::write(socket, res);
                socket.shutdown(tcp::socket::shutdown_both, ec);
            } catch (const std::exception&) {
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "[control] " << ex.what() << std::endl;
    }
}

} // namespace balancify::lb
