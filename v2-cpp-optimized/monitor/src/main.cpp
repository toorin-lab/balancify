#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace json = boost::json;

double read_cpu_usage() {
    static uint64_t last_idle = 0, last_total = 0;
    std::ifstream stat("/proc/stat");
    std::string cpu;
    uint64_t user, nice, system, idle, iowait, irq, softirq, steal;
    stat >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
    uint64_t idle_time = idle + iowait;
    uint64_t total_time = user + nice + system + idle + iowait + irq + softirq + steal;

    uint64_t diff_idle = idle_time - last_idle;
    uint64_t diff_total = total_time - last_total;

    last_idle = idle_time;
    last_total = total_time;

    if (diff_total == 0) return 0.0;
    return (1.0 - static_cast<double>(diff_idle) / diff_total) * 100.0;
}

int main() {
    const std::string server_name = std::getenv("SERVER_NAME") ? std::getenv("SERVER_NAME") : "server-1";
    const std::string load_balancer_url = std::getenv("LOAD_BALANCER_URL") ? std::getenv("LOAD_BALANCER_URL") : "http://load-balancer:8080";
    const auto interval = std::chrono::seconds(std::stoi(std::getenv("MONITORING_INTERVAL") ? std::getenv("MONITORING_INTERVAL") : "10"));

    net::io_context ioc;
    while (true) {
        try {
            double cpu = read_cpu_usage();
            double ram = 2.0; // placeholder

            json::object payload;
            payload["server_name"] = server_name;
            payload["cpu_utilization"] = cpu;
            payload["ram_utilization"] = ram;
            payload["health_status"] = true;

            auto const host = load_balancer_url.substr(load_balancer_url.find("//") + 2);
            const auto colon = host.find(':');
            const auto path = "/update-server-metrics";
            const std::string hostname = host.substr(0, colon);
            const std::string port = host.substr(colon + 1);

            net::ip::tcp::resolver resolver{ioc};
            beast::tcp_stream stream{ioc};
            auto const results = resolver.resolve(hostname, port);
            stream.connect(results);

            http::request<http::string_body> req{http::verb::post, path, 11};
            req.set(http::field::host, hostname);
            req.set(http::field::content_type, "application/json");
            req.body() = json::serialize(payload);
            req.prepare_payload();

            http::write(stream, req);
            beast::flat_buffer buffer;
            http::response<http::dynamic_body> res;
            http::read(stream, buffer, res);
            stream.socket().shutdown(net::ip::tcp::socket::shutdown_both);
        } catch (const std::exception& ex) {
            std::cerr << "Monitor error: " << ex.what() << std::endl;
        }

        std::this_thread::sleep_for(interval);
    }
}

