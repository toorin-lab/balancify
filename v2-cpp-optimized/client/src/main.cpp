#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>

#include <chrono>
#include <iostream>
#include <random>
#include <thread>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace json = boost::json;

json::object build_request_payload(int id) {
    json::object payload;
    payload["request_id"] = "cpp-" + std::to_string(id);
    payload["protocol"] = "HTTP";
    payload["source_ip"] = "192.168.100." + std::to_string((id % 250) + 1);
    payload["source_port"] = 10000 + (id % 5000);
    return payload;
}

int main() {
    const std::string lb_url = std::getenv("LOAD_BALANCER_URL") ? std::getenv("LOAD_BALANCER_URL") : "http://load-balancer-balancify:8080";
    const auto colon = lb_url.find(':', lb_url.find("//") + 2);
    const std::string host = lb_url.substr(lb_url.find("//") + 2, colon - (lb_url.find("//") + 2));
    const std::string port = lb_url.substr(colon + 1);

    net::io_context ioc;
    net::ip::tcp::resolver resolver{ioc};

    int counter = 0;
    while (true) {
        try {
            beast::tcp_stream stream{ioc};
            auto const results = resolver.resolve(host, port);
            stream.connect(results);

            auto payload = build_request_payload(counter++);

            http::request<http::string_body> req{http::verb::post, "/route", 11};
            req.set(http::field::host, host);
            req.set(http::field::content_type, "application/json");
            req.body() = json::serialize(payload);
            req.prepare_payload();

            http::write(stream, req);
            beast::flat_buffer buffer;
            http::response<http::dynamic_body> res;
            http::read(stream, buffer, res);
            std::cout << beast::buffers_to_string(res.body().data()) << std::endl;
            stream.socket().shutdown(net::ip::tcp::socket::shutdown_both);
        } catch (const std::exception& ex) {
            std::cerr << "Client error: " << ex.what() << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

