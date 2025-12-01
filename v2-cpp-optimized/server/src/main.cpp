#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>

#include <chrono>
#include <iostream>
#include <random>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace json = boost::json;

struct ServerState {
    std::string name{"server-1"};
    double cpu_utilization{0.0};
    double ram_utilization{0.0};
};

class HttpServer {
public:
    HttpServer(net::ip::tcp::endpoint endpoint, ServerState state)
        : acceptor_(ioc_), state_(std::move(state)) {
        beast::error_code ec;
        acceptor_.open(endpoint.protocol(), ec);
        acceptor_.set_option(net::socket_base::reuse_address(true));
        acceptor_.bind(endpoint, ec);
        acceptor_.listen(net::socket_base::max_listen_connections, ec);
    }

    void run() {
        while (true) {
            net::ip::tcp::socket socket{ioc_};
            acceptor_.accept(socket);
            beast::flat_buffer buffer;
            http::request<http::string_body> req;
            http::read(socket, buffer, req);
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "application/json");

            if (req.method() == http::verb::post && req.target() == "/process") {
                res.body() = R"({"status":"ok","message":"request processed"})";
            } else if (req.method() == http::verb::get && req.target() == "/metrics") {
                update_metrics();
                json::object obj;
                obj["server_name"] = state_.name;
                obj["cpu_percent"] = state_.cpu_utilization;
                obj["memory_used"] = state_.ram_utilization;
                res.body() = json::serialize(obj);
            } else {
                res.result(http::status::not_found);
                res.body() = R"({"error":"unknown endpoint"})";
            }

            res.prepare_payload();
            http::write(socket, res);
        }
    }

private:
    void update_metrics() {
        static std::mt19937 rng(std::random_device{}());
        std::uniform_real_distribution<double> cpu_dist(10.0, 80.0);
        std::uniform_real_distribution<double> ram_dist(1.0, 8.0);
        state_.cpu_utilization = cpu_dist(rng);
        state_.ram_utilization = ram_dist(rng);
    }

    net::io_context ioc_{1};
    net::ip::tcp::acceptor acceptor_;
    ServerState state_;
};

int main() {
    const auto name = std::getenv("SERVER_NAME") ? std::getenv("SERVER_NAME") : "server-1";
    const auto port = static_cast<unsigned short>(std::stoi(std::getenv("SERVER_PORT") ? std::getenv("SERVER_PORT") : "5000"));

    ServerState state;
    state.name = name;

    HttpServer server({net::ip::tcp::v4(), port}, state);
    std::cout << "C++ Balancify server " << name << " listening on port " << port << std::endl;
    server.run();
}

