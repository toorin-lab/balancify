#include "control_plane.hpp"

#include "common/types.hpp"

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>

#include <iostream>

namespace balancify::lb {

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace json = boost::json;

ControlPlaneServer::ControlPlaneServer(unsigned short port, BalancifyEngine& engine)
    : port_(port), engine_(engine) {}

ControlPlaneServer::~ControlPlaneServer() {
    stop();
}

void ControlPlaneServer::start() {
    running_ = true;
    thread_ = std::thread(&ControlPlaneServer::run, this);
}

void ControlPlaneServer::stop() {
    running_ = false;
    if (thread_.joinable()) {
        thread_.join();
    }
}

void ControlPlaneServer::run() {
    try {
        net::io_context ioc{1};
        net::ip::tcp::acceptor acceptor{ioc, {net::ip::tcp::v4(), port_}};

        while (running_) {
            net::ip::tcp::socket socket{ioc};
            acceptor.accept(socket);

            beast::flat_buffer buffer;
            http::request<http::string_body> req;
            http::read(socket, buffer, req);

            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "application/json");
            res.keep_alive(false);

            try {
                if (req.method() == http::verb::post && req.target() == "/route") {
                    auto body = json::parse(req.body());
                    if (!body.as_object().contains("request_id")) {
                        res.result(http::status::bad_request);
                        res.body() = "{\"error\":\"Missing request_id\"}";
                    } else {
                        const auto request_id = json::value_to<std::string>(body.at("request_id"));
                        const auto source_ip_str = json::value_to<std::string>(body.at("source_ip"));
                        const auto source_port = json::value_to<uint16_t>(body.at("source_port"));
                        
                        // Parse IP address
                        common::ConnectionTuple tuple{};
                        size_t pos = 0;
                        for (int i = 0; i < 4; ++i) {
                            size_t next = source_ip_str.find('.', pos);
                            if (next == std::string::npos) next = source_ip_str.length();
                            tuple.src_ip[i] = static_cast<uint8_t>(std::stoi(source_ip_str.substr(pos, next - pos)));
                            pos = next + 1;
                        }
                        tuple.src_port = source_port;
                        tuple.dst_port = 80; // Default HTTP port
                        tuple.protocol = 6; // TCP
                        
                        auto decision = engine_.route(tuple);
                        if (decision.endpoint) {
                            json::object response;
                            response["request_id"] = request_id;
                            json::object server;
                            server["name"] = decision.endpoint->name;
                            std::string ip_str = std::to_string(decision.endpoint->ip[0]) + "." +
                                                std::to_string(decision.endpoint->ip[1]) + "." +
                                                std::to_string(decision.endpoint->ip[2]) + "." +
                                                std::to_string(decision.endpoint->ip[3]);
                            server["host"] = ip_str;
                            server["port"] = decision.endpoint->port;
                            server["url"] = "http://" + ip_str + ":" + std::to_string(decision.endpoint->port);
                            response["server"] = server;
                            res.body() = json::serialize(response);
                        } else {
                            res.result(http::status::service_unavailable);
                            res.body() = "{\"error\":\"No server available\"}";
                        }
                    }
                } else if (req.method() == http::verb::post && req.target() == "/end-connection") {
                    auto body = json::parse(req.body());
                    if (!body.as_object().contains("request_id")) {
                        res.result(http::status::bad_request);
                        res.body() = "{\"error\":\"Missing request_id\"}";
                    } else {
                        const auto request_id = json::value_to<std::string>(body.at("request_id"));
                        const auto source_ip_str = json::value_to<std::string>(body.at("source_ip"));
                        const auto source_port = json::value_to<uint16_t>(body.at("source_port"));
                        
                        // Parse IP and create tuple key
                        common::ConnectionTuple tuple{};
                        size_t pos = 0;
                        for (int i = 0; i < 4; ++i) {
                            size_t next = source_ip_str.find('.', pos);
                            if (next == std::string::npos) next = source_ip_str.length();
                            tuple.src_ip[i] = static_cast<uint8_t>(std::stoi(source_ip_str.substr(pos, next - pos)));
                            pos = next + 1;
                        }
                        tuple.src_port = source_port;
                        tuple.dst_port = 80;
                        tuple.protocol = 6;
                        
                        engine_.end_connection(tuple.to_string());
                        json::object response;
                        response["status"] = "success";
                        response["request_id"] = request_id;
                        res.body() = json::serialize(response);
                    }
                } else if (req.method() == http::verb::post && req.target() == "/update-server-metrics") {
                    auto body = json::parse(req.body());
                    const auto name = json::value_to<std::string>(body.at("server_name"));
                    const auto cpu = json::value_to<double>(body.at("cpu_utilization"));
                    const auto ram = json::value_to<double>(body.at("ram_utilization"));
                    const auto healthy = json::value_to<bool>(body.at("health_status"));
                    engine_.update_server_metrics(name, cpu, ram, healthy);
                    res.body() = "{\"status\":\"ok\"}";
                } else if (req.method() == http::verb::get && req.target() == "/stats") {
                    const auto total_routes = engine_.stateless_routes() + engine_.stateful_routes();
                    const auto stateless_pct = total_routes > 0 ? (engine_.stateless_routes() * 100.0 / total_routes) : 0.0;
                    const auto stateful_pct = total_routes > 0 ? (engine_.stateful_routes() * 100.0 / total_routes) : 0.0;
                    
                    const auto servers_snapshot = engine_.snapshot_servers();
                    size_t healthy_count = 0;
                    for (const auto& ep : servers_snapshot) {
                        if (ep.healthy) ++healthy_count;
                    }
                    
                    json::object summary;
                    summary["total_servers"] = static_cast<int64_t>(servers_snapshot.size());
                    summary["healthy_servers"] = static_cast<int64_t>(healthy_count);
                    summary["total_connections"] = static_cast<int64_t>(engine_.total_connections());
                    summary["avg_cpu"] = engine_.avg_cpu();
                    summary["avg_ram"] = engine_.avg_ram();
                    summary["stateful_table_size"] = static_cast<int64_t>(engine_.stateful_entry_count());
                    summary["stateful_table_capacity"] = static_cast<int64_t>(engine_.config().max_stateful_entries);
                    
                    json::object routing_stats;
                    routing_stats["stateless_routes"] = static_cast<int64_t>(engine_.stateless_routes());
                    routing_stats["stateful_routes"] = static_cast<int64_t>(engine_.stateful_routes());
                    routing_stats["stateless_percentage"] = stateless_pct;
                    routing_stats["stateful_percentage"] = stateful_pct;
                    routing_stats["routing_changes"] = static_cast<int64_t>(engine_.routing_changes());
                    summary["routing_stats"] = routing_stats;

                    json::object servers_obj;
                    for (const auto& ep : servers_snapshot) {
                        json::object srv;
                        srv["health"] = ep.healthy ? "healthy" : "unhealthy";
                        srv["connections"] = static_cast<int64_t>(engine_.get_server_connection_count(ep.name));
                        srv["cpu"] = ep.cpu_utilization;
                        srv["ram"] = ep.ram_utilization;
                        std::string ip_str = std::to_string(ep.ip[0]) + "." +
                                            std::to_string(ep.ip[1]) + "." +
                                            std::to_string(ep.ip[2]) + "." +
                                            std::to_string(ep.ip[3]);
                        srv["host"] = ip_str;
                        srv["port"] = static_cast<int64_t>(ep.port);
                        servers_obj[ep.name] = srv;
                    }

                    // Add connection statistics
                    const auto conn_stats = engine_.get_connection_stats();
                    json::array connection_stats_array;
                    for (const auto& stat : conn_stats) {
                        json::object stat_obj;
                        auto time_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                            stat.timestamp.time_since_epoch()).count();
                        stat_obj["timestamp"] = static_cast<int64_t>(time_since_epoch);
                        stat_obj["server_id"] = stat.server_id;
                        stat_obj["was_stateful"] = stat.was_stateful;
                        connection_stats_array.push_back(stat_obj);
                    }
                    
                    json::object payload;
                    payload["summary"] = summary;
                    payload["servers"] = servers_obj;
                    payload["connection_stats"] = connection_stats_array;
                    res.body() = json::serialize(payload);
                } else if (req.method() == http::verb::get && req.target() == "/health") {
                    json::object health;
                    health["status"] = "healthy";
                    res.body() = json::serialize(health);
                } else {
                    res.result(http::status::not_found);
                    res.body() = "{\"error\":\"unknown endpoint\"}";
                }
            } catch (const std::exception& ex) {
                res.result(http::status::internal_server_error);
                res.body() = std::string("{\"error\":\"") + ex.what() + "\"}";
            }

            res.prepare_payload();
            http::write(socket, res);
        }
    } catch (const std::exception& ex) {
        std::cerr << "Control plane error: " << ex.what() << std::endl;
    }
}

} // namespace balancify::lb

