#include "balancify_engine.hpp"
#include "control_plane.hpp"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>

using namespace balancify::common;
using namespace balancify::lb;

int main(int argc, char** argv) {
    try {
        LoadBalancerConfig cfg;
        cfg.cpu_threshold = std::stod(std::getenv("CPU_THRESHOLD") ? std::getenv("CPU_THRESHOLD") : "20.0");
        cfg.ram_threshold = std::stod(std::getenv("RAM_THRESHOLD") ? std::getenv("RAM_THRESHOLD") : "2.0");
        cfg.max_stateful_entries = static_cast<size_t>(std::stoul(std::getenv("MAX_STATEFUL_ENTRIES") ? std::getenv("MAX_STATEFUL_ENTRIES") : "1000"));
        cfg.bloom_threshold = static_cast<size_t>(std::stoul(std::getenv("BLOOM_THRESHOLD") ? std::getenv("BLOOM_THRESHOLD") : "500"));
        cfg.bloom_error_rate = std::stod(std::getenv("BLOOM_ERROR_RATE") ? std::getenv("BLOOM_ERROR_RATE") : "0.01");
        cfg.monitoring_interval = static_cast<size_t>(std::stoul(std::getenv("MONITORING_INTERVAL") ? std::getenv("MONITORING_INTERVAL") : "10"));

        // Servers are described via environment variables SERVER_NAME_n, SERVER_IP_n, etc.
        const int server_count = std::stoi(std::getenv("SERVER_COUNT") ? std::getenv("SERVER_COUNT") : "0");
        for (int i = 0; i < server_count; ++i) {
            ServerEndpoint ep;
            const std::string key = "SERVER_" + std::to_string(i) + "_NAME";
            if (const char* value = std::getenv(key.c_str()); value) {
                ep.name = value;
            } else {
                ep.name = "server-" + std::to_string(i + 1);
            }
            ep.ip = {192, 168, 0, static_cast<uint8_t>(10 + i)};
            ep.port = 5000;
            cfg.endpoints.push_back(ep);
        }

        BalancifyEngine engine(cfg);

        ControlPlaneServer control_plane(8080, engine);
        control_plane.start();

        std::cout << "C++ Balancify load balancer running (HTTP-only mode)." << std::endl;
        std::cout << "Press Ctrl+C to exit." << std::endl;

        auto shutdown = [&engine, &control_plane]() {
            std::cout << "\nShutting down..." << std::endl;
            engine.stop_monitoring();
            control_plane.stop();
            std::exit(0);
        };
        
        std::signal(SIGINT, [](int) {
            // Signal handler - will be handled in main loop
        });

        bool running = true;
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            // Check for shutdown signal (simplified - in production use proper signal handling)
        }

        engine.stop_monitoring();
        control_plane.stop();
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
}

