#pragma once

#include "balancify_engine.hpp"
#include <atomic>
#include <thread>

namespace balancify::lb {

class ControlPlaneServer {
public:
    ControlPlaneServer(unsigned short port, BalancifyEngine& engine);
    ~ControlPlaneServer();

    void start();
    void stop();

private:
    void run();

    unsigned short port_;
    BalancifyEngine& engine_;
    std::atomic<bool> running_{false};
    std::thread thread_;
};

} // namespace balancify::lb

