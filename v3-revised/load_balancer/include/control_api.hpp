#pragma once

#include "stats.hpp"

#include <atomic>
#include <map>
#include <string>
#include <thread>

namespace balancify::lb {

// HTTP management interface (kernel network stack, management address).
//
//   GET  /stats                         counters, parameters, coordination state
//   GET  /dips                          per-DIP health, load, and bucket share
//   GET  /ready                         200 once the local copy is synchronized
//   POST /params?threshold=&interval=&rule=r0|r1|r2
//   POST /dips/add?name=&ip=&mac=&weight=
//   POST /dips/drain?name=              remove a DIP from the pool
//   POST /dips/enable?name=             return a drained DIP to the pool
class ControlApi {
public:
    explicit ControlApi(StatsSources src);
    ~ControlApi();
    void start();
    void stop();

private:
    void run();
    std::string handle(bool post, const std::string& target, int& status);

    StatsSources src_;
    std::atomic<bool> running_{false};
    std::thread thread_;
};

} // namespace balancify::lb
