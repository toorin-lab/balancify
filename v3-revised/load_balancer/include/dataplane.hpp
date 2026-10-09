#pragma once

#include "runtime.hpp"
#include "worker.hpp"

#include <rte_mempool.h>

#include <memory>
#include <vector>

namespace balancify::lb {

// Port, queues, mempools and rings; one RX/TX queue pair per forwarding core,
// with RSS spreading connections over the queues.
class Dataplane {
public:
    explicit Dataplane(Runtime& rt);
    ~Dataplane();

    void init();
    void launch();
    void stop();

    // The core whose RSS queue receives packets of this 5-tuple, computed in
    // software with the same Toeplitz key and redirection table as the NIC.
    unsigned owner_worker(const FiveTuple& t) const;

private:
    void create_rings();
    void program_reta();

    Runtime& rt_;
    std::vector<rte_mempool*> pools_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::vector<uint8_t> rss_key_;
    std::vector<uint16_t> reta_;
    bool started_{false};
    bool launched_{false};
};

} // namespace balancify::lb
