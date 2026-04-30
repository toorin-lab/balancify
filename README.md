# Balancify

This repository contains the implementation of Balancify, for the paper:

> **Toward Efficient Layer-4 Load Balancing: A Hybrid Stateful–Stateless Approach**  
> *Amirhossein Sadr, Kowsar Pakzad, Mohammad Hosseini, Hannaneh B. Pasandi, and Sina Darabi*  
> ACM 21st International Conference on Emerging Networking Experiments and Technologies (CoNEXT) Student Workshop, Hong Kong, 2025. — **🏆 Recipient of Best Paper Award and Best Contribution Award**

Balancify is a hybrid of stateless and stateful load balancing approaches, providing the benefits of both: high throughput, low latency, and balanced load distribution across servers.

## Overview

At a high level, Balancify operates as follows:
- For a new request, Balancify first applies consistent hashing; if the selected server is significantly more loaded than others, it discards that result and picks the least-loaded server instead.
- When the least-loaded server is chosen (bypassing the hash function), that assignment is recorded in a connection-to-server table for the request.
- Packet forwarding always checks the stateful table first; if no matching entry is found, it falls back to the hash-based forwarding method.

## Repository Structure

```
balancify/
├── v1-python/          # Python implementation with Flask-based load balancers
├── v2-cpp-optimized/   # C++ implementation with DPDK data plane and Intel CAT
├── simulation/         # Simulation scripts for testing and analysis
└── LICENSE            # MIT License
```

## Versions

### v1-python
Python-based implementation featuring:
- Three load balancer variants: Stateless, Stateful, and Balancify (hybrid)
- Flask-based HTTP API
- Docker Compose configurations for easy deployment
- Real-time monitoring and statistics

See [v1-python/README.md](v1-python/README.md) for detailed documentation.

### v2-cpp-optimized
High-performance C++ implementation featuring:
- DPDK-based data plane for packet processing
- Intel Cache Allocation Technology (CAT) integration
- Boost.Beast HTTP servers
- Optimized connection tracking

See [v2-cpp-optimized/README.md](v2-cpp-optimized/README.md) for detailed documentation.

### simulation
Simulation scripts for testing and analyzing load balancing algorithms.

## Quick Start

### Python Version (v1-python)

```bash
cd v1-python
docker compose -f docker-compose-balancify.yml up -d
```

### C++ Version (v2-cpp-optimized)

```bash
cd v2-cpp-optimized
docker compose -f docker/docker-compose-balancify-cpp.yml up --build
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Copyright

Copyright (c) 2025 Toorin

## Cite

If you use this code in your research, please cite the following paper:

```bibtex
@inproceedings{sadr2025toward,
  title={Toward Efficient Layer-4 Load Balancing: A Hybrid Stateful{\^a}€“Stateless Approach},
  author={Sadr, Amirhossein and Pakzad, Kowsar and Hosseini, Mohammad and Pasandi, Hannaneh B and Darabi, Sina},
  booktitle={Proceedings of the CoNEXT'25 Student workshop},
  pages={5--6},
  year={2025}
}

