# Balancify

A hybrid layer-4 load balaner that intelligently switches between stateful and stateless routing based on server load conditions.

## Overview

Balancify implements a novel hybrid load balancing approach that:
- Uses **stateless routing** (consistent hashing) when servers are under low load
- Automatically switches to **stateful routing** (connection tracking) when servers exceed CPU/RAM thresholds
- Provides optimal load distribution while minimizing memory overhead

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

