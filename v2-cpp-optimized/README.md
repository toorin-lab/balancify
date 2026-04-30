# Balancify C++ Edition (v2-cpp-optimized)

This directory contains a high-performance C++ rewrite of the Balancify load balancer stack. The implementation maintains the hybrid stateless/stateful routing logic from the Python version, adds a DPDK-based data plane for optimized packet processing, and configures Intel Cache Allocation Technology (CAT) to pin the connection table inside the Last Level Cache (LLC).

## Directory Structure

```
v2-cpp-optimized/
├── load_balancer/    # DPDK pipeline + control plane HTTP API
├── server/           # Boost.Beast HTTP server
├── monitor/          # Metrics collection agent
├── client/           # Traffic generator
├── common/           # Shared utilities (Bloom filter, Maglev, etc.)
└── docker/           # Docker configurations
```

## Components

| Component | Description |
|-----------|-------------|
| `load_balancer/` | DPDK pipeline + control plane HTTP API (`/route`, `/update-server-metrics`, `/stats`) |
| `server/` | Boost.Beast HTTP server that exposes `/process` and `/metrics` endpoints |
| `monitor/` | Agent that periodically sends CPU/RAM metrics to the load balancer |
| `client/` | Traffic generator that exercises the `/route` control-plane API |
| `common/` | Shared utilities including Bloom filter and Maglev hashing |

## Build

The project uses CMake and depends on:

- `g++` (C++20)
- `cmake` / `ninja`
- `libdpdk-dev`
- `libpqos-dev`
- `libboost-all-dev`

```bash
cd v2-cpp-optimized
cmake -S . -B build -GNinja
cmake --build build
```

Executables will be located in `build/load_balancer/balancify_lb`, etc.

## Running with Docker

The `docker/` directory provides Dockerfiles for each component and `docker-compose-balancify-cpp.yml` replicates the original Python topology entirely in C++.

```bash
docker compose -f docker/docker-compose-balancify-cpp.yml up --build
```

> ⚠️ DPDK requires elevated privileges and access to hugepages. The load balancer container uses `cap_add` with `NET_ADMIN`/`IPC_LOCK` and expects hugepages to be available on the host.
