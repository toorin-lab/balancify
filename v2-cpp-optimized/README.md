# Balancify C++ Edition

This folder contains a C++ rewrite of the Balancify load balancer stack. The new implementation keeps the hybrid stateless/stateful routing logic, adds a DPDK-based data plane, and configures Intel Cache Allocation Technology (CAT) to pin the connection table inside the LLC.

## Components

| Component | Description |
|-----------|-------------|
| `load_balancer/` | DPDK pipeline + control plane HTTP API (`/route`, `/update-server-metrics`, `/stats`). |
| `server/` | Simple Boost.Beast HTTP server that exposes `/process` and `/metrics` endpoints. |
| `monitor/` | Agent that periodically sends CPU/RAM metrics to the load balancer (`/update-server-metrics`). |
| `client/` | Traffic generator that exercises the `/route` control-plane API. |

## Build

The project uses CMake and depends on:

- `g++` (C++20)
- `cmake` / `ninja`
- `libdpdk-dev`
- `libpqos-dev`
- `libboost-all-dev`

```bash
cd cpp_balancify
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

