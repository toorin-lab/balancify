# Separate Docker Compose Files for Load Balancers

This directory contains separate Docker Compose files for each load balancer implementation, making it easy to test them individually.

## Available Load Balancers

### 1. **Stateless Load Balancer** (`docker-compose-stateless.yml`)
- **Port**: 8080
- **Type**: Pure stateless using Maglev consistent hashing
- **Features**: No connection tracking, zero memory overhead
- **Use Case**: Simple, high-throughput scenarios

### 2. **Stateful Load Balancer** (`docker-compose-stateful.yml`)
- **Port**: 8081
- **Type**: Pure stateful using connection tracking
- **Features**: Perfect load distribution, high memory usage
- **Use Case**: High-performance requirements

### 3. **Balancify Load Balancer** (`docker-compose-balancify.yml`)
- **Port**: 8082
- **Type**: Hybrid with dictionary-based stateful table
- **Features**: Conservative thresholds (20% CPU, 2GB RAM)
- **Use Case**: Production hybrid load balancing

## Usage Instructions

### Starting a Single Load Balancer

To test a specific load balancer:

```bash
# Start Stateless Load Balancer
docker compose -f docker-compose-stateless.yml up -d

# Start Stateful Load Balancer
docker compose -f docker-compose-stateful.yml up -d

# Start Balancify Load Balancer
docker compose -f docker-compose-balancify.yml up -d
```

### Stopping a Load Balancer

```bash
# Stop Stateless Load Balancer
docker compose -f docker-compose-stateless.yml down

# Stop Stateful Load Balancer
docker compose -f docker-compose-stateful.yml down

# Stop Balancify Load Balancer 
docker compose -f docker-compose-balancify.yml down
```

### Checking Load Balancer Status

```bash
# Check if load balancer is running
curl http://localhost:8080/health  # Stateless
curl http://localhost:8081/health  # Stateful
curl http://localhost:8082/health  # Balancify

# Get load balancer statistics
curl http://localhost:8080/stats   # Stateless
curl http://localhost:8081/stats   # Stateful
curl http://localhost:8082/stats   # Balancify
```

### Testing Load Balancers

Each load balancer comes with its own client that automatically generates traffic:

```bash
# The client will automatically start and generate traffic
# Check client logs to see traffic generation
docker logs client-stateless
docker logs client-stateful
docker logs client-balancify
```

## C++ Edition (DPDK + Intel CAT)

The `cpp_balancify/` directory contains a full C++ rewrite of Balancify featuring a DPDK data plane and Intel CAT integration. Each component (load balancer, backend servers, monitors, and clients) is dockerized in `cpp_balancify/docker/docker-compose-balancify-cpp.yml`.

## Configuration Details

### Environment Variables

Each load balancer can be configured using environment variables:

#### Stateless & Stateful
- `SERVER_COUNT`: Number of servers (default: 3)
- `SERVER_X_CAPACITY`: Capacity of server X (default: 30)
- `LB_PORT`: Load balancer port (default: 8080)

#### Balancify
- `CPU_THRESHOLD`: CPU utilization threshold for stateful routing (default: 20.0%)
- `RAM_THRESHOLD`: RAM usage threshold for stateful routing (default: 2.0 GB)
- `MAX_STATEFUL_ENTRIES`: Maximum entries in stateful table (default: 1000)
- `MONITORING_INTERVAL`: Health check interval in seconds (default: 10)

### Client Configuration

Each client can be configured with:

- `TEST_DURATION`: Test duration in seconds (default: 300)
- `TRAFFIC_PATTERN`: Traffic pattern (normal, burst, gradual, mixed, sequential)
- `REQUEST_INTERVAL`: Interval between requests in seconds (default: 0.5)
- `BURST_SIZE`: Number of requests in burst (default: 30)
- `QUIET_PERIOD`: Quiet period between bursts in seconds (default: 2.0)

## Monitoring

Each load balancer includes:

- **3 Server Monitors**: Monitor CPU, RAM, and health of each server
- **1 Client**: Generate traffic for testing
- **Health Checks**: Automatic health monitoring
- **Statistics**: Real-time performance metrics

## Performance Comparison

To compare performance between load balancers:

1. **Start each load balancer separately**
2. **Let them run for the same duration** (e.g., 5 minutes)
3. **Collect statistics** from each endpoint
4. **Compare metrics**:
   - Load distribution
   - Throughput
   - Memory usage
   - Stateful routing percentage

## Troubleshooting

### Common Issues

1. **Port Conflicts**: Each load balancer uses a different port
   - Stateless: 8080
   - Stateful: 8081
   - Balancify: 8082

2. **Container Names**: Each container has unique names to avoid conflicts

3. **Network Isolation**: Each load balancer uses its own network

### Logs

Check logs for debugging:

```bash
# Load balancer logs
docker logs load-balancer-stateless
docker logs load-balancer-stateful
docker logs load-balancer-balancify

# Server logs
docker logs server-1-stateless
docker logs server-2-stateless
docker logs server-3-stateless

# Client logs
docker logs client-stateless
```

## Cleanup

To clean up all containers and networks:

```bash
# Stop and remove all containers
docker compose -f docker-compose-stateless.yml down
docker compose -f docker-compose-stateful.yml down
docker compose -f docker-compose-balancify.yml down

# Remove all images (optional)
docker rmi $(docker images -q lb-*)
```
