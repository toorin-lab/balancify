import hashlib
import threading
import time
import random
import logging
import math
from collections import deque
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RoutingMode(Enum):
    """Routing modes for the load balancer"""
    STATELESS = "stateless"
    STATEFUL = "stateful"
    HYBRID = "hybrid"


@dataclass
class ConnectionStats:
    """Statistics for a connection"""
    timestamp: float
    server_id: str
    mode: RoutingMode


class Connection:
    def __init__(self, id, protocol="HTTP", source_ip=None, source_port=None,
                 dest_ip=None, dest_port=None):
        self.id = id
        self.protocol = protocol
        self.source_ip = source_ip or f"192.168.1.{random.randint(1, 255)}"
        self.source_port = source_port or random.randint(1024, 65535)
        self.dest_ip = dest_ip or "10.0.0.1"
        self.dest_port = dest_port or (80 if protocol == "HTTP" else 443)
        self.is_new = True
        self.start_time = time.time()

    def get_5_tuple(self) -> Tuple[str, int, str, int, str]:
        """Get the 5-tuple for consistent hashing"""
        return (self.source_ip, self.source_port, self.dest_ip,
                self.dest_port, self.protocol)

    def get_5_tuple_hash(self) -> int:
        """Get hash of the 5-tuple"""
        tuple_str = f"{self.source_ip}:{self.source_port}-{self.dest_ip}:{self.dest_port}-{self.protocol}"
        return int(hashlib.sha256(tuple_str.encode()).hexdigest(), 16)


class Server:
    def __init__(self, name, capacity_threshold):
        self.name = name
        self.capacity_threshold = capacity_threshold
        self.current_load = 0
        self.connections = []
        self.cpu_utilization = 0.0
        self.ram_utilization = 0.0
        self.health_status = True
        self.total_connections_served = 0
        self.connection_history = deque(maxlen=1000)  # Keep last 1000 connections

    def add_connection(self, connection):
        self.current_load += 1
        self.connections.append(connection)
        self.total_connections_served += 1
        self.connection_history.append((time.time(), connection.id))
        self._update_resource_utilization()

    def remove_connection(self, connection):
        if self.current_load > 0:
            self.current_load -= 1
            if connection in self.connections:
                self.connections.remove(connection)
            self._update_resource_utilization()

    def _update_resource_utilization(self):
        """Simulate CPU and RAM utilization with more realistic patterns"""
        base_cpu = 10.0
        base_ram = 2.0

        # Heterogeneous load simulation with connection age factor
        cpu_per_conn = []
        ram_per_conn = []

        for conn in self.connections:
            age_factor = min(2.0, 1.0 + (time.time() - conn.start_time) / 60)  # Older connections use more resources
            cpu_per_conn.append(random.uniform(5, 15) * age_factor)
            ram_per_conn.append(random.uniform(0.1, 0.5) * age_factor)

        self.cpu_utilization = min(100.0, base_cpu + sum(cpu_per_conn))
        self.ram_utilization = base_ram + sum(ram_per_conn)

    def get_load_score(self) -> float:
        """Calculate a combined load score for the server"""
        # Weighted combination of different metrics
        cpu_weight = 0.4
        ram_weight = 0.3
        connection_weight = 0.3

        normalized_connections = self.current_load / max(1, self.capacity_threshold)
        normalized_cpu = self.cpu_utilization / 100.0
        normalized_ram = min(1.0, self.ram_utilization / 16.0)  # Assume 16GB max RAM

        return (cpu_weight * normalized_cpu +
                ram_weight * normalized_ram +
                connection_weight * normalized_connections)


class MaglevHashTable:
    """Maglev consistent hash table implementation"""

    def __init__(self, servers: List[Server], table_size: int = 65537):
        self.servers = servers
        self.table_size = table_size
        self.lookup_table = [None] * table_size
        self._build_table()

    def _hash(self, key: str, seed: int = 0) -> int:
        """Hash function for Maglev"""
        data = f"{key}:{seed}".encode()
        return int(hashlib.sha256(data).hexdigest(), 16)

    def _build_table(self):
        """Build the Maglev lookup table"""
        if not self.servers:
            return

        n = len(self.servers)
        permutation = []

        # Generate permutation for each server
        for server in self.servers:
            offset = self._hash(server.name, 0) % self.table_size
            skip = self._hash(server.name, 1) % (self.table_size - 1) + 1
            perm = []
            for j in range(self.table_size):
                perm.append((offset + j * skip) % self.table_size)
            permutation.append(perm)

        # Fill the lookup table
        next_indices = [0] * n
        for i in range(self.table_size):
            for j in range(n):
                c = permutation[j][next_indices[j]]
                while self.lookup_table[c] is not None:
                    next_indices[j] += 1
                    c = permutation[j][next_indices[j]]

                self.lookup_table[c] = j
                next_indices[j] += 1
                break

    def get_server(self, hash_value: int) -> Optional[Server]:
        """Get server for a given hash value"""
        if not self.servers:
            return None

        index = hash_value % self.table_size
        server_index = self.lookup_table[index]

        if server_index is not None and server_index < len(self.servers):
            return self.servers[server_index]
        return None


class CountingBloomFilter:
    """Counting Bloom filter for tracking connection membership"""

    def __init__(self, capacity: int, error_rate: float = 0.01):
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if not 0 < error_rate < 1:
            raise ValueError("Error rate must be between 0 and 1")

        self.capacity = capacity
        self.error_rate = error_rate
        self.num_bits = self._calculate_num_bits(capacity, error_rate)
        self.num_hashes = max(1, int(round((self.num_bits / capacity) * math.log(2))))
        self.counters = [0] * self.num_bits

    @staticmethod
    def _calculate_num_bits(capacity: int, error_rate: float) -> int:
        """Calculate the optimal number of bits"""
        numerator = -capacity * math.log(error_rate)
        denominator = math.log(2) ** 2
        return max(8, int(math.ceil(numerator / denominator)))

    def _hashes(self, item: str):
        """Generate hash positions using double hashing"""
        item_bytes = item.encode('utf-8')
        h1 = int(hashlib.sha256(item_bytes + b'\x00').hexdigest(), 16)
        h2 = int(hashlib.sha256(item_bytes + b'\x01').hexdigest(), 16) or 1

        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.num_bits

    def add(self, item: str):
        for position in self._hashes(item):
            self.counters[position] += 1

    def remove(self, item: str):
        for position in self._hashes(item):
            if self.counters[position] > 0:
                self.counters[position] -= 1

    def contains(self, item: str) -> bool:
        for position in self._hashes(item):
            if self.counters[position] == 0:
                return False
        return True

    def reset(self):
        self.counters = [0] * self.num_bits


class BalancifyLoadBalancer:
    def __init__(self, servers, cpu_threshold=20.0, ram_threshold=2.0,
                 max_stateful_entries=1000, monitoring_interval=10):
        self.servers = servers
        self.cpu_threshold = cpu_threshold
        self.ram_threshold = ram_threshold
        self.max_stateful_entries = max_stateful_entries
        self.monitoring_interval = monitoring_interval

        # Connection tracking
        self.connection_table = {}  # Stateful connection mapping (5-tuple -> server)
        self.connection_stats = {}  # Connection statistics (connection.id -> stats)
        self.connection_id_to_5tuple = {}  # Reverse mapping: connection.id -> 5-tuple key

        # Maglev hash table
        self.maglev_table = MaglevHashTable(servers)

        # Load metrics
        self.avg_cpu_load = 0.0
        self.avg_ram_load = 0.0
        self.server_loads = {}

        # Routing statistics
        self.stateless_routes = 0
        self.stateful_routes = 0
        self.routing_changes = 0

        # Monitoring thread
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitoring_thread, daemon=True)
        self.monitor_thread.start()

        # Initial metrics update
        self._update_load_metrics()

        # Bloom filter configuration
        self.bloom_threshold = max_stateful_entries // 2 or 1
        self.bloom_error_rate = 0.01
        self.bloom_filter: Optional[CountingBloomFilter] = None
        self.bloom_active = False

        logger.info(f"Balancify Load Balancer initialized with {len(servers)} servers")

    def _monitoring_thread(self):
        """Monitoring thread that updates metrics and performs maintenance"""
        while self.monitoring_active:
            self._update_load_metrics()
            self._cleanup_stale_connections()
            time.sleep(self.monitoring_interval)

    def _update_load_metrics(self):
        """Update average CPU and RAM utilization across all servers by querying DIPs (Algorithm 1)"""
        total_cpu = 0.0
        total_ram = 0.0
        active_servers = 0

        # Query all DIPs for CPU utilization (simulated - directly access server metrics)
        for i, server in enumerate(self.servers):
            if server.health_status:
                # Simulate querying server agent for CPU utilization
                cpu_load = self._get_load(i)  # GetLoad(i) from Algorithm 1
                ram_load = server.ram_utilization
                
                self.server_loads[server.name] = {
                    'cpu': cpu_load,
                    'ram': ram_load,
                    'connections': server.current_load,
                    'score': server.get_load_score()
                }
                total_cpu += cpu_load
                total_ram += ram_load
                active_servers += 1

        if active_servers > 0:
            self.avg_cpu_load = total_cpu / active_servers
            self.avg_ram_load = total_ram / active_servers
    
    def _get_load(self, server_index: int) -> float:
        """GetLoad function for Algorithm 1 - gets CPU utilization from server"""
        if server_index < 0 or server_index >= len(self.servers):
            return 0.0
        return self.servers[server_index].cpu_utilization

    def _cleanup_stale_connections(self):
        """Remove stale entries from connection table (Point 5: using 5-tuple keys)"""
        current_time = time.time()
        stale_timeout = 300  # 5 minutes

        stale_connections = []
        for conn_id, stats in list(self.connection_stats.items()):
            if stats.timestamp < current_time - stale_timeout:
                stale_connections.append(conn_id)

        # Clean up stale entries using reverse mapping
        for conn_id in stale_connections:
            if conn_id in self.connection_id_to_5tuple:
                conn_5_tuple_key = self.connection_id_to_5tuple[conn_id]
                if conn_5_tuple_key in self.connection_table:
                    del self.connection_table[conn_5_tuple_key]
                del self.connection_id_to_5tuple[conn_id]
            if conn_id in self.connection_stats:
                del self.connection_stats[conn_id]
                logger.debug(f"Removed stale connection {conn_id}")

    def _maybe_toggle_bloom_filter(self):
        """Enable/disable bloom filter based on table size"""
        table_size = len(self.connection_table)
        if table_size >= self.bloom_threshold:
            if not self.bloom_active or self.bloom_filter is None:
                self._rebuild_bloom_filter()
        elif self.bloom_active and table_size < max(1, int(self.bloom_threshold * 0.5)):
            logger.info("Disabling Bloom filter due to smaller connection table")
            self.bloom_filter = None
            self.bloom_active = False

    def _rebuild_bloom_filter(self):
        """Rebuild Bloom filter from current table"""
        capacity = max(len(self.connection_table), self.bloom_threshold)
        self.bloom_filter = CountingBloomFilter(capacity=capacity, error_rate=self.bloom_error_rate)
        for key in self.connection_table.keys():
            self.bloom_filter.add(key)
        self.bloom_active = True
        logger.info(f"Bloom filter rebuilt with capacity {capacity} (entries={len(self.connection_table)})")

    def _on_stateful_entry_added(self, conn_key: str):
        """Track additions for Bloom filter"""
        if self.bloom_active and self.bloom_filter:
            self.bloom_filter.add(conn_key)
        self._maybe_toggle_bloom_filter()

    def _on_stateful_entry_removed(self, conn_key: str):
        """Track removals for Bloom filter"""
        if self.bloom_active and self.bloom_filter:
            self.bloom_filter.remove(conn_key)
        self._maybe_toggle_bloom_filter()

    def _get_stateful_server(self, conn_key: str) -> Optional[Server]:
        """Lookup server via Bloom filter + table"""
        if self.bloom_active and self.bloom_filter:
            if not self.bloom_filter.contains(conn_key):
                return None
        return self.connection_table.get(conn_key)

    def _rebalance_if_needed(self):
        """Check if rebalancing is needed and rebuild Maglev table if necessary"""
        # This method is now deprecated - rebuilds only happen in set_server_health()
        # or when explicitly adding/removing servers
        pass

    def _should_use_stateful_routing(self, server: Server) -> bool:
        """Determine if stateful routing should be used based on CPU threshold (Algorithm 2) - Point 3"""
        # Simple threshold comparison: dips_load[dip] - avg_load > THRESHOLD
        cpu_diff = server.cpu_utilization - self.avg_cpu_load
        return cpu_diff > self.cpu_threshold

    def route_connection(self, connection: Connection) -> Server:
        """Route a connection using hybrid approach"""
        conn_5_tuple = connection.get_5_tuple()
        conn_hash = connection.get_5_tuple_hash()

        # Update connection stats
        self.connection_stats[connection.id] = ConnectionStats(
            timestamp=time.time(),
            server_id=None,
            mode=RoutingMode.HYBRID
        )

        # Check stateful table first using 5-tuple directly (Point 5) and Bloom filter
        conn_5_tuple_key = str(conn_5_tuple)
        server = self._get_stateful_server(conn_5_tuple_key)
        if server:
            if server.health_status:
                self.stateful_routes += 1
                self.connection_stats[connection.id].mode = RoutingMode.STATEFUL
                logger.debug(f"[STATEFUL] Connection {connection.id} -> {server.name}")
                return server
            else:
                # Server unhealthy, remove from stateful table
                del self.connection_table[conn_5_tuple_key]
                self._on_stateful_entry_removed(conn_5_tuple_key)

        # For new connections, use Maglev consistent hashing
        selected_server = self.maglev_table.get_server(conn_hash)

        if not selected_server:
            # Fallback to least loaded server
            selected_server = self._find_least_loaded_server()
            if not selected_server:
                raise RuntimeError("No healthy servers available")

        # Check if we should add to stateful table (Point 5: using 5-tuple key)
        if self._should_use_stateful_routing(selected_server):
            # Find a better server if current one is overloaded
            alternative = self._find_least_loaded_server()

            if (alternative and alternative != selected_server and
                    alternative.get_load_score() < selected_server.get_load_score() * 0.7):

                # Add to stateful table if not full (using 5-tuple as key)
                if len(self.connection_table) < self.max_stateful_entries:
                    self.connection_table[conn_5_tuple_key] = alternative
                    self.connection_id_to_5tuple[connection.id] = conn_5_tuple_key
                    self._on_stateful_entry_added(conn_5_tuple_key)
                    self.stateful_routes += 1
                    self.connection_stats[connection.id].mode = RoutingMode.STATEFUL
                    selected_server = alternative
                    logger.info(f"[STATEFUL] Connection {connection.id} -> {selected_server.name} "
                                f"(load score: {selected_server.get_load_score():.2f})")
                else:
                    # Table full, evict oldest entry
                    self._evict_oldest_stateful_entry()
                    self.connection_table[conn_5_tuple_key] = alternative
                    self.connection_id_to_5tuple[connection.id] = conn_5_tuple_key
                    self._on_stateful_entry_added(conn_5_tuple_key)
                    selected_server = alternative
            else:
                self.stateless_routes += 1
                self.connection_stats[connection.id].mode = RoutingMode.STATELESS
                logger.debug(f"[STATELESS] Connection {connection.id} -> {selected_server.name}")
        else:
            self.stateless_routes += 1
            self.connection_stats[connection.id].mode = RoutingMode.STATELESS
            logger.debug(f"[STATELESS] Connection {connection.id} -> {selected_server.name}")

        # Update connection stats with selected server
        self.connection_stats[connection.id].server_id = selected_server.name

        # Add connection to server
        selected_server.add_connection(connection)
        connection.is_new = False

        return selected_server

    def _find_least_loaded_server(self) -> Optional[Server]:
        """Find the least loaded healthy server"""
        healthy_servers = [s for s in self.servers if s.health_status]

        if not healthy_servers:
            return None

        return min(healthy_servers, key=lambda s: s.get_load_score())

    def _evict_oldest_stateful_entry(self):
        """Evict the oldest entry from stateful table (Point 5: using 5-tuple keys)"""
        if not self.connection_table:
            return

        oldest_conn_id = None
        oldest_time = float('inf')

        # Find oldest connection using connection_stats
        for conn_id, stats in self.connection_stats.items():
            if conn_id in self.connection_id_to_5tuple:
                timestamp = stats.timestamp
                if timestamp < oldest_time:
                    oldest_time = timestamp
                    oldest_conn_id = conn_id

        if oldest_conn_id and oldest_conn_id in self.connection_id_to_5tuple:
            conn_5_tuple_key = self.connection_id_to_5tuple[oldest_conn_id]
            if conn_5_tuple_key in self.connection_table:
                del self.connection_table[conn_5_tuple_key]
            del self.connection_id_to_5tuple[oldest_conn_id]
            self._on_stateful_entry_removed(conn_5_tuple_key)
            logger.debug(f"Evicted oldest connection {oldest_conn_id} from stateful table")

    def end_connection(self, connection: Connection):
        """End a connection and clean up resources (Point 5: using 5-tuple key)"""
        conn_id = connection.id

        # Find the server handling this connection
        server = None
        conn_5_tuple = connection.get_5_tuple()
        conn_5_tuple_key = str(conn_5_tuple)
        conn_hash = connection.get_5_tuple_hash()

        if conn_5_tuple_key in self.connection_table:
            server = self.connection_table.pop(conn_5_tuple_key)
            if conn_id in self.connection_id_to_5tuple:
                del self.connection_id_to_5tuple[conn_id]
            self._on_stateful_entry_removed(conn_5_tuple_key)
            logger.debug(f"[END] Connection {conn_id} removed from stateful table")
        else:
            # Try to find via Maglev hash
            server = self.maglev_table.get_server(conn_hash)

        if server:
            server.remove_connection(connection)

        # Clean up stats
        if conn_id in self.connection_stats:
            del self.connection_stats[conn_id]

    def get_stats(self) -> Dict:
        """Get comprehensive load balancer statistics"""
        total_connections = sum(s.current_load for s in self.servers)
        healthy_servers = sum(1 for s in self.servers if s.health_status)

        routing_total = self.stateless_routes + self.stateful_routes
        stateless_percentage = (self.stateless_routes / routing_total * 100) if routing_total > 0 else 0
        stateful_percentage = (self.stateful_routes / routing_total * 100) if routing_total > 0 else 0

        stats = {
            'summary': {
                'total_servers': len(self.servers),
                'healthy_servers': healthy_servers,
                'total_connections': total_connections,
                'avg_cpu': self.avg_cpu_load,
                'avg_ram': self.avg_ram_load,
                'stateful_table_size': len(self.connection_table),
                'stateful_table_capacity': self.max_stateful_entries,
                'bloom_filter_active': self.bloom_active,
                'bloom_filter_threshold': self.bloom_threshold,
                'routing_stats': {
                    'stateless_routes': self.stateless_routes,
                    'stateful_routes': self.stateful_routes,
                    'stateless_percentage': stateless_percentage,
                    'stateful_percentage': stateful_percentage,
                    'routing_changes': self.routing_changes
                }
            },
            'servers': {}
        }

        for server in self.servers:
            stats['servers'][server.name] = {
                'health': 'healthy' if server.health_status else 'unhealthy',
                'connections': server.current_load,
                'cpu': server.cpu_utilization,
                'ram': server.ram_utilization,
                'load_score': server.get_load_score(),
                'total_served': server.total_connections_served
            }

        return stats

    def set_server_health(self, server_name: str, health_status: bool):
        """Set health status of a server"""
        for server in self.servers:
            if server.name == server_name:
                server.health_status = health_status
                logger.info(f"Server {server_name} health status set to {health_status}")

                # Rebuild Maglev table if health status changed
                healthy_servers = [s for s in self.servers if s.health_status]
                self.maglev_table = MaglevHashTable(healthy_servers)
                break

    def add_server(self, server: Server):
        """Add a new server to the load balancer"""
        self.servers.append(server)
        logger.info(f"Added server {server.name} to load balancer")
        
        # Rebuild Maglev table with new server
        healthy_servers = [s for s in self.servers if s.health_status]
        self.maglev_table = MaglevHashTable(healthy_servers)

    def remove_server(self, server_name: str):
        """Remove a server from the load balancer"""
        for i, server in enumerate(self.servers):
            if server.name == server_name:
                # Remove server from list
                removed_server = self.servers.pop(i)
                logger.info(f"Removed server {server_name} from load balancer")
                
                # Clean up any connections that were routed to this server (Point 5: using 5-tuple keys)
                connections_to_remove = []
                for conn_5_tuple_key, server_obj in list(self.connection_table.items()):
                    if server_obj == removed_server:
                        connections_to_remove.append(conn_5_tuple_key)
                
                for conn_5_tuple_key in connections_to_remove:
                    del self.connection_table[conn_5_tuple_key]
                    self._on_stateful_entry_removed(conn_5_tuple_key)
                    # Find and remove from reverse mapping
                    conn_ids_to_remove = [cid for cid, key in self.connection_id_to_5tuple.items() 
                                         if key == conn_5_tuple_key]
                    for conn_id in conn_ids_to_remove:
                        del self.connection_id_to_5tuple[conn_id]
                        if conn_id in self.connection_stats:
                            del self.connection_stats[conn_id]
                
                # Rebuild Maglev table without the removed server
                healthy_servers = [s for s in self.servers if s.health_status]
                self.maglev_table = MaglevHashTable(healthy_servers)
                break
        else:
            logger.warning(f"Server {server_name} not found for removal")

    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join()


def simulate_balancify():
    """Enhanced simulation of Balancify load balancer"""
    print("=== Enhanced Balancify Load Balancer Simulation ===\n")

    # Create servers
    servers = [
        Server(name=f"S{i + 1}", capacity_threshold=10)
        for i in range(4)
    ]

    # Initialize Balancify load balancer
    lb = BalancifyLoadBalancer(
        servers=servers,
        cpu_threshold=20.0,
        ram_threshold=2.0,
        max_stateful_entries=100,
        monitoring_interval=10 # Faster for simulation
    )

    # Wait for initial metrics
    time.sleep(0.1)

    print("Initial state:")
    stats = lb.get_stats()
    print(f"Healthy servers: {stats['summary']['healthy_servers']}/{stats['summary']['total_servers']}")
    print(f"Average CPU: {stats['summary']['avg_cpu']:.1f}%, Average RAM: {stats['summary']['avg_ram']:.1f}GB")
    print(f"Bloom filter active: {stats['summary'].get('bloom_filter_active', False)}\n")

    connections = []

    # Phase 1: Normal load
    print("Phase 1: Normal load (20 connections)")
    for i in range(1, 21):
        conn = Connection(
            id=f"conn_{i}",
            protocol="HTTP" if i % 2 == 0 else "HTTPS"
        )
        server = lb.route_connection(conn)
        connections.append(conn)

    time.sleep(0.5)
    stats = lb.get_stats()
    print(f"\nRouting distribution: {stats['summary']['routing_stats']['stateless_percentage']:.1f}% stateless, "
          f"{stats['summary']['routing_stats']['stateful_percentage']:.1f}% stateful")
    print(f"Stateful table: {stats['summary']['stateful_table_size']}/{stats['summary']['stateful_table_capacity']}")
    print(f"Bloom filter active: {stats['summary'].get('bloom_filter_active', False)}")

    # Phase 2: Heavy load to trigger more stateful routing
    print("\n\nPhase 2: Heavy load (50 more connections)")
    for i in range(21, 71):
        conn = Connection(
            id=f"conn_{i}",
            protocol="TCP" if i % 3 == 0 else "HTTP"
        )
        server = lb.route_connection(conn)
        connections.append(conn)

    time.sleep(0.5)
    stats = lb.get_stats()
    print(f"\nAfter heavy load:")
    print(f"Routing distribution: {stats['summary']['routing_stats']['stateless_percentage']:.1f}% stateless, "
          f"{stats['summary']['routing_stats']['stateful_percentage']:.1f}% stateful")
    print(f"Stateful table: {stats['summary']['stateful_table_size']}/{stats['summary']['stateful_table_capacity']}")
    print(f"Bloom filter active: {stats['summary'].get('bloom_filter_active', False)}")

    for name, data in stats['servers'].items():
        print(f"  {name}: {data['connections']} connections, "
              f"CPU: {data['cpu']:.1f}%, RAM: {data['ram']:.1f}GB, "
              f"Load Score: {data['load_score']:.2f}")

    # Phase 3: Server failure simulation
    print("\n\nPhase 3: Simulating server failure")
    lb.set_server_health("S2", False)

    # Add more connections to see rebalancing
    for i in range(71, 81):
        conn = Connection(id=f"conn_{i}")
        server = lb.route_connection(conn)
        connections.append(conn)

    time.sleep(0.5)
    stats = lb.get_stats()
    print(f"\nAfter S2 failure:")
    print(f"Healthy servers: {stats['summary']['healthy_servers']}/{stats['summary']['total_servers']}")
    for name, data in stats['servers'].items():
        print(f"  {name}: {data['health']}, {data['connections']} connections")

    # Phase 4: Connection cleanup
    print("\n\nPhase 4: Removing connections")
    for i in range(20):
        if connections:
            conn = connections.pop(0)
            lb.end_connection(conn)

    time.sleep(0.5)
    stats = lb.get_stats()
    print(f"\nAfter removing 20 connections:")
    print(f"Total connections: {stats['summary']['total_connections']}")
    print(f"Stateful table: {stats['summary']['stateful_table_size']}/{stats['summary']['stateful_table_capacity']}")
    print(f"Bloom filter active: {stats['summary'].get('bloom_filter_active', False)}")

    # Final comprehensive stats
    print("\n\n=== Final Statistics ===")
    final_stats = lb.get_stats()
    print(
        f"Total routing decisions: {final_stats['summary']['routing_stats']['stateless_routes'] + final_stats['summary']['routing_stats']['stateful_routes']}")
    print(
        f"Stateless routes: {final_stats['summary']['routing_stats']['stateless_routes']} ({final_stats['summary']['routing_stats']['stateless_percentage']:.1f}%)")
    print(
        f"Stateful routes: {final_stats['summary']['routing_stats']['stateful_routes']} ({final_stats['summary']['routing_stats']['stateful_percentage']:.1f}%)")
    print(f"Bloom filter active: {final_stats['summary'].get('bloom_filter_active', False)}")

    print("\nServer statistics:")
    for name, data in final_stats['servers'].items():
        print(f"  {name}: Total served: {data['total_served']}, "
              f"Current: {data['connections']}, "
              f"Load Score: {data['load_score']:.2f}")

    # Stop monitoring
    lb.stop_monitoring()
    print("\nSimulation complete!")


if __name__ == "__main__":
    simulate_balancify()
