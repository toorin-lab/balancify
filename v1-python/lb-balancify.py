import hashlib
import threading
import time
import random
import logging
import json
import socket
import requests
import math
from collections import deque
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum
from flask import Flask, request, jsonify
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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
    def __init__(self, name, capacity_threshold, host, port):
        self.name = name
        self.capacity_threshold = capacity_threshold
        self.host = host
        self.port = port
        self.current_load = 0
        self.connections = []
        self.cpu_utilization = 0.0
        self.ram_utilization = 0.0
        self.health_status = True
        self.total_connections_served = 0
        self.connection_history = deque(maxlen=1000)
        self.last_health_check = time.time()

    def add_connection(self, connection):
        self.current_load += 1
        self.connections.append(connection)
        self.total_connections_served += 1
        self.connection_history.append((time.time(), connection.id))

    def remove_connection(self, connection):
        if self.current_load > 0:
            self.current_load -= 1
            if connection in self.connections:
                self.connections.remove(connection)

    def update_metrics(self, cpu_utilization, ram_utilization):
        """Update server metrics from health monitor"""
        self.cpu_utilization = cpu_utilization
        self.ram_utilization = ram_utilization
        self.last_health_check = time.time()

    def get_load_score(self) -> float:
        """Calculate a combined load score for the server"""
        cpu_weight = 0.4
        ram_weight = 0.3
        connection_weight = 0.3

        normalized_connections = self.current_load / max(1, self.capacity_threshold)
        normalized_cpu = self.cpu_utilization / 100.0
        normalized_ram = min(1.0, self.ram_utilization / 16.0)

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

        for i, server in enumerate(self.servers):
            # Use server host and port for better hash distribution
            server_key = f"{server.host}:{server.port}:{i}"
            offset = self._hash(server_key, 0) % self.table_size
            skip = self._hash(server_key, 1) % (self.table_size - 1) + 1
            perm = []
            for j in range(self.table_size):
                perm.append((offset + j * skip) % self.table_size)
            permutation.append(perm)

        next_indices = [0] * n
        filled = 0
        
        while filled < self.table_size:
            for j in range(n):
                if filled >= self.table_size:
                    break
                    
                c = permutation[j][next_indices[j]]
                while self.lookup_table[c] is not None:
                    next_indices[j] += 1
                    c = permutation[j][next_indices[j]]

                self.lookup_table[c] = j
                next_indices[j] += 1
                filled += 1

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
        self.connection_table = {}  # Stateful connection mapping
        self.connection_stats = {}  # Connection statistics

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
        self.bloom_threshold = int(os.getenv('BLOOM_THRESHOLD', str(max_stateful_entries // 2 or 1)))
        self.bloom_error_rate = float(os.getenv('BLOOM_ERROR_RATE', '0.01'))
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
        """Update average CPU and RAM utilization across all servers by querying DIPs"""
        total_cpu = 0.0
        total_ram = 0.0
        active_servers = 0

        # Query all DIPs via agent endpoint (Algorithm 1)
        for server in self.servers:
            try:
                # Query server agent for CPU utilization
                server_url = f"http://{server.host}:{server.port}"
                response = requests.get(f"{server_url}/metrics", timeout=2)
                
                if response.status_code == 200:
                    metrics = response.json()
                    cpu_load = metrics.get('cpu_percent', 0.0)
                    ram_load = metrics.get('memory_used', 0.0)  # MB
                    
                    # Update server metrics
                    server.update_metrics(cpu_load, ram_load)
                    server.health_status = True
                    
                    self.server_loads[server.name] = {
                        'cpu': cpu_load,
                        'ram': ram_load,
                        'connections': server.current_load,
                        'score': server.get_load_score()
                    }
                    total_cpu += cpu_load
                    total_ram += ram_load
                    active_servers += 1
                else:
                    logger.warning(f"Failed to query {server.name}: HTTP {response.status_code}")
                    server.health_status = False
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error querying {server.name} at {server.host}:{server.port}: {e}")
                server.health_status = False
            except Exception as e:
                logger.error(f"Unexpected error querying {server.name}: {e}")
                server.health_status = False

        if active_servers > 0:
            self.avg_cpu_load = total_cpu / active_servers
            self.avg_ram_load = total_ram / active_servers

    def _cleanup_stale_connections(self):
        """Remove stale entries from connection table"""
        current_time = time.time()
        stale_timeout = 300  # 5 minutes

        stale_connections = []
        for conn_id, server in list(self.connection_table.items()):
            if conn_id in self.connection_stats:
                last_seen = self.connection_stats[conn_id].timestamp
                if current_time - last_seen > stale_timeout:
                    stale_connections.append(conn_id)

        for conn_id in stale_connections:
            del self.connection_table[conn_id]
            del self.connection_stats[conn_id]
            logger.debug(f"Removed stale connection {conn_id}")

            self._on_stateful_entry_removed(conn_id)

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
    
    def _get_load(self, server_index: int) -> float:
        """GetLoad function for Algorithm 1 - queries server for CPU utilization"""
        if server_index < 0 or server_index >= len(self.servers):
            return 0.0
        
        server = self.servers[server_index]
        try:
            server_url = f"http://{server.host}:{server.port}"
            response = requests.get(f"{server_url}/metrics", timeout=2)
            if response.status_code == 200:
                metrics = response.json()
                return metrics.get('cpu_percent', 0.0)
        except Exception as e:
            logger.warning(f"Error getting load from server {server.name}: {e}")
        return 0.0

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

    def _should_use_stateful_routing(self, server: Server) -> bool:
        """Determine if stateful routing should be used based on CPU threshold (Algorithm 2)"""
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

        # Check stateful table first using 5-tuple directly (not hash)
        conn_5_tuple_key = str(conn_5_tuple)
        server = self._get_stateful_server(conn_5_tuple_key)
        if server:
            if server.health_status:
                self.stateful_routes += 1
                self.connection_stats[connection.id].mode = RoutingMode.STATEFUL
                logger.debug(f"[STATEFUL] Connection {connection.id} -> {server.name}")
                return server
            else:
                del self.connection_table[conn_5_tuple_key]
                self._on_stateful_entry_removed(conn_5_tuple_key)

        # Use Maglev consistent hashing for new connections
        selected_server = self.maglev_table.get_server(conn_hash)

        if not selected_server:
            selected_server = self._find_least_loaded_server()
            if not selected_server:
                raise RuntimeError("No healthy servers available")

        # Check if we should add to stateful table
        if self._should_use_stateful_routing(selected_server):
            # Find a better server if current one is overloaded (conservative approach)
            alternative = self._find_least_loaded_server()

            if (alternative and alternative != selected_server and
                    alternative.cpu_utilization < selected_server.cpu_utilization * 0.7):

                if len(self.connection_table) < self.max_stateful_entries:
                    self.connection_table[conn_5_tuple_key] = alternative
                    self._on_stateful_entry_added(conn_5_tuple_key)
                    self.stateful_routes += 1
                    self.connection_stats[connection.id].mode = RoutingMode.STATEFUL
                    selected_server = alternative
                    logger.info(f"[STATEFUL] Connection {connection.id} -> {selected_server.name}")
                else:
                    self._evict_oldest_stateful_entry()
                    self.connection_table[conn_5_tuple_key] = alternative
                    self._on_stateful_entry_added(conn_5_tuple_key)
                    selected_server = alternative
                    self.stateful_routes += 1
                    self.connection_stats[connection.id].mode = RoutingMode.STATEFUL
                    logger.info(f"[STATEFUL] Connection {connection.id} -> {selected_server.name}")
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
        """Find the least loaded healthy server (by CPU utilization)"""
        healthy_servers = [s for s in self.servers if s.health_status]

        if not healthy_servers:
            return None

        # Use CPU utilization as the primary metric (as per paper description)
        return min(healthy_servers, key=lambda s: s.cpu_utilization)

    def _evict_oldest_stateful_entry(self):
        """Evict the oldest entry from stateful table"""
        if not self.connection_table:
            return

        oldest_conn_id = None
        oldest_time = float('inf')

        for conn_id in self.connection_table:
            if conn_id in self.connection_stats:
                timestamp = self.connection_stats[conn_id].timestamp
                if timestamp < oldest_time:
                    oldest_time = timestamp
                    oldest_conn_id = conn_id

        if oldest_conn_id:
            del self.connection_table[oldest_conn_id]
            logger.debug(f"Evicted oldest connection {oldest_conn_id} from stateful table")
            self._on_stateful_entry_removed(oldest_conn_id)

    def end_connection(self, connection: Connection):
        """End a connection and clean up resources"""
        conn_id = connection.id

        server = None
        conn_5_tuple = connection.get_5_tuple()
        conn_5_tuple_key = str(conn_5_tuple)
        conn_hash = connection.get_5_tuple_hash()

        if conn_5_tuple_key in self.connection_table:
            server = self.connection_table.pop(conn_5_tuple_key)
            logger.debug(f"[END] Connection {conn_id} removed from stateful table")
            self._on_stateful_entry_removed(conn_5_tuple_key)
        else:
            server = self.maglev_table.get_server(conn_hash)

        if server:
            server.remove_connection(connection)

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
                'total_served': server.total_connections_served,
                'host': server.host,
                'port': server.port
            }

        return stats

    def set_server_health(self, server_name: str, health_status: bool):
        """Set health status of a server"""
        for server in self.servers:
            if server.name == server_name:
                server.health_status = health_status
                logger.info(f"Server {server_name} health status set to {health_status}")

                healthy_servers = [s for s in self.servers if s.health_status]
                self.maglev_table = MaglevHashTable(healthy_servers)
                break

    def update_server_metrics(self, server_name: str, cpu_utilization: float, ram_utilization: float):
        """Update server metrics from health monitor"""
        for server in self.servers:
            if server.name == server_name:
                server.update_metrics(cpu_utilization, ram_utilization)
                break

    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join()


# Global load balancer instance
load_balancer = None


def initialize_load_balancer():
    """Initialize the load balancer with server configuration"""
    global load_balancer
    
    # Get server configuration from environment
    server_count = int(os.getenv('SERVER_COUNT', '3'))
    servers = []
    
    for i in range(server_count):
        server_name = f"server-{i+1}"
        host = os.getenv(f'SERVER_{i+1}_HOST', f'server-{i+1}')
        port = int(os.getenv(f'SERVER_{i+1}_PORT', '5000'))
        capacity = int(os.getenv(f'SERVER_{i+1}_CAPACITY', '100'))
        
        server = Server(server_name, capacity, host, port)
        servers.append(server)
    
    load_balancer = BalancifyLoadBalancer(
        servers=servers,
        cpu_threshold=float(os.getenv('CPU_THRESHOLD', '20.0')),
        ram_threshold=float(os.getenv('RAM_THRESHOLD', '2.0')),
        max_stateful_entries=int(os.getenv('MAX_STATEFUL_ENTRIES', '1000')),
        monitoring_interval=int(os.getenv('MONITORING_INTERVAL', '10'))
    )
    
    logger.info(f"Load balancer initialized with {len(servers)} servers")


@app.route('/route', methods=['POST'])
def route_request():
    """Route a request to an appropriate server"""
    try:
        data = request.get_json()
        
        if not data or 'request_id' not in data:
            return jsonify({'error': 'Missing request_id'}), 400
        
        request_id = data['request_id']
        protocol = data.get('protocol', 'HTTP')
        source_ip = data.get('source_ip')
        source_port = data.get('source_port')
        
        # Create connection object
        connection = Connection(
            id=request_id,
            protocol=protocol,
            source_ip=source_ip,
            source_port=source_port
        )
        
        # Route the connection
        selected_server = load_balancer.route_connection(connection)
        
        return jsonify({
            'request_id': request_id,
            'server': {
                'name': selected_server.name,
                'host': selected_server.host,
                'port': selected_server.port,
                'url': f"http://{selected_server.host}:{selected_server.port}"
            }
        })
        
    except Exception as e:
        logger.error(f"Error routing request: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/end-connection', methods=['POST'])
def end_connection():
    """End a connection"""
    try:
        data = request.get_json()
        
        if not data or 'request_id' not in data:
            return jsonify({'error': 'Missing request_id'}), 400
        
        request_id = data['request_id']
        protocol = data.get('protocol', 'HTTP')
        source_ip = data.get('source_ip')
        source_port = data.get('source_port')
        
        # Create connection object
        connection = Connection(
            id=request_id,
            protocol=protocol,
            source_ip=source_ip,
            source_port=source_port
        )
        
        # End the connection
        load_balancer.end_connection(connection)
        
        return jsonify({'status': 'success', 'request_id': request_id})
        
    except Exception as e:
        logger.error(f"Error ending connection: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/stats', methods=['GET'])
def get_stats():
    """Get load balancer statistics"""
    try:
        stats = load_balancer.get_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/update-server-metrics', methods=['POST'])
def update_server_metrics():
    """Update server metrics from health monitor"""
    try:
        data = request.get_json()
        
        if not data or 'server_name' not in data:
            return jsonify({'error': 'Missing server_name'}), 400
        
        server_name = data['server_name']
        cpu_utilization = data.get('cpu_utilization', 0.0)
        ram_utilization = data.get('ram_utilization', 0.0)
        
        load_balancer.update_server_metrics(server_name, cpu_utilization, ram_utilization)
        
        return jsonify({'status': 'success'})
        
    except Exception as e:
        logger.error(f"Error updating server metrics: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': time.time()})


if __name__ == '__main__':
    initialize_load_balancer()
    
    port = int(os.getenv('LB_PORT', '8080'))
    debug = os.getenv('DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting load balancer on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
