import hashlib
import threading
import time
import random
import logging
import json
import socket
import requests
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


class StatelessLoadBalancer:
    def __init__(self, servers, monitoring_interval=10):
        self.servers = servers
        self.monitoring_interval = monitoring_interval

        # Connection tracking
        self.connection_stats = {}  # Connection statistics

        # Maglev hash table
        self.maglev_table = MaglevHashTable(servers)

        # Load metrics
        self.avg_cpu_load = 0.0
        self.avg_ram_load = 0.0
        self.server_loads = {}

        # Routing statistics
        self.stateless_routes = 0

        # Monitoring thread
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitoring_thread, daemon=True)
        self.monitor_thread.start()

        # Initial metrics update
        self._update_load_metrics()

        logger.info(f"Stateless Load Balancer initialized with {len(servers)} servers")

    def _monitoring_thread(self):
        """Monitoring thread that updates metrics and performs maintenance"""
        while self.monitoring_active:
            self._update_load_metrics()
            time.sleep(self.monitoring_interval)

    def _update_load_metrics(self):
        """Update average CPU and RAM utilization across all servers"""
        total_cpu = 0.0
        total_ram = 0.0
        active_servers = 0

        for server in self.servers:
            if server.health_status:
                cpu_load = server.cpu_utilization
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

    def route_connection(self, connection: Connection) -> Server:
        """Route a connection using stateless approach only"""
        conn_hash = connection.get_5_tuple_hash()

        # Update connection stats
        self.connection_stats[connection.id] = ConnectionStats(
            timestamp=time.time(),
            server_id=None,
            mode=RoutingMode.STATELESS
        )

        # Use Maglev consistent hashing for all connections
        selected_server = self.maglev_table.get_server(conn_hash)

        if not selected_server:
            selected_server = self._find_least_loaded_server()
            if not selected_server:
                raise RuntimeError("No healthy servers available")

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

    def end_connection(self, connection: Connection):
        """End a connection and clean up resources"""
        conn_id = connection.id

        # Find server using hash
        conn_hash = connection.get_5_tuple_hash()
        server = self.maglev_table.get_server(conn_hash)

        if server:
            server.remove_connection(connection)

        if conn_id in self.connection_stats:
            del self.connection_stats[conn_id]

    def get_stats(self) -> Dict:
        """Get comprehensive load balancer statistics"""
        total_connections = sum(s.current_load for s in self.servers)
        healthy_servers = sum(1 for s in self.servers if s.health_status)

        routing_total = self.stateless_routes
        stateless_percentage = 100.0  # Always 100% for stateless

        stats = {
            'summary': {
                'total_servers': len(self.servers),
                'healthy_servers': healthy_servers,
                'total_connections': total_connections,
                'avg_cpu': self.avg_cpu_load,
                'avg_ram': self.avg_ram_load,
                'stateful_table_size': 0,  # No stateful table
                'stateful_table_capacity': 0,
                'routing_stats': {
                    'stateless_routes': self.stateless_routes,
                    'stateful_routes': 0,
                    'stateless_percentage': stateless_percentage,
                    'stateful_percentage': 0.0,
                    'routing_changes': 0
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
    
    load_balancer = StatelessLoadBalancer(
        servers=servers,
        monitoring_interval=int(os.getenv('MONITORING_INTERVAL', '10'))
    )
    
    logger.info(f"Stateless load balancer initialized with {len(servers)} servers")


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
    
    logger.info(f"Starting stateless load balancer on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
