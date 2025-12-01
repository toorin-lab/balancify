import time
import logging
import json
import requests
import os
import psutil
import threading
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ServerMetrics:
    """Server metrics data structure"""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_available: float  # MB
    memory_used: float  # MB
    memory_total: float  # MB
    disk_usage_percent: float
    network_io: Dict[str, float]  # bytes sent/received
    load_average: Optional[float] = None


class ServerMonitor:
    """Monitors server health and reports to load balancer"""
    
    def __init__(self, server_name: str, load_balancer_url: str, monitor_interval: int = 10):
        self.server_name = server_name
        self.load_balancer_url = load_balancer_url
        self.monitor_interval = monitor_interval
        
        # Monitoring state
        self.monitoring_active = True
        self.metrics_history = []
        self.max_history_size = 1000
        
        # Performance thresholds
        self.cpu_threshold = float(os.getenv('CPU_THRESHOLD', '80.0'))
        self.memory_threshold = float(os.getenv('MEMORY_THRESHOLD', '85.0'))
        self.disk_threshold = float(os.getenv('DISK_THRESHOLD', '90.0'))
        
        # Health status
        self.health_status = True
        self.last_health_check = time.time()
        self.consecutive_failures = 0
        self.max_consecutive_failures = int(os.getenv('MAX_CONSECUTIVE_FAILURES', '3'))
        
        # Network monitoring
        self.last_network_io = psutil.net_io_counters()
        self.last_network_time = time.time()
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info(f"Server monitor initialized for {server_name}")
        logger.info(f"Load balancer URL: {load_balancer_url}")
        logger.info(f"Monitor interval: {monitor_interval}s")
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.monitoring_active:
            try:
                # Collect metrics
                metrics = self._collect_metrics()
                
                # Store in history
                self.metrics_history.append(metrics)
                if len(self.metrics_history) > self.max_history_size:
                    self.metrics_history.pop(0)
                
                # Check health status
                self._check_health_status(metrics)
                
                # Report to load balancer
                self._report_to_load_balancer(metrics)
                
                # Wait for next interval
                time.sleep(self.monitor_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                self.consecutive_failures += 1
                
                # If too many consecutive failures, mark as unhealthy
                if self.consecutive_failures >= self.max_consecutive_failures:
                    self.health_status = False
                    logger.warning(f"Server marked as unhealthy due to {self.consecutive_failures} consecutive failures")
                
                time.sleep(self.monitor_interval)
    
    def _collect_metrics(self) -> ServerMetrics:
        """Collect current server metrics"""
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1.0)
        
        # Memory usage
        memory = psutil.virtual_memory()
        
        # Disk usage
        disk_usage = psutil.disk_usage('/')
        
        # Network I/O
        current_network_io = psutil.net_io_counters()
        current_time = time.time()
        
        # Calculate network I/O rates
        time_diff = current_time - self.last_network_time
        bytes_sent_rate = (current_network_io.bytes_sent - self.last_network_io.bytes_sent) / time_diff if time_diff > 0 else 0
        bytes_recv_rate = (current_network_io.bytes_recv - self.last_network_io.bytes_recv) / time_diff if time_diff > 0 else 0
        
        # Update last values
        self.last_network_io = current_network_io
        self.last_network_time = current_time
        
        # Load average (Unix-like systems only)
        load_average = None
        try:
            load_average = psutil.getloadavg()[0]  # 1-minute load average
        except AttributeError:
            # Windows doesn't have load average
            pass
        
        return ServerMetrics(
            timestamp=current_time,
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_available=memory.available / (1024 * 1024),  # MB
            memory_used=memory.used / (1024 * 1024),  # MB
            memory_total=memory.total / (1024 * 1024),  # MB
            disk_usage_percent=disk_usage.percent,
            network_io={
                'bytes_sent_rate': bytes_sent_rate,
                'bytes_recv_rate': bytes_recv_rate,
                'bytes_sent_total': current_network_io.bytes_sent,
                'bytes_recv_total': current_network_io.bytes_recv
            },
            load_average=load_average
        )
    
    def _check_health_status(self, metrics: ServerMetrics):
        """Check if server is healthy based on metrics"""
        previous_status = self.health_status
        
        # Check thresholds
        cpu_healthy = metrics.cpu_percent < self.cpu_threshold
        memory_healthy = metrics.memory_percent < self.memory_threshold
        disk_healthy = metrics.disk_usage_percent < self.disk_threshold
        
        # Server is healthy if all metrics are within thresholds
        self.health_status = cpu_healthy and memory_healthy and disk_healthy
        
        # Log status changes
        if self.health_status != previous_status:
            if self.health_status:
                logger.info(f"Server {self.server_name} is now healthy")
                self.consecutive_failures = 0
            else:
                logger.warning(f"Server {self.server_name} is now unhealthy")
                logger.warning(f"CPU: {metrics.cpu_percent:.1f}% (threshold: {self.cpu_threshold}%)")
                logger.warning(f"Memory: {metrics.memory_percent:.1f}% (threshold: {self.memory_threshold}%)")
                logger.warning(f"Disk: {metrics.disk_usage_percent:.1f}% (threshold: {self.disk_threshold}%)")
        
        self.last_health_check = time.time()
    
    def _report_to_load_balancer(self, metrics: ServerMetrics):
        """Report metrics to load balancer"""
        try:
            # Prepare metrics data
            metrics_data = {
                'server_name': self.server_name,
                'timestamp': metrics.timestamp,
                'health_status': self.health_status,
                'cpu_utilization': metrics.cpu_percent,
                'ram_utilization': metrics.memory_used,  # MB
                'memory_percent': metrics.memory_percent,
                'disk_usage_percent': metrics.disk_usage_percent,
                'network_io': metrics.network_io,
                'load_average': metrics.load_average,
                'memory_available': metrics.memory_available,
                'memory_total': metrics.memory_total
            }
            
            # Send to load balancer
            response = requests.post(
                f"{self.load_balancer_url}/update-server-metrics",
                json=metrics_data,
                timeout=10
            )
            
            if response.status_code == 200:
                self.consecutive_failures = 0
                logger.debug(f"Successfully reported metrics to load balancer")
            else:
                logger.warning(f"Failed to report metrics: {response.status_code} - {response.text}")
                self.consecutive_failures += 1
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error reporting to load balancer: {e}")
            self.consecutive_failures += 1
    
    def get_current_metrics(self) -> Optional[ServerMetrics]:
        """Get the most recent metrics"""
        if self.metrics_history:
            return self.metrics_history[-1]
        return None
    
    def get_metrics_summary(self) -> Dict:
        """Get a summary of recent metrics"""
        if not self.metrics_history:
            return {'error': 'No metrics available'}
        
        recent_metrics = self.metrics_history[-10:]  # Last 10 measurements
        
        cpu_values = [m.cpu_percent for m in recent_metrics]
        memory_values = [m.memory_percent for m in recent_metrics]
        
        return {
            'server_name': self.server_name,
            'health_status': self.health_status,
            'last_check': self.last_health_check,
            'consecutive_failures': self.consecutive_failures,
            'current_metrics': {
                'cpu_percent': recent_metrics[-1].cpu_percent if recent_metrics else 0,
                'memory_percent': recent_metrics[-1].memory_percent if recent_metrics else 0,
                'memory_used_mb': recent_metrics[-1].memory_used if recent_metrics else 0,
                'disk_usage_percent': recent_metrics[-1].disk_usage_percent if recent_metrics else 0
            },
            'averages': {
                'cpu_percent': sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                'memory_percent': sum(memory_values) / len(memory_values) if memory_values else 0
            },
            'thresholds': {
                'cpu_threshold': self.cpu_threshold,
                'memory_threshold': self.memory_threshold,
                'disk_threshold': self.disk_threshold
            },
            'total_measurements': len(self.metrics_history)
        }
    
    def stop_monitoring(self):
        """Stop the monitoring process"""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join()
        logger.info(f"Server monitor stopped for {self.server_name}")


def main():
    """Main function to run the server monitor"""
    # Get configuration from environment
    server_name = os.getenv('SERVER_NAME', 'server-1')
    load_balancer_url = os.getenv('LOAD_BALANCER_URL', 'http://load-balancer:8080')
    monitor_interval = int(os.getenv('MONITOR_INTERVAL', '10'))
    
    logger.info(f"Starting server monitor for {server_name}")
    logger.info(f"Load balancer URL: {load_balancer_url}")
    logger.info(f"Monitor interval: {monitor_interval}s")
    
    # Create and start monitor
    monitor = ServerMonitor(server_name, load_balancer_url, monitor_interval)
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(60)  # Check every minute
            
            # Print summary every 5 minutes
            if int(time.time()) % 300 == 0:  # Every 5 minutes
                summary = monitor.get_metrics_summary()
                logger.info(f"=== Monitor Summary for {server_name} ===")
                logger.info(f"Health Status: {'Healthy' if summary['health_status'] else 'Unhealthy'}")
                logger.info(f"CPU: {summary['current_metrics']['cpu_percent']:.1f}% (avg: {summary['averages']['cpu_percent']:.1f}%)")
                logger.info(f"Memory: {summary['current_metrics']['memory_percent']:.1f}% (avg: {summary['averages']['memory_percent']:.1f}%)")
                logger.info(f"Disk: {summary['current_metrics']['disk_usage_percent']:.1f}%")
                logger.info(f"Consecutive Failures: {summary['consecutive_failures']}")
                logger.info(f"Total Measurements: {summary['total_measurements']}")
                
    except KeyboardInterrupt:
        logger.info("Server monitor stopped by user")
    except Exception as e:
        logger.error(f"Server monitor error: {e}")
    finally:
        monitor.stop_monitoring()


if __name__ == '__main__':
    main()
