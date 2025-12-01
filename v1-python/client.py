import time
import random
import threading
import logging
import json
import requests
import uuid
import os
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import psutil
import multiprocessing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServiceType(Enum):
    """Different types of services that can be requested"""
    DNS = "dns"
    FILE_SHARING = "file_sharing"
    ONLINE_STORE = "online_store"
    EMAIL_SERVICE = "email_service"
    NEWS_WEBSITE = "news_website"
    CALLING_SERVICE = "calling_service"
    VIDEO_STREAMING = "video_streaming"


@dataclass
class RequestProfile:
    """Profile for different types of requests"""
    service_type: ServiceType
    cpu_load: float  # CPU usage percentage
    memory_load: float  # Memory usage in MB
    duration: float  # Request duration in seconds
    frequency: float  # Requests per second
    payload_size: int  # Request payload size in bytes


class RequestGenerator:
    """Generates realistic service requests with varying loads"""
    
    def __init__(self, load_balancer_url: str):
        self.load_balancer_url = load_balancer_url
        self.active_requests = {}
        self.request_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_response_time': 0.0,
            'service_stats': {}
        }
        
        # Initialize service profiles with realistic loads
        self.service_profiles = {
            ServiceType.DNS: RequestProfile(
                service_type=ServiceType.DNS,
                cpu_load=5.0,  # Low CPU
                memory_load=10.0,  # Low memory
                duration=0.1,  # Very fast
                frequency=100.0,  # High frequency
                payload_size=64  # Small payload
            ),
            ServiceType.FILE_SHARING: RequestProfile(
                service_type=ServiceType.FILE_SHARING,
                cpu_load=15.0,  # Medium CPU
                memory_load=50.0,  # Medium memory
                duration=2.0,  # Medium duration
                frequency=10.0,  # Medium frequency
                payload_size=1024  # Medium payload
            ),
            ServiceType.ONLINE_STORE: RequestProfile(
                service_type=ServiceType.ONLINE_STORE,
                cpu_load=25.0,  # High CPU
                memory_load=100.0,  # High memory
                duration=1.5,  # Medium duration
                frequency=20.0,  # Medium frequency
                payload_size=2048  # Large payload
            ),
            ServiceType.EMAIL_SERVICE: RequestProfile(
                service_type=ServiceType.EMAIL_SERVICE,
                cpu_load=20.0,  # Medium-high CPU
                memory_load=75.0,  # Medium-high memory
                duration=1.0,  # Medium duration
                frequency=15.0,  # Medium frequency
                payload_size=1536  # Medium-large payload
            ),
            ServiceType.NEWS_WEBSITE: RequestProfile(
                service_type=ServiceType.NEWS_WEBSITE,
                cpu_load=30.0,  # High CPU
                memory_load=150.0,  # High memory
                duration=0.8,  # Fast
                frequency=25.0,  # High frequency
                payload_size=3072  # Large payload
            ),
            ServiceType.CALLING_SERVICE: RequestProfile(
                service_type=ServiceType.CALLING_SERVICE,
                cpu_load=40.0,  # Very high CPU
                memory_load=200.0,  # Very high memory
                duration=5.0,  # Long duration
                frequency=5.0,  # Low frequency
                payload_size=4096  # Very large payload
            ),
            ServiceType.VIDEO_STREAMING: RequestProfile(
                service_type=ServiceType.VIDEO_STREAMING,
                cpu_load=50.0,  # Very high CPU
                memory_load=300.0,  # Very high memory
                duration=10.0,  # Very long duration
                frequency=2.0,  # Very low frequency
                payload_size=8192  # Very large payload
            )
        }
        
        # Initialize stats for each service
        for service_type in ServiceType:
            self.request_stats['service_stats'][service_type.value] = {
                'requests': 0,
                'successful': 0,
                'failed': 0,
                'total_response_time': 0.0,
                'avg_response_time': 0.0
            }
    
    def generate_request_payload(self, service_type: ServiceType) -> Dict:
        """Generate realistic payload for different service types"""
        profile = self.service_profiles[service_type]
        
        if service_type == ServiceType.DNS:
            return {
                'query': f"www.example{random.randint(1, 1000)}.com",
                'record_type': random.choice(['A', 'AAAA', 'MX', 'CNAME']),
                'client_ip': f"192.168.1.{random.randint(1, 255)}"
            }
        elif service_type == ServiceType.FILE_SHARING:
            return {
                'file_id': str(uuid.uuid4()),
                'operation': random.choice(['upload', 'download', 'delete']),
                'file_size': random.randint(1024, 10485760),  # 1KB to 10MB
                'user_id': f"user_{random.randint(1, 10000)}"
            }
        elif service_type == ServiceType.ONLINE_STORE:
            return {
                'product_id': random.randint(1, 10000),
                'action': random.choice(['view', 'add_to_cart', 'purchase']),
                'user_id': f"user_{random.randint(1, 10000)}",
                'session_id': str(uuid.uuid4()),
                'items': [{'id': random.randint(1, 100), 'quantity': random.randint(1, 5)} for _ in range(random.randint(1, 10))]
            }
        elif service_type == ServiceType.EMAIL_SERVICE:
            return {
                'action': random.choice(['send', 'receive', 'delete', 'mark_read']),
                'email_id': str(uuid.uuid4()),
                'sender': f"user{random.randint(1, 1000)}@example.com",
                'recipient': f"user{random.randint(1, 1000)}@example.com",
                'subject': f"Email subject {random.randint(1, 1000)}",
                'body_size': random.randint(100, 10000)
            }
        elif service_type == ServiceType.NEWS_WEBSITE:
            return {
                'article_id': random.randint(1, 10000),
                'category': random.choice(['politics', 'technology', 'sports', 'entertainment']),
                'user_id': f"user_{random.randint(1, 10000)}",
                'action': random.choice(['view', 'like', 'share', 'comment']),
                'content_length': random.randint(1000, 50000)
            }
        elif service_type == ServiceType.CALLING_SERVICE:
            return {
                'call_id': str(uuid.uuid4()),
                'caller_id': f"user_{random.randint(1, 10000)}",
                'callee_id': f"user_{random.randint(1, 10000)}",
                'call_type': random.choice(['audio', 'video']),
                'duration': random.randint(30, 3600),  # 30 seconds to 1 hour
                'quality': random.choice(['low', 'medium', 'high'])
            }
        elif service_type == ServiceType.VIDEO_STREAMING:
            return {
                'video_id': str(uuid.uuid4()),
                'user_id': f"user_{random.randint(1, 10000)}",
                'action': random.choice(['play', 'pause', 'seek', 'stop']),
                'resolution': random.choice(['480p', '720p', '1080p', '4K']),
                'bitrate': random.randint(1000, 8000),  # Kbps
                'duration': random.randint(60, 7200)  # 1 minute to 2 hours
            }
        
        return {'data': 'default_payload'}
    
    def simulate_load(self, profile: RequestProfile):
        """Simulate CPU and memory load for the request"""
        # Simulate CPU load by doing computational work
        start_time = time.time()
        while time.time() - start_time < profile.duration:
            # Perform some CPU-intensive work
            _ = sum(i * i for i in range(int(profile.cpu_load * 1000)))
            time.sleep(0.001)  # Small sleep to prevent blocking
    
    def send_request(self, service_type: ServiceType) -> Optional[Dict]:
        """Send a request to the load balancer"""
        request_id = str(uuid.uuid4())
        profile = self.service_profiles[service_type]
        
        # Generate payload
        payload = self.generate_request_payload(service_type)
        
        # Prepare request data
        request_data = {
            'request_id': request_id,
            'service_type': service_type.value,
            'protocol': 'HTTP',
            'source_ip': f"{random.choice(['192.168.1', '10.0.0', '172.16.0', '203.0.113'])}.{random.randint(1, 255)}",
            'source_port': random.randint(1024, 65535),
            'payload': payload,
            'cpu_load': profile.cpu_load,
            'memory_load': profile.memory_load,
            'duration': profile.duration
        }
        
        start_time = time.time()
        
        try:
            # Send request to load balancer
            response = requests.post(
                f"{self.load_balancer_url}/route",
                json=request_data,
                timeout=30
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                
                # Simulate the actual service processing
                self.simulate_load(profile)
                
                # Send request to the selected server
                server_url = result['server']['url']
                server_response = requests.post(
                    f"{server_url}/process",
                    json=request_data,
                    timeout=30
                )
                
                # End the connection
                requests.post(
                    f"{self.load_balancer_url}/end-connection",
                    json={'request_id': request_id}
                )
                
                # Update statistics
                self._update_stats(service_type, True, response_time)
                
                return {
                    'request_id': request_id,
                    'server': result['server'],
                    'response_time': response_time,
                    'status': 'success'
                }
            else:
                self._update_stats(service_type, False, response_time)
                logger.error(f"Load balancer error: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            response_time = time.time() - start_time
            self._update_stats(service_type, False, response_time)
            logger.error(f"Request failed: {e}")
            return None
    
    def _update_stats(self, service_type: ServiceType, success: bool, response_time: float):
        """Update request statistics"""
        self.request_stats['total_requests'] += 1
        self.request_stats['total_response_time'] += response_time
        
        service_stats = self.request_stats['service_stats'][service_type.value]
        service_stats['requests'] += 1
        service_stats['total_response_time'] += response_time
        
        if success:
            self.request_stats['successful_requests'] += 1
            service_stats['successful'] += 1
        else:
            self.request_stats['failed_requests'] += 1
            service_stats['failed'] += 1
        
        # Update average response time
        if service_stats['requests'] > 0:
            service_stats['avg_response_time'] = service_stats['total_response_time'] / service_stats['requests']
    
    def generate_traffic_pattern(self, duration: int = 60, pattern: str = 'normal'):
        """Generate different traffic patterns"""
        if pattern == 'normal':
            return self._normal_traffic(duration)
        elif pattern == 'burst':
            return self._burst_traffic(duration)
        elif pattern == 'gradual':
            return self._gradual_traffic(duration)
        elif pattern == 'mixed':
            return self._mixed_traffic(duration)
        else:
            return self._normal_traffic(duration)
    
    def _normal_traffic(self, duration: int):
        """Generate normal traffic pattern"""
        services = list(ServiceType)
        start_time = time.time()
        
        while time.time() - start_time < duration:
            # Randomly select service based on frequency
            service = random.choices(
                services,
                weights=[self.service_profiles[s].frequency for s in services]
            )[0]
            
            # Send request
            self.send_request(service)
            
            # Random delay between requests
            time.sleep(random.uniform(0.1, 1.0))
    
    def _burst_traffic(self, duration: int):
        """Generate burst traffic pattern"""
        services = list(ServiceType)
        start_time = time.time()
        
        while time.time() - start_time < duration:
            # Burst of requests
            burst_size = random.randint(30, 60)
            for _ in range(burst_size):
                service = random.choice(services)
                self.send_request(service)
            
            # Quiet period
            time.sleep(random.uniform(0.5, 1.5))
    
    def _gradual_traffic(self, duration: int):
        """Generate gradually increasing traffic"""
        services = list(ServiceType)
        start_time = time.time()
        phase = 0
        
        while time.time() - start_time < duration:
            # Gradually increase request frequency
            if phase == 0 and time.time() - start_time > duration * 0.25:
                phase = 1
            elif phase == 1 and time.time() - start_time > duration * 0.5:
                phase = 2
            elif phase == 2 and time.time() - start_time > duration * 0.75:
                phase = 3
            
            # Send requests based on phase
            requests_per_phase = [1, 3, 5, 2]  # Number of requests per iteration
            for _ in range(requests_per_phase[phase]):
                service = random.choice(services)
                self.send_request(service)
            
            time.sleep(random.uniform(0.5, 2.0))
    
    def _mixed_traffic(self, duration: int):
        """Generate mixed traffic pattern"""
        services = list(ServiceType)
        start_time = time.time()
        pattern_switch = 0
        
        while time.time() - start_time < duration:
            # Switch between patterns
            if time.time() - start_time > pattern_switch * (duration / 4):
                pattern_switch += 1
                current_pattern = pattern_switch % 4
            
            if current_pattern == 0:
                # Normal traffic
                service = random.choice(services)
                self.send_request(service)
                time.sleep(random.uniform(0.1, 1.0))
            elif current_pattern == 1:
                # Burst traffic
                for _ in range(random.randint(3, 10)):
                    service = random.choice(services)
                    self.send_request(service)
                time.sleep(random.uniform(1.0, 3.0))
            elif current_pattern == 2:
                # Heavy load traffic
                for _ in range(random.randint(5, 15)):
                    service = random.choice([ServiceType.VIDEO_STREAMING, ServiceType.CALLING_SERVICE])
                    self.send_request(service)
                time.sleep(random.uniform(0.5, 1.5))
            else:
                # Light load traffic
                for _ in range(random.randint(1, 3)):
                    service = random.choice([ServiceType.DNS, ServiceType.EMAIL_SERVICE])
                    self.send_request(service)
                time.sleep(random.uniform(1.0, 2.0))
    
    def get_stats(self) -> Dict:
        """Get request generation statistics"""
        total_time = self.request_stats['total_response_time']
        avg_response_time = total_time / self.request_stats['total_requests'] if self.request_stats['total_requests'] > 0 else 0
        
        return {
            'summary': {
                'total_requests': self.request_stats['total_requests'],
                'successful_requests': self.request_stats['successful_requests'],
                'failed_requests': self.request_stats['failed_requests'],
                'success_rate': (self.request_stats['successful_requests'] / self.request_stats['total_requests'] * 100) if self.request_stats['total_requests'] > 0 else 0,
                'avg_response_time': avg_response_time,
                'total_response_time': total_time
            },
            'service_stats': self.request_stats['service_stats']
        }


def main():
    """Main function to run the client"""
    # Get configuration from environment
    load_balancer_url = os.getenv('LOAD_BALANCER_URL', 'http://load-balancer:8080')
    duration = int(os.getenv('TEST_DURATION', '300'))  # 5 minutes default
    pattern = os.getenv('TRAFFIC_PATTERN', 'normal')
    
    logger.info(f"Starting client with pattern: {pattern}, duration: {duration}s")
    logger.info(f"Load balancer URL: {load_balancer_url}")
    
    # Create request generator
    generator = RequestGenerator(load_balancer_url)
    
    try:
        # Generate traffic
        generator.generate_traffic_pattern(duration, pattern)
        
        # Print final statistics
        stats = generator.get_stats()
        logger.info("=== Final Statistics ===")
        logger.info(f"Total requests: {stats['summary']['total_requests']}")
        logger.info(f"Successful requests: {stats['summary']['successful_requests']}")
        logger.info(f"Failed requests: {stats['summary']['failed_requests']}")
        logger.info(f"Success rate: {stats['summary']['success_rate']:.2f}%")
        logger.info(f"Average response time: {stats['summary']['avg_response_time']:.3f}s")
        
        logger.info("\n=== Service Statistics ===")
        for service, service_stats in stats['service_stats'].items():
            logger.info(f"{service}: {service_stats['requests']} requests, "
                       f"{service_stats['successful']} successful, "
                       f"avg response time: {service_stats['avg_response_time']:.3f}s")
        
    except KeyboardInterrupt:
        logger.info("Client stopped by user")
    except Exception as e:
        logger.error(f"Client error: {e}")


if __name__ == '__main__':
    main()
