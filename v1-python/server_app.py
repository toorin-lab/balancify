import time
import random
import logging
import json
import threading
import os
import psutil
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
from flask import Flask, request, jsonify
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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
class ServiceHandler:
    """Handler for different service types"""
    service_type: ServiceType
    cpu_intensity: float  # CPU usage multiplier
    memory_intensity: float  # Memory usage multiplier
    processing_time: float  # Base processing time in seconds


class ServerApplication:
    """Server application that handles different service requests"""
    
    def __init__(self, server_name: str):
        self.server_name = server_name
        self.active_requests = {}
        self.request_history = []
        self.service_handlers = {
            ServiceType.DNS: ServiceHandler(
                service_type=ServiceType.DNS,
                cpu_intensity=0.1,  # Low CPU
                memory_intensity=0.05,  # Low memory
                processing_time=0.05  # Very fast
            ),
            ServiceType.FILE_SHARING: ServiceHandler(
                service_type=ServiceType.FILE_SHARING,
                cpu_intensity=0.3,  # Medium CPU
                memory_intensity=0.2,  # Medium memory
                processing_time=1.0  # Medium duration
            ),
            ServiceType.ONLINE_STORE: ServiceHandler(
                service_type=ServiceType.ONLINE_STORE,
                cpu_intensity=0.5,  # High CPU
                memory_intensity=0.4,  # High memory
                processing_time=0.8  # Medium duration
            ),
            ServiceType.EMAIL_SERVICE: ServiceHandler(
                service_type=ServiceType.EMAIL_SERVICE,
                cpu_intensity=0.4,  # Medium-high CPU
                memory_intensity=0.3,  # Medium-high memory
                processing_time=0.6  # Medium duration
            ),
            ServiceType.NEWS_WEBSITE: ServiceHandler(
                service_type=ServiceType.NEWS_WEBSITE,
                cpu_intensity=0.6,  # High CPU
                memory_intensity=0.5,  # High memory
                processing_time=0.4  # Fast
            ),
            ServiceType.CALLING_SERVICE: ServiceHandler(
                service_type=ServiceType.CALLING_SERVICE,
                cpu_intensity=0.8,  # Very high CPU
                memory_intensity=0.7,  # Very high memory
                processing_time=2.0  # Long duration
            ),
            ServiceType.VIDEO_STREAMING: ServiceHandler(
                service_type=ServiceType.VIDEO_STREAMING,
                cpu_intensity=1.0,  # Maximum CPU
                memory_intensity=0.9,  # Maximum memory
                processing_time=3.0  # Very long duration
            )
        }
        
        # Performance metrics
        self.metrics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_processing_time': 0.0,
            'avg_processing_time': 0.0,
            'service_stats': {}
        }
        
        # Initialize service statistics
        for service_type in ServiceType:
            self.metrics['service_stats'][service_type.value] = {
                'requests': 0,
                'successful': 0,
                'failed': 0,
                'total_processing_time': 0.0,
                'avg_processing_time': 0.0,
                'total_cpu_usage': 0.0,
                'total_memory_usage': 0.0
            }
    
    def process_request(self, request_data: Dict) -> Dict:
        """Process a service request"""
        request_id = request_data.get('request_id')
        service_type_str = request_data.get('service_type')
        
        if not service_type_str:
            raise ValueError("Missing service_type in request")
        
        try:
            service_type = ServiceType(service_type_str)
        except ValueError:
            raise ValueError(f"Invalid service type: {service_type_str}")
        
        handler = self.service_handlers[service_type]
        
        # Record start time
        start_time = time.time()
        
        # Simulate processing with realistic resource usage
        self._simulate_processing(handler, request_data)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Generate response
        response = self._generate_response(service_type, request_data, processing_time)
        
        # Update metrics
        self._update_metrics(service_type, True, processing_time, handler)
        
        # Add to history
        self.request_history.append({
            'request_id': request_id,
            'service_type': service_type.value,
            'processing_time': processing_time,
            'timestamp': time.time(),
            'status': 'success'
        })
        
        return response
    
    def _simulate_processing(self, handler: ServiceHandler, request_data: Dict):
        """Simulate realistic processing for different service types"""
        # Get current system resources
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        
        # Calculate processing time based on handler and current load
        base_time = handler.processing_time
        load_factor = 1.0 + (cpu_percent / 100.0) * 0.5  # Increase time if CPU is busy
        processing_time = base_time * load_factor
        
        # Simulate CPU-intensive work
        start_time = time.time()
        while time.time() - start_time < processing_time:
            # Perform CPU-intensive operations based on service type
            if handler.service_type == ServiceType.DNS:
                # DNS resolution simulation
                _ = hash(str(request_data.get('payload', {}).get('query', '')))
            elif handler.service_type == ServiceType.FILE_SHARING:
                # File operation simulation
                file_size = request_data.get('payload', {}).get('file_size', 1024)
                _ = sum(i * i for i in range(min(1000, file_size // 100)))
            elif handler.service_type == ServiceType.ONLINE_STORE:
                # E-commerce processing simulation
                items = request_data.get('payload', {}).get('items', [])
                _ = sum(item.get('quantity', 1) * item.get('id', 1) for item in items)
            elif handler.service_type == ServiceType.EMAIL_SERVICE:
                # Email processing simulation
                body_size = request_data.get('payload', {}).get('body_size', 100)
                _ = sum(ord(c) for c in str(body_size) * 100)
            elif handler.service_type == ServiceType.NEWS_WEBSITE:
                # Content processing simulation
                content_length = request_data.get('payload', {}).get('content_length', 1000)
                _ = sum(i * i for i in range(min(1000, content_length // 10)))
            elif handler.service_type == ServiceType.CALLING_SERVICE:
                # Call processing simulation
                duration = request_data.get('payload', {}).get('duration', 60)
                _ = sum(i * i * i for i in range(min(500, duration // 10)))
            elif handler.service_type == ServiceType.VIDEO_STREAMING:
                # Video processing simulation
                bitrate = request_data.get('payload', {}).get('bitrate', 1000)
                _ = sum(i * i * i * i for i in range(min(200, bitrate // 50)))
            
            # Small sleep to prevent blocking
            time.sleep(0.001)
    
    def _generate_response(self, service_type: ServiceType, request_data: Dict, processing_time: float) -> Dict:
        """Generate appropriate response for different service types"""
        payload = request_data.get('payload', {})
        
        if service_type == ServiceType.DNS:
            return {
                'status': 'success',
                'service_type': 'dns',
                'response': {
                    'query': payload.get('query', ''),
                    'record_type': payload.get('record_type', 'A'),
                    'result': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    'ttl': random.randint(300, 3600)
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        elif service_type == ServiceType.FILE_SHARING:
            return {
                'status': 'success',
                'service_type': 'file_sharing',
                'response': {
                    'file_id': payload.get('file_id', ''),
                    'operation': payload.get('operation', ''),
                    'file_size': payload.get('file_size', 0),
                    'status': 'completed',
                    'url': f"https://files.example.com/{payload.get('file_id', '')}"
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        elif service_type == ServiceType.ONLINE_STORE:
            return {
                'status': 'success',
                'service_type': 'online_store',
                'response': {
                    'product_id': payload.get('product_id', 0),
                    'action': payload.get('action', ''),
                    'user_id': payload.get('user_id', ''),
                    'session_id': payload.get('session_id', ''),
                    'items': payload.get('items', []),
                    'total_price': sum(item.get('quantity', 1) * random.uniform(10, 100) for item in payload.get('items', [])),
                    'order_id': str(uuid.uuid4())
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        elif service_type == ServiceType.EMAIL_SERVICE:
            return {
                'status': 'success',
                'service_type': 'email_service',
                'response': {
                    'action': payload.get('action', ''),
                    'email_id': payload.get('email_id', ''),
                    'sender': payload.get('sender', ''),
                    'recipient': payload.get('recipient', ''),
                    'subject': payload.get('subject', ''),
                    'status': 'delivered',
                    'timestamp': time.time()
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        elif service_type == ServiceType.NEWS_WEBSITE:
            return {
                'status': 'success',
                'service_type': 'news_website',
                'response': {
                    'article_id': payload.get('article_id', 0),
                    'category': payload.get('category', ''),
                    'user_id': payload.get('user_id', ''),
                    'action': payload.get('action', ''),
                    'content_length': payload.get('content_length', 0),
                    'title': f"Article {payload.get('article_id', 0)}",
                    'content': "Lorem ipsum dolor sit amet..." * (payload.get('content_length', 1000) // 26),
                    'views': random.randint(100, 10000)
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        elif service_type == ServiceType.CALLING_SERVICE:
            return {
                'status': 'success',
                'service_type': 'calling_service',
                'response': {
                    'call_id': payload.get('call_id', ''),
                    'caller_id': payload.get('caller_id', ''),
                    'callee_id': payload.get('callee_id', ''),
                    'call_type': payload.get('call_type', ''),
                    'duration': payload.get('duration', 0),
                    'quality': payload.get('quality', ''),
                    'status': 'connected',
                    'start_time': time.time()
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        elif service_type == ServiceType.VIDEO_STREAMING:
            return {
                'status': 'success',
                'service_type': 'video_streaming',
                'response': {
                    'video_id': payload.get('video_id', ''),
                    'user_id': payload.get('user_id', ''),
                    'action': payload.get('action', ''),
                    'resolution': payload.get('resolution', ''),
                    'bitrate': payload.get('bitrate', 0),
                    'duration': payload.get('duration', 0),
                    'stream_url': f"https://stream.example.com/{payload.get('video_id', '')}",
                    'status': 'streaming'
                },
                'processing_time': processing_time,
                'server': self.server_name
            }
        
        return {
            'status': 'success',
            'service_type': service_type.value,
            'response': {'message': 'Request processed successfully'},
            'processing_time': processing_time,
            'server': self.server_name
        }
    
    def _update_metrics(self, service_type: ServiceType, success: bool, processing_time: float, handler: ServiceHandler):
        """Update performance metrics"""
        self.metrics['total_requests'] += 1
        self.metrics['total_processing_time'] += processing_time
        
        service_stats = self.metrics['service_stats'][service_type.value]
        service_stats['requests'] += 1
        service_stats['total_processing_time'] += processing_time
        
        # Calculate CPU and memory usage
        cpu_usage = handler.cpu_intensity * 100.0  # Convert to percentage
        memory_usage = handler.memory_intensity * 1024.0  # Convert to MB
        
        service_stats['total_cpu_usage'] += cpu_usage
        service_stats['total_memory_usage'] += memory_usage
        
        if success:
            self.metrics['successful_requests'] += 1
            service_stats['successful'] += 1
        else:
            self.metrics['failed_requests'] += 1
            service_stats['failed'] += 1
        
        # Update averages
        if service_stats['requests'] > 0:
            service_stats['avg_processing_time'] = service_stats['total_processing_time'] / service_stats['requests']
        
        if self.metrics['total_requests'] > 0:
            self.metrics['avg_processing_time'] = self.metrics['total_processing_time'] / self.metrics['total_requests']
    
    def get_metrics(self) -> Dict:
        """Get server performance metrics"""
        return {
            'server_name': self.server_name,
            'summary': {
                'total_requests': self.metrics['total_requests'],
                'successful_requests': self.metrics['successful_requests'],
                'failed_requests': self.metrics['failed_requests'],
                'success_rate': (self.metrics['successful_requests'] / self.metrics['total_requests'] * 100) if self.metrics['total_requests'] > 0 else 0,
                'avg_processing_time': self.metrics['avg_processing_time'],
                'total_processing_time': self.metrics['total_processing_time']
            },
            'service_stats': self.metrics['service_stats'],
            'recent_requests': self.request_history[-10:] if self.request_history else []
        }


# Global server application instance
server_app = None


def initialize_server():
    """Initialize the server application"""
    global server_app
    
    server_name = os.getenv('SERVER_NAME', 'server-1')
    server_app = ServerApplication(server_name)
    
    logger.info(f"Server application initialized: {server_name}")


@app.route('/process', methods=['POST'])
def process_request():
    """Process a service request"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Process the request
        response = server_app.process_request(data)
        
        return jsonify(response)
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get server metrics"""
    try:
        metrics = server_app.get_metrics()
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Get system metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        
        return jsonify({
            'status': 'healthy',
            'server_name': server_app.server_name if server_app else 'unknown',
            'timestamp': time.time(),
            'system': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available': memory.available // (1024 * 1024),  # MB
                'memory_total': memory.total // (1024 * 1024)  # MB
            }
        })
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        'message': f'Server application running: {server_app.server_name if server_app else "unknown"}',
        'endpoints': {
            'process': '/process - Process service requests',
            'metrics': '/metrics - Get server metrics',
            'health': '/health - Health check'
        }
    })


if __name__ == '__main__':
    initialize_server()
    
    port = int(os.getenv('SERVER_PORT', '5000'))
    debug = os.getenv('DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting server application on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
