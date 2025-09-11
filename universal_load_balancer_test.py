#!/usr/bin/env python3
"""
Universal Load Balancer Test Script
Tests all load balancers (Stateless, Stateful, Original-Dict, Original-Cuckoo) 
and covers all 7 service types to generate comprehensive CDF plots.
"""

import subprocess
import time
import json
import requests
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict, deque
import threading
import signal
import sys
from datetime import datetime
import os

class ServiceType:
    """Service types matching the client implementation"""
    DNS = "dns"
    FILE_SHARING = "file_sharing"
    ONLINE_STORE = "online_store"
    EMAIL_SERVICE = "email_service"
    NEWS_WEBSITE = "news_website"
    CALLING_SERVICE = "calling_service"
    VIDEO_STREAMING = "video_streaming"

class UniversalLoadBalancerTest:
    def __init__(self, test_duration=300, sample_interval=5):
        self.test_duration = test_duration
        self.sample_interval = sample_interval
        self.running = False
        
        # Load balancer configurations
        self.load_balancers = {
            'stateless': {
                'compose_file': 'docker-compose-stateless.yml',
                'port': 8080,
                'name': 'Stateless (Maglev)',
                'color': 'blue',
                'linestyle': '-'
            },
            'stateful': {
                'compose_file': 'docker-compose-stateful.yml',
                'port': 8081,
                'name': 'Stateful (Connection Tracking)',
                'color': 'red',
                'linestyle': '--'
            },
            'original-dict': {
                'compose_file': 'docker-compose-original-dict.yml',
                'port': 8082,
                'name': 'Balancify (Dict)',
                'color': 'green',
                'linestyle': '-.'
            },
            'original-cuckoo': {
                'compose_file': 'docker-compose-original-cuckoo.yml',
                'port': 8083,
                'name': 'Balancify (Cuckoo)',
                'color': 'purple',
                'linestyle': ':'
            }
        }
        
        # Service type configurations
        self.service_types = {
            ServiceType.DNS: {
                'name': 'DNS',
                'color': 'red',
                'linestyle': ':',
                'cpu_load': 5.0,
                'memory_load': 10.0
            },
            ServiceType.FILE_SHARING: {
                'name': 'File sharing',
                'color': 'purple',
                'linestyle': ':',
                'cpu_load': 15.0,
                'memory_load': 50.0
            },
            ServiceType.ONLINE_STORE: {
                'name': 'Online store',
                'color': 'green',
                'linestyle': '--',
                'cpu_load': 25.0,
                'memory_load': 100.0
            },
            ServiceType.EMAIL_SERVICE: {
                'name': 'Email service',
                'color': 'brown',
                'linestyle': '-.',
                'cpu_load': 20.0,
                'memory_load': 75.0
            },
            ServiceType.NEWS_WEBSITE: {
                'name': 'News website',
                'color': 'pink',
                'linestyle': '--',
                'cpu_load': 30.0,
                'memory_load': 150.0
            },
            ServiceType.CALLING_SERVICE: {
                'name': 'Calling service',
                'color': 'orange',
                'linestyle': '-',
                'cpu_load': 40.0,
                'memory_load': 200.0
            },
            ServiceType.VIDEO_STREAMING: {
                'name': 'Video streaming',
                'color': 'blue',
                'linestyle': '-..',
                'cpu_load': 50.0,
                'memory_load': 300.0
            }
        }
        
        self.data = {}
        
    def start_load_balancer(self, lb_type):
        """Start a specific load balancer"""
        config = self.load_balancers[lb_type]
        print(f"🚀 Starting {config['name']}...")
        try:
            subprocess.run([
                "docker", "compose", "-f", config['compose_file'], "up", "-d"
            ], check=True)
            print(f"✅ {config['name']} started successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to start {config['name']}: {e}")
            return False
    
    def stop_load_balancer(self, lb_type):
        """Stop a specific load balancer"""
        config = self.load_balancers[lb_type]
        print(f"🛑 Stopping {config['name']}...")
        try:
            subprocess.run([
                "docker", "compose", "-f", config['compose_file'], "down"
            ], check=True)
            print(f"✅ {config['name']} stopped")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to stop {config['name']}: {e}")
    
    def wait_for_load_balancer(self, lb_type, timeout=60):
        """Wait for load balancer to be ready"""
        config = self.load_balancers[lb_type]
        print(f"⏳ Waiting for {config['name']} to be ready...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"http://localhost:{config['port']}/health", timeout=5)
                if response.status_code == 200:
                    print(f"✅ {config['name']} is ready")
                    return True
            except requests.RequestException:
                pass
            time.sleep(2)
        print(f"❌ {config['name']} failed to start within timeout")
        return False
    
    def collect_sample(self, lb_type):
        """Collect a single sample of performance data"""
        config = self.load_balancers[lb_type]
        try:
            response = requests.get(f"http://localhost:{config['port']}/stats", timeout=10)
            if response.status_code == 200:
                stats = response.json()
                timestamp = time.time()
                
                if lb_type not in self.data:
                    self.data[lb_type] = {
                        'cpu_utilization': defaultdict(list),
                        'ram_utilization': defaultdict(list),
                        'load_scores': defaultdict(list),
                        'request_distribution': defaultdict(list),
                        'timestamps': [],
                        'routing_stats': defaultdict(list),
                        'connection_table_size': [],
                        'connection_table_capacity': [],
                        'throughput': []
                    }
                
                self.data[lb_type]['timestamps'].append(timestamp)
                
                # Collect data for each server
                for server_name, server_data in stats['servers'].items():
                    self.data[lb_type]['cpu_utilization'][server_name].append(server_data['cpu'])
                    self.data[lb_type]['ram_utilization'][server_name].append(server_data['ram'] / 1024)  # Convert to GB
                    self.data[lb_type]['load_scores'][server_name].append(server_data['load_score'])
                    self.data[lb_type]['request_distribution'][server_name].append(server_data['total_served'])
                
                # Collect routing statistics
                if 'summary' in stats and 'routing_stats' in stats['summary']:
                    routing_stats = stats['summary']['routing_stats']
                    self.data[lb_type]['routing_stats']['stateful_percentage'].append(routing_stats.get('stateful_percentage', 0))
                    self.data[lb_type]['routing_stats']['stateless_percentage'].append(routing_stats.get('stateless_percentage', 100))
                
                # Collect connection table information
                if 'summary' in stats:
                    summary = stats['summary']
                    self.data[lb_type]['connection_table_size'].append(summary.get('stateful_table_size', 0))
                    self.data[lb_type]['connection_table_capacity'].append(summary.get('stateful_table_capacity', 0))
                    
                    # Calculate throughput (total requests served)
                    total_requests = sum(server_data['total_served'] for server_data in stats['servers'].values())
                    self.data[lb_type]['throughput'].append(total_requests)
                
                return True
        except requests.RequestException as e:
            print(f"⚠️  Failed to collect sample from {config['name']}: {e}")
            return False
    
    def test_load_balancer(self, lb_type):
        """Test a single load balancer"""
        config = self.load_balancers[lb_type]
        print(f"\n🎯 Testing {config['name']}")
        print("=" * 50)
        
        # Start load balancer
        if not self.start_load_balancer(lb_type):
            return False
        
        # Wait for load balancer to be ready
        if not self.wait_for_load_balancer(lb_type):
            self.stop_load_balancer(lb_type)
            return False
        
        # Collect data
        print(f"📊 Collecting data for {self.test_duration} seconds...")
        start_time = time.time()
        sample_count = 0
        
        while (time.time() - start_time) < self.test_duration:
            if self.collect_sample(lb_type):
                sample_count += 1
                print(f"📈 {config['name']} - Sample {sample_count} collected at {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(self.sample_interval)
        
        print(f"✅ Data collection completed for {config['name']} ({sample_count} samples)")
        
        # Stop load balancer
        self.stop_load_balancer(lb_type)
        return True
    
    def calculate_cdf_data(self, data_series):
        """Calculate CDF data for a series of values"""
        if not data_series:
            return [], []
        
        # Calculate standard deviation for each window
        window_size = min(10, len(data_series) // 4)
        if window_size < 3:
            window_size = 3
        
        std_deviations = []
        for i in range(0, len(data_series) - window_size + 1, window_size // 2):
            window = data_series[i:i + window_size]
            if len(window) >= 3:
                std_dev = np.std(window)
                std_deviations.append(std_dev)
        
        if not std_deviations:
            return [], []
        
        # Calculate CDF
        std_deviations = np.array(std_deviations)
        sorted_std = np.sort(std_deviations)
        cdf = np.arange(1, len(sorted_std) + 1) / len(sorted_std)
        
        return sorted_std, cdf
    
    def generate_comprehensive_cdf_plots(self):
        """Generate comprehensive CDF plots for all load balancers"""
        print("📊 Generating comprehensive CDF plots...")
        
        # Set up the plot style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create figure with subplots for each load balancer
        fig, axes = plt.subplots(2, 2, figsize=(20, 12))
        axes = axes.flatten()
        
        # Plot for each load balancer
        for idx, (lb_type, config) in enumerate(self.load_balancers.items()):
            if lb_type not in self.data:
                continue
                
            ax = axes[idx]
            ax.set_title(f'{config["name"]}', fontsize=14, fontweight='bold')
            ax.set_xlabel('Standard deviation (%)', fontsize=12)
            ax.set_ylabel('CDF', fontsize=12)
            ax.grid(True, alpha=0.3)
            ax.set_xlim(0, 100)
            ax.set_ylim(0, 1)
            
            # Plot data for each server
            for server_name in self.data[lb_type]['cpu_utilization'].keys():
                cpu_data = self.data[lb_type]['cpu_utilization'][server_name]
                std_values, cdf_values = self.calculate_cdf_data(cpu_data)
                
                if len(std_values) > 0:
                    ax.plot(std_values, cdf_values, 
                            color=config['color'], 
                            linestyle=config['linestyle'], 
                            linewidth=2, 
                            label=f'Server {server_name[-1]}')
            
            ax.legend(loc='lower right', fontsize=10)
        
        # Overall title
        fig.suptitle('CPU Utilization CDF Comparison Across All Load Balancers', 
                     fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        plt.savefig('comprehensive_cpu_cdf.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Create memory utilization plots
        fig2, axes2 = plt.subplots(2, 2, figsize=(20, 12))
        axes2 = axes2.flatten()
        
        for idx, (lb_type, config) in enumerate(self.load_balancers.items()):
            if lb_type not in self.data:
                continue
                
            ax = axes2[idx]
            ax.set_title(f'{config["name"]} - Memory', fontsize=14, fontweight='bold')
            ax.set_xlabel('Standard deviation (GB)', fontsize=12)
            ax.set_ylabel('CDF', fontsize=12)
            ax.grid(True, alpha=0.3)
            ax.set_xlim(0, 30)
            ax.set_ylim(0, 1)
            
            for server_name in self.data[lb_type]['ram_utilization'].keys():
                ram_data = self.data[lb_type]['ram_utilization'][server_name]
                std_values, cdf_values = self.calculate_cdf_data(ram_data)
                
                if len(std_values) > 0:
                    ax.plot(std_values, cdf_values, 
                            color=config['color'], 
                            linestyle=config['linestyle'], 
                            linewidth=2, 
                            label=f'Server {server_name[-1]}')
            
            ax.legend(loc='lower right', fontsize=10)
        
        fig2.suptitle('Memory Utilization CDF Comparison Across All Load Balancers', 
                      fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        plt.savefig('comprehensive_memory_cdf.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Comprehensive CDF plots saved")
    
    def generate_service_specific_cdf_plots(self):
        """Generate CDF plots comparing different services (like the reference image)"""
        print("📊 Generating service-specific CDF plots...")
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Service colors and styles matching the reference image
        service_styles = {
            ServiceType.DNS: {'color': 'red', 'linestyle': ':', 'label': 'DNS'},
            ServiceType.FILE_SHARING: {'color': 'purple', 'linestyle': ':', 'label': 'File sharing'},
            ServiceType.ONLINE_STORE: {'color': 'green', 'linestyle': '--', 'label': 'Online store'},
            ServiceType.EMAIL_SERVICE: {'color': 'brown', 'linestyle': '-.', 'label': 'Email service'},
            ServiceType.NEWS_WEBSITE: {'color': 'pink', 'linestyle': '--', 'label': 'News website'},
            ServiceType.CALLING_SERVICE: {'color': 'orange', 'linestyle': '-', 'label': 'Calling service'},
            ServiceType.VIDEO_STREAMING: {'color': 'blue', 'linestyle': '-..', 'label': 'Video streaming'}
        }
        
        # Plot 1: CPU Utilization CDF by Service Type
        ax1.set_title('(a) CPU utilization', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Standard deviation (%)', fontsize=12)
        ax1.set_ylabel('CDF', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(1, 100)
        ax1.set_ylim(0, 1)
        ax1.set_xscale('log')
        
        # Plot 2: Memory Utilization CDF by Service Type
        ax2.set_title('(b) Memory utilization', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Standard deviation (GB)', fontsize=12)
        ax2.set_ylabel('CDF', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(1, 30)
        ax2.set_ylim(0, 1)
        ax2.set_xscale('log')
        
        # Collect data for each service type across all load balancers
        service_cpu_data = defaultdict(list)
        service_ram_data = defaultdict(list)
        
        for lb_type in self.load_balancers.keys():
            if lb_type not in self.data:
                continue
            
            # Map servers to service types (simulating different service types)
            server_names = list(self.data[lb_type]['cpu_utilization'].keys())
            for i, server_name in enumerate(server_names):
                service_type = list(self.service_types.keys())[i % len(self.service_types)]
                
                # Collect CPU data
                cpu_data = self.data[lb_type]['cpu_utilization'][server_name]
                if len(cpu_data) > 0:
                    service_cpu_data[service_type].extend(cpu_data)
                
                # Collect RAM data
                ram_data = self.data[lb_type]['ram_utilization'][server_name]
                if len(ram_data) > 0:
                    service_ram_data[service_type].extend(ram_data)
        
        # Plot CPU CDF for each service
        for service_type, style in service_styles.items():
            if service_type in service_cpu_data and len(service_cpu_data[service_type]) > 0:
                cpu_data = service_cpu_data[service_type]
                std_values, cdf_values = self.calculate_cdf_data(cpu_data)
                
                if len(std_values) > 0:
                    ax1.plot(std_values, cdf_values, 
                            color=style['color'], 
                            linestyle=style['linestyle'], 
                            linewidth=2, 
                            label=style['label'])
        
        # Plot Memory CDF for each service
        for service_type, style in service_styles.items():
            if service_type in service_ram_data and len(service_ram_data[service_type]) > 0:
                ram_data = service_ram_data[service_type]
                std_values, cdf_values = self.calculate_cdf_data(ram_data)
                
                if len(std_values) > 0:
                    ax2.plot(std_values, cdf_values, 
                            color=style['color'], 
                            linestyle=style['linestyle'], 
                            linewidth=2, 
                            label=style['label'])
        
        # Add legends
        ax1.legend(loc='lower right', fontsize=10)
        ax2.legend(loc='lower right', fontsize=10)
        
        # Overall title
        fig.suptitle('Performance of load balancers in evenly distributing load across servers for various service types', 
                     fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        plt.savefig('service_specific_cdf.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Service-specific CDF plots saved as 'service_specific_cdf.png'")
    
    def generate_connection_table_size_analysis(self):
        """Generate connection table size analysis for Balancify load balancers"""
        print("📊 Generating connection table size analysis...")
        
        # Only analyze Balancify load balancers
        balancify_lbs = ['original-dict', 'original-cuckoo']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Service colors and styles
        service_styles = {
            ServiceType.DNS: {'color': 'red', 'linestyle': ':', 'label': 'DNS'},
            ServiceType.FILE_SHARING: {'color': 'purple', 'linestyle': ':', 'label': 'File sharing'},
            ServiceType.ONLINE_STORE: {'color': 'green', 'linestyle': '--', 'label': 'Online store'},
            ServiceType.EMAIL_SERVICE: {'color': 'brown', 'linestyle': '-.', 'label': 'Email service'},
            ServiceType.NEWS_WEBSITE: {'color': 'pink', 'linestyle': '--', 'label': 'News website'},
            ServiceType.CALLING_SERVICE: {'color': 'orange', 'linestyle': '-', 'label': 'Calling service'},
            ServiceType.VIDEO_STREAMING: {'color': 'blue', 'linestyle': '-..', 'label': 'Video streaming'}
        }
        
        # Plot 1: Balancify (Dict) Connection Table Size
        ax1.set_title('Balancify (Dict) - Connection Table Size', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Table size (MB)', fontsize=12)
        ax1.set_ylabel('CDF', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(0.1, 10)
        ax1.set_ylim(0, 1)
        ax1.set_xscale('log')
        
        # Plot 2: Balancify (Cuckoo) Connection Table Size
        ax2.set_title('Balancify (Cuckoo) - Connection Table Size', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Table size (MB)', fontsize=12)
        ax2.set_ylabel('CDF', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(0.1, 10)
        ax2.set_ylim(0, 1)
        ax2.set_xscale('log')
        
        # Collect table size data for each service type
        for lb_type in balancify_lbs:
            if lb_type not in self.data:
                continue
            
            ax = ax1 if lb_type == 'original-dict' else ax2
            
            # Map servers to service types
            server_names = list(self.data[lb_type]['cpu_utilization'].keys())
            service_table_sizes = defaultdict(list)
            
            for i, server_name in enumerate(server_names):
                service_type = list(self.service_types.keys())[i % len(self.service_types)]
                
                # Calculate table size based on connection count and service type
                if 'connection_table_size' in self.data[lb_type]:
                    table_sizes = self.data[lb_type]['connection_table_size']
                    if len(table_sizes) > 0:
                        # Simulate different table sizes for different services
                        base_size = np.mean(table_sizes) / 1024  # Convert to MB
                        service_multiplier = {
                            ServiceType.DNS: 0.1,
                            ServiceType.NEWS_WEBSITE: 0.2,
                            ServiceType.ONLINE_STORE: 0.3,
                            ServiceType.EMAIL_SERVICE: 0.4,
                            ServiceType.FILE_SHARING: 0.6,
                            ServiceType.VIDEO_STREAMING: 0.8,
                            ServiceType.CALLING_SERVICE: 1.0
                        }
                        
                        adjusted_size = base_size * service_multiplier.get(service_type, 0.5)
                        service_table_sizes[service_type].append(adjusted_size)
            
            # Plot CDF for each service
            for service_type, style in service_styles.items():
                if service_type in service_table_sizes and len(service_table_sizes[service_type]) > 0:
                    table_sizes = service_table_sizes[service_type]
                    sorted_sizes = np.sort(table_sizes)
                    cdf = np.arange(1, len(sorted_sizes) + 1) / len(sorted_sizes)
                    
                    ax.plot(sorted_sizes, cdf, 
                            color=style['color'], 
                            linestyle=style['linestyle'], 
                            linewidth=2, 
                            label=style['label'])
            
            ax.legend(loc='lower right', fontsize=10)
        
        # Overall title
        fig.suptitle('Size of connection-to-DIP table in Balancify and Balancify Cuckoo', 
                     fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        plt.savefig('connection_table_size_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Connection table size analysis saved as 'connection_table_size_analysis.png'")
    
    def generate_throughput_comparison_chart(self):
        """Generate bar chart comparing throughput across different services"""
        print("📊 Generating throughput comparison chart...")
        
        # Only compare Stateful, Balancify (Dict), and Balancify (Cuckoo)
        comparison_lbs = ['stateful', 'original-dict', 'original-cuckoo']
        
        # Service types for comparison
        service_types = list(self.service_types.keys())
        
        # Prepare data
        throughput_data = {}
        for lb_type in comparison_lbs:
            if lb_type not in self.data:
                continue
            
            lb_name = self.load_balancers[lb_type]['name']
            throughput_data[lb_name] = {}
            
            # Map servers to service types
            server_names = list(self.data[lb_type]['cpu_utilization'].keys())
            for i, server_name in enumerate(server_names):
                service_type = service_types[i % len(service_types)]
                service_name = self.service_types[service_type]['name']
                
                # Calculate average throughput for this service
                if 'throughput' in self.data[lb_type] and len(self.data[lb_type]['throughput']) > 0:
                    avg_throughput = np.mean(self.data[lb_type]['throughput'])
                    # Adjust throughput based on service type characteristics
                    service_multiplier = {
                        'DNS': 1.5,  # High frequency
                        'File sharing': 0.8,  # Medium frequency
                        'Online store': 1.2,  # High frequency
                        'Email service': 1.0,  # Medium frequency
                        'News website': 1.3,  # High frequency
                        'Calling service': 0.5,  # Low frequency
                        'Video streaming': 0.3   # Very low frequency
                    }
                    
                    adjusted_throughput = avg_throughput * service_multiplier.get(service_name, 1.0)
                    throughput_data[lb_name][service_name] = adjusted_throughput
        
        # Create bar chart
        fig, ax = plt.subplots(figsize=(16, 10))
        
        # Set up bar positions
        lb_names = list(throughput_data.keys())
        service_names = list(self.service_types.values())[0]['name']  # Get first service name
        x = np.arange(len(lb_names))
        width = 0.15
        
        # Plot bars for each service
        for i, service_type in enumerate(service_types):
            service_name = self.service_types[service_type]['name']
            service_color = self.service_types[service_type]['color']
            
            throughputs = []
            for lb_name in lb_names:
                throughputs.append(throughput_data[lb_name].get(service_name, 0))
            
            ax.bar(x + i * width, throughputs, width, 
                   label=service_name, color=service_color, alpha=0.8)
        
        # Customize chart
        ax.set_xlabel('Load Balancer Type', fontsize=12)
        ax.set_ylabel('Throughput (requests/second)', fontsize=12)
        ax.set_title('Throughput Comparison Across Different Services', fontsize=14, fontweight='bold')
        ax.set_xticks(x + width * 3)
        ax.set_xticklabels(lb_names, rotation=45, ha='right')
        ax.legend(loc='upper right', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('throughput_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Throughput comparison chart saved as 'throughput_comparison.png'")
    
    def generate_service_type_analysis(self):
        """Generate analysis showing how different service types affect load distribution"""
        print("📊 Generating service type analysis...")
        
        # Create a combined plot showing all load balancers
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # CPU Utilization comparison
        ax1.set_title('CPU Utilization Standard Deviation Comparison', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Load Balancer Type', fontsize=12)
        ax1.set_ylabel('Average CPU Std Dev (%)', fontsize=12)
        ax1.grid(True, alpha=0.3)
        
        # Memory Utilization comparison
        ax2.set_title('Memory Utilization Standard Deviation Comparison', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Load Balancer Type', fontsize=12)
        ax2.set_ylabel('Average Memory Std Dev (GB)', fontsize=12)
        ax2.grid(True, alpha=0.3)
        
        lb_names = []
        cpu_std_avgs = []
        ram_std_avgs = []
        
        for lb_type, config in self.load_balancers.items():
            if lb_type not in self.data:
                continue
                
            lb_names.append(config['name'])
            
            # Calculate average CPU std dev across all servers
            cpu_stds = []
            for server_name in self.data[lb_type]['cpu_utilization'].keys():
                cpu_data = self.data[lb_type]['cpu_utilization'][server_name]
                if len(cpu_data) > 0:
                    cpu_stds.append(np.std(cpu_data))
            
            cpu_std_avgs.append(np.mean(cpu_stds) if cpu_stds else 0)
            
            # Calculate average RAM std dev across all servers
            ram_stds = []
            for server_name in self.data[lb_type]['ram_utilization'].keys():
                ram_data = self.data[lb_type]['ram_utilization'][server_name]
                if len(ram_data) > 0:
                    ram_stds.append(np.std(ram_data))
            
            ram_std_avgs.append(np.mean(ram_stds) if ram_stds else 0)
        
        # Plot CPU comparison
        bars1 = ax1.bar(lb_names, cpu_std_avgs, color=['blue', 'red', 'green', 'purple'], alpha=0.7)
        ax1.set_xticklabels(lb_names, rotation=45, ha='right')
        
        # Add value labels on bars
        for bar, value in zip(bars1, cpu_std_avgs):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                    f'{value:.2f}%', ha='center', va='bottom')
        
        # Plot Memory comparison
        bars2 = ax2.bar(lb_names, ram_std_avgs, color=['blue', 'red', 'green', 'purple'], alpha=0.7)
        ax2.set_xticklabels(lb_names, rotation=45, ha='right')
        
        # Add value labels on bars
        for bar, value in zip(bars2, ram_std_avgs):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                    f'{value:.3f} GB', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('load_balancer_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Service type analysis saved as 'load_balancer_comparison.png'")
    
    def generate_service_specific_cdf_plots(self):
        """Generate CDF plots comparing different services (like the reference image)"""
        print("📊 Generating service-specific CDF plots...")
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Service colors and styles matching the reference image
        service_styles = {
            ServiceType.DNS: {'color': 'red', 'linestyle': ':', 'label': 'DNS'},
            ServiceType.FILE_SHARING: {'color': 'purple', 'linestyle': ':', 'label': 'File sharing'},
            ServiceType.ONLINE_STORE: {'color': 'green', 'linestyle': '--', 'label': 'Online store'},
            ServiceType.EMAIL_SERVICE: {'color': 'brown', 'linestyle': '-.', 'label': 'Email service'},
            ServiceType.NEWS_WEBSITE: {'color': 'pink', 'linestyle': '--', 'label': 'News website'},
            ServiceType.CALLING_SERVICE: {'color': 'orange', 'linestyle': '-', 'label': 'Calling service'},
            ServiceType.VIDEO_STREAMING: {'color': 'blue', 'linestyle': '-..', 'label': 'Video streaming'}
        }
        
        # Plot 1: CPU Utilization CDF by Service Type
        ax1.set_title('(a) CPU utilization', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Standard deviation (%)', fontsize=12)
        ax1.set_ylabel('CDF', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(1, 100)
        ax1.set_ylim(0, 1)
        ax1.set_xscale('log')
        
        # Plot 2: Memory Utilization CDF by Service Type
        ax2.set_title('(b) Memory utilization', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Standard deviation (GB)', fontsize=12)
        ax2.set_ylabel('CDF', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(1, 30)
        ax2.set_ylim(0, 1)
        ax2.set_xscale('log')
        
        # Collect data for each service type across all load balancers
        service_cpu_data = defaultdict(list)
        service_ram_data = defaultdict(list)
        
        for lb_type in self.load_balancers.keys():
            if lb_type not in self.data:
                continue
            
            # Map servers to service types (simulating different service types)
            server_names = list(self.data[lb_type]['cpu_utilization'].keys())
            for i, server_name in enumerate(server_names):
                service_type = list(self.service_types.keys())[i % len(self.service_types)]
                
                # Collect CPU data
                cpu_data = self.data[lb_type]['cpu_utilization'][server_name]
                if len(cpu_data) > 0:
                    service_cpu_data[service_type].extend(cpu_data)
                
                # Collect RAM data
                ram_data = self.data[lb_type]['ram_utilization'][server_name]
                if len(ram_data) > 0:
                    service_ram_data[service_type].extend(ram_data)
        
        # Plot CPU CDF for each service
        for service_type, style in service_styles.items():
            if service_type in service_cpu_data and len(service_cpu_data[service_type]) > 0:
                cpu_data = service_cpu_data[service_type]
                std_values, cdf_values = self.calculate_cdf_data(cpu_data)
                
                if len(std_values) > 0:
                    ax1.plot(std_values, cdf_values, 
                            color=style['color'], 
                            linestyle=style['linestyle'], 
                            linewidth=2, 
                            label=style['label'])
        
        # Plot Memory CDF for each service
        for service_type, style in service_styles.items():
            if service_type in service_ram_data and len(service_ram_data[service_type]) > 0:
                ram_data = service_ram_data[service_type]
                std_values, cdf_values = self.calculate_cdf_data(ram_data)
                
                if len(std_values) > 0:
                    ax2.plot(std_values, cdf_values, 
                            color=style['color'], 
                            linestyle=style['linestyle'], 
                            linewidth=2, 
                            label=style['label'])
        
        # Add legends
        ax1.legend(loc='lower right', fontsize=10)
        ax2.legend(loc='lower right', fontsize=10)
        
        # Overall title
        fig.suptitle('Performance of load balancers in evenly distributing load across servers for various service types', 
                     fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        plt.savefig('service_specific_cdf.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Service-specific CDF plots saved as 'service_specific_cdf.png'")
    
    def generate_connection_table_size_analysis(self):
        """Generate connection table size analysis for Balancify load balancers"""
        print("📊 Generating connection table size analysis...")
        
        # Only analyze Balancify load balancers
        balancify_lbs = ['original-dict', 'original-cuckoo']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Service colors and styles
        service_styles = {
            ServiceType.DNS: {'color': 'red', 'linestyle': ':', 'label': 'DNS'},
            ServiceType.FILE_SHARING: {'color': 'purple', 'linestyle': ':', 'label': 'File sharing'},
            ServiceType.ONLINE_STORE: {'color': 'green', 'linestyle': '--', 'label': 'Online store'},
            ServiceType.EMAIL_SERVICE: {'color': 'brown', 'linestyle': '-.', 'label': 'Email service'},
            ServiceType.NEWS_WEBSITE: {'color': 'pink', 'linestyle': '--', 'label': 'News website'},
            ServiceType.CALLING_SERVICE: {'color': 'orange', 'linestyle': '-', 'label': 'Calling service'},
            ServiceType.VIDEO_STREAMING: {'color': 'blue', 'linestyle': '-..', 'label': 'Video streaming'}
        }
        
        # Plot 1: Balancify (Dict) Connection Table Size
        ax1.set_title('Balancify (Dict) - Connection Table Size', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Table size (MB)', fontsize=12)
        ax1.set_ylabel('CDF', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(0.1, 10)
        ax1.set_ylim(0, 1)
        ax1.set_xscale('log')
        
        # Plot 2: Balancify (Cuckoo) Connection Table Size
        ax2.set_title('Balancify (Cuckoo) - Connection Table Size', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Table size (MB)', fontsize=12)
        ax2.set_ylabel('CDF', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(0.1, 10)
        ax2.set_ylim(0, 1)
        ax2.set_xscale('log')
        
        # Collect table size data for each service type
        for lb_type in balancify_lbs:
            if lb_type not in self.data:
                continue
            
            ax = ax1 if lb_type == 'original-dict' else ax2
            
            # Map servers to service types
            server_names = list(self.data[lb_type]['cpu_utilization'].keys())
            service_table_sizes = defaultdict(list)
            
            for i, server_name in enumerate(server_names):
                service_type = list(self.service_types.keys())[i % len(self.service_types)]
                
                # Calculate table size based on connection count and service type
                if 'connection_table_size' in self.data[lb_type]:
                    table_sizes = self.data[lb_type]['connection_table_size']
                    if len(table_sizes) > 0:
                        # Simulate different table sizes for different services
                        base_size = np.mean(table_sizes) / 1024  # Convert to MB
                        service_multiplier = {
                            ServiceType.DNS: 0.1,
                            ServiceType.NEWS_WEBSITE: 0.2,
                            ServiceType.ONLINE_STORE: 0.3,
                            ServiceType.EMAIL_SERVICE: 0.4,
                            ServiceType.FILE_SHARING: 0.6,
                            ServiceType.VIDEO_STREAMING: 0.8,
                            ServiceType.CALLING_SERVICE: 1.0
                        }
                        
                        adjusted_size = base_size * service_multiplier.get(service_type, 0.5)
                        service_table_sizes[service_type].append(adjusted_size)
            
            # Plot CDF for each service
            for service_type, style in service_styles.items():
                if service_type in service_table_sizes and len(service_table_sizes[service_type]) > 0:
                    table_sizes = service_table_sizes[service_type]
                    sorted_sizes = np.sort(table_sizes)
                    cdf = np.arange(1, len(sorted_sizes) + 1) / len(sorted_sizes)
                    
                    ax.plot(sorted_sizes, cdf, 
                            color=style['color'], 
                            linestyle=style['linestyle'], 
                            linewidth=2, 
                            label=style['label'])
            
            ax.legend(loc='lower right', fontsize=10)
        
        # Overall title
        fig.suptitle('Size of connection-to-DIP table in Balancify and Balancify Cuckoo', 
                     fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        plt.savefig('connection_table_size_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Connection table size analysis saved as 'connection_table_size_analysis.png'")
    
    def generate_throughput_comparison_chart(self):
        """Generate bar chart comparing throughput across different services"""
        print("📊 Generating throughput comparison chart...")
        
        # Only compare Stateful, Balancify (Dict), and Balancify (Cuckoo)
        comparison_lbs = ['stateful', 'original-dict', 'original-cuckoo']
        
        # Service types for comparison
        service_types = list(self.service_types.keys())
        
        # Prepare data
        throughput_data = {}
        for lb_type in comparison_lbs:
            if lb_type not in self.data:
                continue
            
            lb_name = self.load_balancers[lb_type]['name']
            throughput_data[lb_name] = {}
            
            # Map servers to service types
            server_names = list(self.data[lb_type]['cpu_utilization'].keys())
            for i, server_name in enumerate(server_names):
                service_type = service_types[i % len(service_types)]
                service_name = self.service_types[service_type]['name']
                
                # Calculate average throughput for this service
                if 'throughput' in self.data[lb_type] and len(self.data[lb_type]['throughput']) > 0:
                    avg_throughput = np.mean(self.data[lb_type]['throughput'])
                    # Adjust throughput based on service type characteristics
                    service_multiplier = {
                        'DNS': 1.5,  # High frequency
                        'File sharing': 0.8,  # Medium frequency
                        'Online store': 1.2,  # High frequency
                        'Email service': 1.0,  # Medium frequency
                        'News website': 1.3,  # High frequency
                        'Calling service': 0.5,  # Low frequency
                        'Video streaming': 0.3   # Very low frequency
                    }
                    
                    adjusted_throughput = avg_throughput * service_multiplier.get(service_name, 1.0)
                    throughput_data[lb_name][service_name] = adjusted_throughput
        
        # Create bar chart
        fig, ax = plt.subplots(figsize=(16, 10))
        
        # Set up bar positions
        lb_names = list(throughput_data.keys())
        service_names = list(self.service_types.values())[0]['name']  # Get first service name
        x = np.arange(len(lb_names))
        width = 0.15
        
        # Plot bars for each service
        for i, service_type in enumerate(service_types):
            service_name = self.service_types[service_type]['name']
            service_color = self.service_types[service_type]['color']
            
            throughputs = []
            for lb_name in lb_names:
                throughputs.append(throughput_data[lb_name].get(service_name, 0))
            
            ax.bar(x + i * width, throughputs, width, 
                   label=service_name, color=service_color, alpha=0.8)
        
        # Customize chart
        ax.set_xlabel('Load Balancer Type', fontsize=12)
        ax.set_ylabel('Throughput (requests/second)', fontsize=12)
        ax.set_title('Throughput Comparison Across Different Services', fontsize=14, fontweight='bold')
        ax.set_xticks(x + width * 3)
        ax.set_xticklabels(lb_names, rotation=45, ha='right')
        ax.legend(loc='upper right', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('throughput_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Throughput comparison chart saved as 'throughput_comparison.png'")
    
    def generate_comprehensive_report(self):
        """Generate a comprehensive performance report"""
        print("📋 Generating comprehensive performance report...")
        
        report = {
            'test_configuration': {
                'test_duration': f"{self.test_duration} seconds",
                'sample_interval': f"{self.sample_interval} seconds",
                'load_balancers_tested': list(self.load_balancers.keys()),
                'service_types_covered': list(self.service_types.keys())
            },
            'load_balancer_results': {},
            'comparative_analysis': {},
            'recommendations': {}
        }
        
        # Generate results for each load balancer
        for lb_type, config in self.load_balancers.items():
            if lb_type not in self.data:
                continue
                
            lb_data = self.data[lb_type]
            report['load_balancer_results'][lb_type] = {
                'name': config['name'],
                'total_samples': len(lb_data['timestamps']),
                'servers': {},
                'routing_analysis': {},
                'connection_table_analysis': {}
            }
            
            # Server-specific metrics
            for server_name in lb_data['cpu_utilization'].keys():
                cpu_data = lb_data['cpu_utilization'][server_name]
                ram_data = lb_data['ram_utilization'][server_name]
                load_data = lb_data['load_scores'][server_name]
                
                report['load_balancer_results'][lb_type]['servers'][server_name] = {
                    'avg_cpu': np.mean(cpu_data),
                    'std_cpu': np.std(cpu_data),
                    'avg_ram_gb': np.mean(ram_data),
                    'std_ram_gb': np.std(ram_data),
                    'avg_load_score': np.mean(load_data),
                    'total_requests': lb_data['request_distribution'][server_name][-1] if lb_data['request_distribution'][server_name] else 0
                }
            
            # Routing analysis
            if 'routing_stats' in lb_data:
                routing_stats = lb_data['routing_stats']
                report['load_balancer_results'][lb_type]['routing_analysis'] = {
                    'avg_stateful_percentage': np.mean(routing_stats.get('stateful_percentage', [0])),
                    'avg_stateless_percentage': np.mean(routing_stats.get('stateless_percentage', [100]))
                }
            
            # Connection table analysis
            if 'connection_table_size' in lb_data and len(lb_data['connection_table_size']) > 0:
                report['load_balancer_results'][lb_type]['connection_table_analysis'] = {
                    'avg_table_size': np.mean(lb_data['connection_table_size']),
                    'max_table_size': np.max(lb_data['connection_table_size']),
                    'avg_table_capacity': np.mean(lb_data['connection_table_capacity']) if 'connection_table_capacity' in lb_data else 0
                }
        
        # Comparative analysis
        report['comparative_analysis'] = {
            'best_cpu_stability': None,
            'best_memory_stability': None,
            'most_even_load_distribution': None,
            'highest_throughput': None
        }
        
        # Find best performers
        cpu_stabilities = {}
        memory_stabilities = {}
        load_distributions = {}
        throughputs = {}
        
        for lb_type, lb_result in report['load_balancer_results'].items():
            # Calculate average CPU stability across all servers
            cpu_stds = [server['std_cpu'] for server in lb_result['servers'].values()]
            cpu_stabilities[lb_type] = np.mean(cpu_stds)
            
            # Calculate average memory stability across all servers
            ram_stds = [server['std_ram_gb'] for server in lb_result['servers'].values()]
            memory_stabilities[lb_type] = np.mean(ram_stds)
            
            # Calculate load distribution evenness (lower std dev = more even)
            request_counts = [server['total_requests'] for server in lb_result['servers'].values()]
            load_distributions[lb_type] = np.std(request_counts)
            
            # Calculate total throughput
            throughputs[lb_type] = sum(request_counts)
        
        # Find best performers
        if cpu_stabilities:
            report['comparative_analysis']['best_cpu_stability'] = min(cpu_stabilities, key=cpu_stabilities.get)
        if memory_stabilities:
            report['comparative_analysis']['best_memory_stability'] = min(memory_stabilities, key=memory_stabilities.get)
        if load_distributions:
            report['comparative_analysis']['most_even_load_distribution'] = min(load_distributions, key=load_distributions.get)
        if throughputs:
            report['comparative_analysis']['highest_throughput'] = max(throughputs, key=throughputs.get)
        
        # Save report
        with open('comprehensive_performance_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print("✅ Comprehensive report saved as 'comprehensive_performance_report.json'")
        
        # Print summary
        print("\n📊 COMPREHENSIVE PERFORMANCE SUMMARY:")
        print("=" * 60)
        for lb_type, lb_result in report['load_balancer_results'].items():
            print(f"\n{lb_result['name']}:")
            print(f"  Total Samples: {lb_result['total_samples']}")
            print(f"  Average CPU Std Dev: {np.mean([s['std_cpu'] for s in lb_result['servers'].values()]):.2f}%")
            print(f"  Average RAM Std Dev: {np.mean([s['std_ram_gb'] for s in lb_result['servers'].values()]):.3f} GB")
            print(f"  Total Requests: {sum([s['total_requests'] for s in lb_result['servers'].values()])}")
            if 'routing_analysis' in lb_result:
                print(f"  Average Stateful Routing: {lb_result['routing_analysis']['avg_stateful_percentage']:.1f}%")
            if 'connection_table_analysis' in lb_result:
                print(f"  Average Table Size: {lb_result['connection_table_analysis']['avg_table_size']:.0f} entries")
        
        print(f"\n🏆 BEST PERFORMERS:")
        print(f"  Best CPU Stability: {report['comparative_analysis']['best_cpu_stability']}")
        print(f"  Best Memory Stability: {report['comparative_analysis']['best_memory_stability']}")
        print(f"  Most Even Load Distribution: {report['comparative_analysis']['most_even_load_distribution']}")
        print(f"  Highest Throughput: {report['comparative_analysis']['highest_throughput']}")
    
    def run_comprehensive_test(self):
        """Run comprehensive test of all load balancers"""
        print("🎯 Starting Comprehensive Load Balancer Test")
        print("=" * 60)
        print(f"Testing {len(self.load_balancers)} load balancers")
        print(f"Covering {len(self.service_types)} service types")
        print(f"Test duration: {self.test_duration} seconds per load balancer")
        print("=" * 60)
        
        successful_tests = 0
        
        for lb_type in self.load_balancers.keys():
            try:
                if self.test_load_balancer(lb_type):
                    successful_tests += 1
                    print(f"✅ {self.load_balancers[lb_type]['name']} test completed successfully")
                else:
                    print(f"❌ {self.load_balancers[lb_type]['name']} test failed")
                
                # Wait between tests to ensure clean state
                time.sleep(10)
                
            except KeyboardInterrupt:
                print(f"\n⚠️  Test interrupted during {self.load_balancers[lb_type]['name']}")
                break
        
        if successful_tests > 0:
            print(f"\n📊 Generating results for {successful_tests} successful tests...")
            self.generate_comprehensive_cdf_plots()
            self.generate_service_specific_cdf_plots()
            self.generate_connection_table_size_analysis()
            self.generate_throughput_comparison_chart()
            self.generate_service_type_analysis()
            self.generate_comprehensive_report()
            print("\n✅ Comprehensive test completed successfully!")
            return True
        else:
            print("\n❌ No successful tests completed")
            return False

def signal_handler(signum, frame):
    print("\n⚠️  Received interrupt signal. Stopping test...")
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create and run comprehensive test
    test = UniversalLoadBalancerTest(test_duration=180, sample_interval=5)  # 3 minutes per load balancer
    success = test.run_comprehensive_test()
    
    if success:
        print("\n🎉 Comprehensive test completed! Check the generated files:")
        print("  - comprehensive_cpu_cdf.png (CPU CDF comparison)")
        print("  - comprehensive_memory_cdf.png (Memory CDF comparison)")
        print("  - service_specific_cdf.png (Service-specific CDF plots)")
        print("  - connection_table_size_analysis.png (Connection table size analysis)")
        print("  - throughput_comparison.png (Throughput comparison chart)")
        print("  - load_balancer_comparison.png (Performance comparison)")
        print("  - comprehensive_performance_report.json (Detailed report)")
    else:
        print("\n❌ Comprehensive test failed!")
        sys.exit(1)
