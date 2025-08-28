"""
Comprehensive Data Collection and Logging System for blockchain
Implements detailed metrics collection for experimental validation
"""

import json
import csv
import time
import logging
import asyncio
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
from collections import defaultdict, deque
import threading
import sqlite3


@dataclass
class ExperimentMetrics:
    """Container for comprehensive experiment metrics"""
    timestamp: float
    experiment_id: str
    system_name: str
    
    # Performance metrics
    throughput_tps: float = 0.0
    latency_ms: float = 0.0
    success_rate: float = 0.0
    
    # Resource utilization
    cpu_utilization: Dict[str, float] = None
    memory_usage: float = 0.0
    network_throughput: float = 0.0
    storage_io: float = 0.0
    
    # Blockchain-specific metrics
    block_generation_time: float = 0.0
    consensus_time: float = 0.0
    shard_efficiency: Dict[str, float] = None
    cross_shard_latency: float = 0.0
    
    # Multi-tier metrics
    edge_processing_ratio: float = 0.0
    fog_processing_ratio: float = 0.0
    cloud_processing_ratio: float = 0.0
    tier_communication_cost: float = 0.0
    
    # Reputation metrics
    average_node_reputation: float = 0.0
    reputation_variance: float = 0.0
    malicious_node_detection_rate: float = 0.0
    
    # QoS metrics
    service_availability: float = 0.0
    response_time_99p: float = 0.0
    bandwidth_efficiency: float = 0.0
    
    def __post_init__(self):
        if self.cpu_utilization is None:
            self.cpu_utilization = {}
        if self.shard_efficiency is None:
            self.shard_efficiency = {}


class RealTimeDataCollector:
    """
    Real-time data collection system for blockchain experiments
    Provides continuous monitoring and metrics aggregation
    """
    
    def __init__(self, experiment_id: str, output_dir: str = "experimental_data"):
        self.experiment_id = experiment_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Data storage
        self.metrics_buffer = deque(maxlen=10000)  # In-memory buffer
        self.real_time_metrics = defaultdict(list)
        
        # Database connection
        self.db_path = self.output_dir / f"experiment_{experiment_id}.db"
        self._initialize_database()
        
        # Logging setup
        self.logger = logging.getLogger(f'DataCollector-{experiment_id}')
        self._setup_logging()
        
        # Collection state
        self.is_collecting = False
        self.collection_thread = None
        self.collection_interval = 1.0  # seconds
        
        # Performance tracking
        self.start_time = None
        self.last_collection_time = 0.0
        
    def _initialize_database(self):
        """Initialize SQLite database for persistent storage"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # Create metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                experiment_id TEXT,
                system_name TEXT,
                metric_type TEXT,
                metric_value REAL,
                additional_data TEXT
            )
        ''')
        
        # Create experiments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS experiments (
                experiment_id TEXT PRIMARY KEY,
                start_time REAL,
                end_time REAL,
                config TEXT,
                status TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Database initialized at {self.db_path}")
    
    def _setup_logging(self):
        """Setup detailed logging configuration"""
        log_file = self.output_dir / f"experiment_{self.experiment_id}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.DEBUG)
    
    async def start_collection(self, system_components: Dict):
        """Start real-time data collection"""
        if self.is_collecting:
            self.logger.warning("Data collection already started")
            return
        
        self.is_collecting = True
        self.start_time = time.time()
        self.system_components = system_components
        
        self.logger.info("Starting real-time data collection")
        
        # Start collection in background thread
        self.collection_thread = threading.Thread(
            target=self._collection_loop,
            daemon=True
        )
        self.collection_thread.start()
    
    def _collection_loop(self):
        """Background thread for continuous data collection"""
        while self.is_collecting:
            try:
                current_time = time.time()
                
                # Collect metrics from all system components
                metrics = self._collect_current_metrics()
                
                # Store in buffer and database
                self._store_metrics(metrics)
                
                # Update real-time aggregations
                self._update_real_time_aggregations(metrics)
                
                self.last_collection_time = current_time
                
                # Sleep until next collection interval
                time.sleep(self.collection_interval)
                
            except Exception as e:
                self.logger.error(f"Error in collection loop: {e}")
                time.sleep(1.0)  # Brief pause before retry
    
    def _collect_current_metrics(self) -> ExperimentMetrics:
        """Collect current system metrics"""
        current_time = time.time()
        
        # Initialize metrics object
        metrics = ExperimentMetrics(
            timestamp=current_time,
            experiment_id=self.experiment_id,
            system_name=self.system_components.get('system_name', 'unknown')
        )
        
        try:
            # Collect performance metrics
            self._collect_performance_metrics(metrics)
            
            # Collect resource utilization
            self._collect_resource_metrics(metrics)
            
            # Collect blockchain metrics
            self._collect_blockchain_metrics(metrics)
            
            # Collect multi-tier metrics
            self._collect_tier_metrics(metrics)
            
            # Collect reputation metrics
            self._collect_reputation_metrics(metrics)
            
            # Collect QoS metrics
            self._collect_qos_metrics(metrics)
            
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {e}")
        
        return metrics
    
    def _collect_performance_metrics(self, metrics: ExperimentMetrics):
        """Collect performance-related metrics"""
        if 'blockchain_manager' in self.system_components:
            blockchain = self.system_components['blockchain_manager']
            
            # Calculate throughput
            processed_transactions = sum(
                node.processed_transactions for node in blockchain.node_manager.nodes.values()
            )
            elapsed_time = time.time() - self.start_time
            metrics.throughput_tps = processed_transactions / max(elapsed_time, 1.0)
            
            # Calculate average latency
            processing_times = [
                node.average_processing_time for node in blockchain.node_manager.nodes.values()
                if node.average_processing_time > 0
            ]
            metrics.latency_ms = np.mean(processing_times) * 1000 if processing_times else 0.0
            
            # Calculate success rate
            total_processed = sum(node.processed_transactions for node in blockchain.node_manager.nodes.values())
            total_failed = sum(node.failed_transactions for node in blockchain.node_manager.nodes.values())
            total_attempts = total_processed + total_failed
            metrics.success_rate = (total_processed / total_attempts * 100) if total_attempts > 0 else 0.0
    
    def _collect_resource_metrics(self, metrics: ExperimentMetrics):
        """Collect resource utilization metrics"""
        try:
            import psutil
            
            # System-wide metrics
            metrics.memory_usage = psutil.virtual_memory().percent
            metrics.network_throughput = sum(psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv)
            metrics.storage_io = sum(psutil.disk_io_counters().read_bytes + psutil.disk_io_counters().write_bytes)
            
            # Per-tier CPU utilization
            if 'blockchain_manager' in self.system_components:
                blockchain = self.system_components['blockchain_manager']
                
                for tier in ['cloud', 'fog', 'edge']:
                    tier_nodes = [node for node in blockchain.node_manager.nodes.values() 
                                if node.tier.value == tier]
                    if tier_nodes:
                        avg_cpu = np.mean([node.cpu_utilization for node in tier_nodes])
                        metrics.cpu_utilization[tier] = avg_cpu
            
        except ImportError:
            self.logger.warning("psutil not available, using simulated resource metrics")
            metrics.memory_usage = np.random.uniform(40, 80)
            metrics.cpu_utilization = {
                'cloud': np.random.uniform(5, 15),
                'fog': np.random.uniform(8, 18),
                'edge': np.random.uniform(3, 10)
            }
    
    def _collect_blockchain_metrics(self, metrics: ExperimentMetrics):
        """Collect blockchain-specific metrics"""
        if 'consensus_manager' in self.system_components:
            consensus = self.system_components['consensus_manager']
            consensus_metrics = consensus.get_consensus_metrics()
            
            metrics.block_generation_time = consensus_metrics.get('avg_block_time', 0.0)
            metrics.consensus_time = consensus_metrics.get('avg_consensus_time', 0.0)
        
        if 'shard_manager' in self.system_components:
            shard_manager = self.system_components['shard_manager']
            
            # Calculate shard efficiency
            for shard in shard_manager.get_all_shards():
                efficiency = shard.processed_transactions / max(shard.total_capacity, 1)
                metrics.shard_efficiency[shard.shard_id] = efficiency
            
            # Calculate cross-shard communication latency
            metrics.cross_shard_latency = shard_manager.get_cross_shard_latency()
    
    def _collect_tier_metrics(self, metrics: ExperimentMetrics):
        """Collect multi-tier computing metrics"""
        if 'blockchain_manager' in self.system_components:
            blockchain = self.system_components['blockchain_manager']
            
            # Calculate processing ratios
            total_processed = sum(node.processed_transactions for node in blockchain.node_manager.nodes.values())
            
            if total_processed > 0:
                edge_processed = sum(node.processed_transactions for node in blockchain.node_manager.nodes.values() 
                                   if node.tier.value == 'edge')
                fog_processed = sum(node.processed_transactions for node in blockchain.node_manager.nodes.values()
                                  if node.tier.value == 'fog')  
                cloud_processed = sum(node.processed_transactions for node in blockchain.node_manager.nodes.values()
                                    if node.tier.value == 'cloud')
                
                metrics.edge_processing_ratio = (edge_processed / total_processed) * 100
                metrics.fog_processing_ratio = (fog_processed / total_processed) * 100
                metrics.cloud_processing_ratio = (cloud_processed / total_processed) * 100
            
            # Calculate tier communication cost
            metrics.tier_communication_cost = blockchain.slice_manager.get_communication_cost()
    
    def _collect_reputation_metrics(self, metrics: ExperimentMetrics):
        """Collect reputation-related metrics"""
        if 'reputation_manager' in self.system_components:
            reputation_manager = self.system_components['reputation_manager']
            
            # Calculate reputation statistics
            reputation_scores = [node.reputation_score for node in reputation_manager.nodes.values()]
            
            if reputation_scores:
                metrics.average_node_reputation = np.mean(reputation_scores)
                metrics.reputation_variance = np.var(reputation_scores)
            
            # Calculate malicious node detection rate
            detected_malicious = reputation_manager.get_detected_malicious_nodes()
            actual_malicious = reputation_manager.get_actual_malicious_nodes()
            
            if actual_malicious > 0:
                metrics.malicious_node_detection_rate = (detected_malicious / actual_malicious) * 100
    
    def _collect_qos_metrics(self, metrics: ExperimentMetrics):
        """Collect Quality of Service metrics"""
        if 'slice_manager' in self.system_components:
            slice_manager = self.system_components['slice_manager']
            
            # Service availability
            total_slices = len(slice_manager.get_all_slices())
            active_slices = len([s for s in slice_manager.get_all_slices() if s.is_active])
            metrics.service_availability = (active_slices / total_slices * 100) if total_slices > 0 else 0.0
            
            # 99th percentile response time
            response_times = slice_manager.get_all_response_times()
            if response_times:
                metrics.response_time_99p = np.percentile(response_times, 99)
            
            # Bandwidth efficiency
            metrics.bandwidth_efficiency = slice_manager.get_bandwidth_efficiency()
    
    def _store_metrics(self, metrics: ExperimentMetrics):
        """Store metrics in buffer and database"""
        # Add to in-memory buffer
        self.metrics_buffer.append(metrics)
        
        # Store in database
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            # Insert individual metrics
            for field_name, value in asdict(metrics).items():
                if field_name not in ['timestamp', 'experiment_id', 'system_name']:
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            cursor.execute('''
                                INSERT INTO metrics (timestamp, experiment_id, system_name, metric_type, metric_value, additional_data)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (metrics.timestamp, metrics.experiment_id, metrics.system_name, 
                                  f"{field_name}_{sub_key}", float(sub_value), ""))
                    else:
                        cursor.execute('''
                            INSERT INTO metrics (timestamp, experiment_id, system_name, metric_type, metric_value, additional_data)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (metrics.timestamp, metrics.experiment_id, metrics.system_name, 
                              field_name, float(value), ""))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error storing metrics in database: {e}")
    
    def _update_real_time_aggregations(self, metrics: ExperimentMetrics):
        """Update real-time metric aggregations"""
        # Keep sliding window of recent metrics
        window_size = 60  # Last 60 data points
        
        for field_name, value in asdict(metrics).items():
            if isinstance(value, (int, float)) and field_name not in ['timestamp', 'experiment_id']:
                self.real_time_metrics[field_name].append(value)
                
                # Maintain window size
                if len(self.real_time_metrics[field_name]) > window_size:
                    self.real_time_metrics[field_name].pop(0)
    
    def get_real_time_summary(self) -> Dict[str, Any]:
        """Get real-time summary of key metrics"""
        summary = {
            'collection_status': 'active' if self.is_collecting else 'stopped',
            'collection_duration': time.time() - self.start_time if self.start_time else 0,
            'data_points_collected': len(self.metrics_buffer),
            'last_collection': self.last_collection_time
        }
        
        # Add statistical summaries for key metrics
        key_metrics = ['throughput_tps', 'latency_ms', 'success_rate', 'memory_usage']
        
        for metric in key_metrics:
            if metric in self.real_time_metrics and self.real_time_metrics[metric]:
                values = self.real_time_metrics[metric]
                summary[metric] = {
                    'current': values[-1],
                    'average': np.mean(values),
                    'min': np.min(values),
                    'max': np.max(values),
                    'std': np.std(values)
                }
        
        return summary
    
    def stop_collection(self):
        """Stop data collection and save final results"""
        if not self.is_collecting:
            return
        
        self.is_collecting = False
        self.logger.info("Stopping data collection")
        
        # Wait for collection thread to finish
        if self.collection_thread and self.collection_thread.is_alive():
            self.collection_thread.join(timeout=5.0)
        
        # Save final results
        self._save_final_results()
    
    def _save_final_results(self):
        """Save comprehensive final results"""
        try:
            # Save metrics as CSV
            csv_file = self.output_dir / f"experiment_{self.experiment_id}_metrics.csv"
            self._save_metrics_csv(csv_file)
            
            # Save summary statistics
            summary_file = self.output_dir / f"experiment_{self.experiment_id}_summary.json"
            summary = self.get_real_time_summary()
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            # Save detailed analysis
            analysis_file = self.output_dir / f"experiment_{self.experiment_id}_analysis.json"
            analysis = self._generate_detailed_analysis()
            with open(analysis_file, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            
            self.logger.info(f"Final results saved to {self.output_dir}")
            
        except Exception as e:
            self.logger.error(f"Error saving final results: {e}")
    
    def _save_metrics_csv(self, csv_file: Path):
        """Save all collected metrics as CSV"""
        if not self.metrics_buffer:
            return
        
        # Convert metrics to DataFrame
        rows = []
        for metrics in self.metrics_buffer:
            row = asdict(metrics)
            
            # Flatten nested dictionaries
            flattened_row = {}
            for key, value in row.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        flattened_row[f"{key}_{sub_key}"] = sub_value
                else:
                    flattened_row[key] = value
            
            rows.append(flattened_row)
        
        df = pd.DataFrame(rows)
        df.to_csv(csv_file, index=False)
        self.logger.info(f"Metrics saved to {csv_file}")
    
    def _generate_detailed_analysis(self) -> Dict[str, Any]:
        """Generate detailed analysis of collected data"""
        if not self.metrics_buffer:
            return {"error": "No data collected"}
        
        analysis = {
            'experiment_overview': {
                'experiment_id': self.experiment_id,
                'duration_seconds': time.time() - self.start_time if self.start_time else 0,
                'data_points': len(self.metrics_buffer),
                'collection_rate': len(self.metrics_buffer) / max(time.time() - self.start_time, 1) if self.start_time else 0
            }
        }
        
        # Performance analysis
        throughput_values = [m.throughput_tps for m in self.metrics_buffer if m.throughput_tps > 0]
        latency_values = [m.latency_ms for m in self.metrics_buffer if m.latency_ms > 0]
        
        if throughput_values:
            analysis['performance'] = {
                'throughput_stats': {
                    'mean': np.mean(throughput_values),
                    'median': np.median(throughput_values),
                    'std': np.std(throughput_values),
                    'min': np.min(throughput_values),
                    'max': np.max(throughput_values),
                    'p95': np.percentile(throughput_values, 95),
                    'p99': np.percentile(throughput_values, 99)
                }
            }
        
        if latency_values:
            analysis['performance']['latency_stats'] = {
                'mean': np.mean(latency_values),
                'median': np.median(latency_values),
                'std': np.std(latency_values),
                'min': np.min(latency_values),
                'max': np.max(latency_values),
                'p95': np.percentile(latency_values, 95),
                'p99': np.percentile(latency_values, 99)
            }
        
        # Resource utilization trends
        cpu_data = defaultdict(list)
        for metrics in self.metrics_buffer:
            for tier, usage in metrics.cpu_utilization.items():
                cpu_data[tier].append(usage)
        
        if cpu_data:
            analysis['resource_utilization'] = {}
            for tier, values in cpu_data.items():
                analysis['resource_utilization'][f'{tier}_cpu'] = {
                    'mean': np.mean(values),
                    'max': np.max(values),
                    'trend': 'increasing' if values[-1] > values[0] else 'decreasing' if values[-1] < values[0] else 'stable'
                }
        
        return analysis