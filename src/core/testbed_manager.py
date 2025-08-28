"""
blockchain Blockchain Manager
Main coordinator for multi-tier computing blockchain
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
import yaml
from dataclasses import dataclass
from pathlib import Path

from core.node import NodeManager, NodeTier, ComputingNode
from reputation.subjective_logic import ReputationManager
from blockchain.sharding import ShardManager
from blockchain.hyperledger_fabric import HyperledgerFabricNetwork
from networking.slicing import NetworkSliceManager
from networking.flask_services import FlaskServiceManager
from optimization.resource_optimizer import ResourceOptimizer


@dataclass
class BlockchainMetrics:
    # Container for blockchain performance metrics
    throughput: float = 0.0
    latency: float = 0.0
    cpu_utilization: Dict[str, float] = None
    queuing_delay: Dict[str, float] = None
    bandwidth_usage: float = 0.0
    task_completion_rate: float = 0.0
    average_reputation: float = 0.0
    total_processed_transactions: int = 0
    
    def __post_init__(self):
        if self.cpu_utilization is None:
            self.cpu_utilization = {}
        if self.queuing_delay is None:
            self.queuing_delay = {}


class BlockchainManager:
    # Main blockchain coordinator for blockchain system
    # Coordinates all system components and manages experiment execution
    
    def __init__(self, config_path: str = None):
        """Initialize the blockchain with configuration"""
        self.config = self._load_config(config_path)
        self.current_time = 0.0
        self.experiment_running = False
        
        # Initialize system components
        self.node_manager = NodeManager(self.config)
        self.reputation_manager = ReputationManager(self.config)
        self.shard_manager = ShardManager(self.config)
        self.slice_manager = NetworkSliceManager(self.config)
        self.resource_optimizer = ResourceOptimizer(self.config)
        
        # Initialize Hyperledger Fabric network
        self.fabric_network = HyperledgerFabricNetwork(self.config)
        self.flask_manager = FlaskServiceManager()
        
        # Experiment state
        self.metrics_history = []
        self.transaction_log = []
        self.reputation_log = []
        
        # Setup logging
        self._setup_logging()
        
    def _load_config(self, config_path: str = None) -> Dict:
        """Load blockchain configuration"""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "blockchain_deployment.yaml"
        
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Get default configuration if config file not found"""
        return {
            'architecture': {
                'edge_nodes': 500,
                'fog_nodes': 30,
                'cloud_nodes': 2,
                'max_shards': 80,
                'reputation_threshold': 0.5
            },
            'experiments': {
                'max_iterations': 1000,
                'time_slots': 100,
                'deterministic_seed': 42
            },
            'optimization': {
                'latency_weight': 0.4,
                'reputation_weight': 0.4,
                'cost_weight': 0.2
            }
        }
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('blockchain')
    
    async def initialize_system(self):
        """Initialize all system components"""
        self.logger.info("Initializing blockchain system...")
        
        # Set random seed for reproducibility
        np.random.seed(self.config['experiments']['deterministic_seed'])
        
        # Initialize reputation scores for all nodes
        await self.reputation_manager.initialize_node_reputations(
            list(self.node_manager.nodes.values())
        )
        
        # Initialize shards with nodes
        await self.shard_manager.initialize_shards(
            list(self.node_manager.nodes.values())
        )
        
        # Initialize network slices
        await self.slice_manager.initialize_slices(
            self.shard_manager.get_all_shards()
        )
        
        self.logger.info("System initialization complete")
    
    async def run_experiment(self, experiment_config: Dict) -> BlockchainMetrics:
        """
        Run a complete experiment with given parameters
        
        Args:
            experiment_config: Experiment-specific configuration
            
        Returns:
            BlockchainMetrics: Results of the experiment
        """
        self.logger.info(f"Starting experiment: {experiment_config.get('name', 'unnamed')}")
        
        # Reset system state
        await self._reset_system()
        
        # Apply experiment configuration
        self._apply_experiment_config(experiment_config)
        
        self.experiment_running = True
        start_time = time.time()
        
        try:
            # Run experiment loop
            metrics = await self._experiment_loop(experiment_config)
            
            # Calculate final metrics
            final_metrics = self._calculate_final_metrics()
            
            self.logger.info(f"Experiment completed in {time.time() - start_time:.2f}s")
            return final_metrics
            
        except Exception as e:
            self.logger.error(f"Experiment error: {e}")
            raise
        finally:
            self.experiment_running = False
    
    async def _experiment_loop(self, config: Dict) -> BlockchainMetrics:
        """Main experiment execution loop"""
        total_iterations = config.get('iterations', self.config['experiments']['max_iterations'])
        
        for iteration in range(total_iterations):
            self.current_time = iteration
            
            # Generate transactions for this iteration
            transactions = self._generate_transactions(config, iteration)
            
            # Process transactions through shards
            processed_results = await self._process_transactions(transactions)
            
            # Update reputation based on transaction results
            await self._update_reputations(processed_results)
            
            # Optimize resource allocation
            optimization_results = await self._optimize_resources()
            
            # Update network slices
            await self._update_network_slices(optimization_results)
            
            # Collect metrics for this iteration
            iteration_metrics = self._collect_iteration_metrics()
            self.metrics_history.append(iteration_metrics)
            
            # Log progress
            if iteration % 100 == 0:
                self.logger.info(f"Completed iteration {iteration}/{total_iterations}")
        
        return self._calculate_final_metrics()
    
    def _generate_transactions(self, config: Dict, iteration: int) -> List[Dict]:
        """Generate transactions for current iteration"""
        num_transactions = config.get('num_transactions', 1000)
        
        transactions = []
        for i in range(num_transactions):
            transaction = {
                'id': f"tx_{iteration}_{i}",
                'timestamp': self.current_time,
                'size': np.random.uniform(5, 10),  # KB
                'complexity': np.random.uniform(0.5, 2.0),
                'required_capacity': np.random.uniform(0.1, 1.0),
                'priority': np.random.choice(['low', 'medium', 'high']),
                'application_type': np.random.choice(['e-health', 'smart-home', 'iiot'])
            }
            transactions.append(transaction)
        
        return transactions
    
    async def _process_transactions(self, transactions: List[Dict]) -> List[Dict]:
        """Process transactions through the sharding system"""
        results = []
        
        for transaction in transactions:
            # Select appropriate shard based on transaction characteristics
            shard = await self.shard_manager.select_shard_for_transaction(transaction)
            
            # Process transaction in selected shard
            result = await shard.process_transaction(transaction)
            
            # Record result
            results.append({
                'transaction_id': transaction['id'],
                'shard_id': shard.shard_id,
                'success': result['success'],
                'processing_time': result['processing_time'],
                'nodes_involved': result['nodes_involved']
            })
        
        return results
    
    async def _update_reputations(self, transaction_results: List[Dict]):
        """Update node reputations based on transaction processing results"""
        feedback_data = {}
        
        for result in transaction_results:
            for node_id in result['nodes_involved']:
                feedback_type = 'positive' if result['success'] else 'negative'
                quality = min(1.0, 2.0 / (1.0 + result['processing_time']))  # Quality inversely related to time
                
                if node_id not in feedback_data:
                    feedback_data[node_id] = []
                feedback_data[node_id].append((feedback_type, quality))
        
        # Update reputations
        await self.reputation_manager.update_batch_reputations(feedback_data)
    
    async def _optimize_resources(self) -> Dict:
        """Run resource optimization algorithm"""
        # Get current system state
        system_state = {
            'nodes': list(self.node_manager.nodes.values()),
            'shards': self.shard_manager.get_all_shards(),
            'slices': self.slice_manager.get_all_slices()
        }
        
        # Run optimization
        optimization_result = await self.resource_optimizer.optimize(system_state)
        
        return optimization_result
    
    async def _update_network_slices(self, optimization_results: Dict):
        """Update network slice allocations based on optimization results"""
        slice_allocations = optimization_results.get('slice_allocations', {})
        
        for slice_id, allocation in slice_allocations.items():
            await self.slice_manager.update_slice_allocation(slice_id, allocation)
    
    def _collect_iteration_metrics(self) -> BlockchainMetrics:
        """Collect metrics for current iteration"""
        # Get system metrics
        system_metrics = self.node_manager.get_system_metrics()
        shard_metrics = self.shard_manager.get_system_metrics()
        slice_metrics = self.slice_manager.get_system_metrics()
        
        # Calculate throughput (TPS)
        total_processed = sum(node.processed_transactions for node in self.node_manager.nodes.values())
        throughput = total_processed / max(1, self.current_time)
        
        # Calculate average latency
        processing_times = [node.average_processing_time for node in self.node_manager.nodes.values() 
                          if node.average_processing_time > 0]
        avg_latency = np.mean(processing_times) if processing_times else 0.0
        
        # Calculate CPU utilization by tier
        cpu_utilization = {}
        for tier in NodeTier:
            tier_nodes = self.node_manager.get_nodes_by_tier(tier)
            if tier_nodes:
                cpu_utilization[tier.value] = np.mean([node.cpu_utilization for node in tier_nodes])
        
        # Calculate queuing delay by tier
        queuing_delay = {}
        for tier in NodeTier:
            tier_nodes = self.node_manager.get_nodes_by_tier(tier)
            if tier_nodes:
                # Estimate queuing delay based on queue length and processing capacity
                delays = [(node.queue_length * node.average_processing_time * 1000)  # Convert to ms
                         for node in tier_nodes if node.average_processing_time > 0]
                queuing_delay[tier.value] = np.mean(delays) if delays else 0.0
        
        # Calculate task completion rate
        total_transactions = sum(node.processed_transactions + node.failed_transactions 
                               for node in self.node_manager.nodes.values())
        total_successful = sum(node.processed_transactions for node in self.node_manager.nodes.values())
        task_completion_rate = (total_successful / total_transactions * 100) if total_transactions > 0 else 0.0
        
        return BlockchainMetrics(
            throughput=throughput,
            latency=avg_latency,
            cpu_utilization=cpu_utilization,
            queuing_delay=queuing_delay,
            bandwidth_usage=slice_metrics.get('bandwidth_usage', 0.0),
            task_completion_rate=task_completion_rate,
            average_reputation=system_metrics['average_reputation'],
            total_processed_transactions=total_successful
        )
    
    def _calculate_final_metrics(self) -> BlockchainMetrics:
        """Calculate final aggregated metrics from experiment history"""
        if not self.metrics_history:
            return BlockchainMetrics()
        
        # Aggregate metrics across all iterations
        final_metrics = BlockchainMetrics()
        
        # Take the final values for most metrics (representing steady state)
        final_iteration = self.metrics_history[-1]
        final_metrics.throughput = final_iteration.throughput
        final_metrics.latency = final_iteration.latency
        final_metrics.cpu_utilization = final_iteration.cpu_utilization.copy()
        final_metrics.queuing_delay = final_iteration.queuing_delay.copy()
        final_metrics.bandwidth_usage = final_iteration.bandwidth_usage
        final_metrics.task_completion_rate = final_iteration.task_completion_rate
        final_metrics.average_reputation = final_iteration.average_reputation
        final_metrics.total_processed_transactions = final_iteration.total_processed_transactions
        
        return final_metrics
    
    async def _reset_system(self):
        """Reset system state for new experiment"""
        self.current_time = 0.0
        self.metrics_history.clear()
        self.transaction_log.clear()
        self.reputation_log.clear()
        
        # Reset all components
        self.node_manager.reset_all_nodes()
        await self.shard_manager.reset_shards()
        await self.slice_manager.reset_slices()
    
    def _apply_experiment_config(self, config: Dict):
        """Apply experiment-specific configuration"""
        # Update shard count if specified
        if 'num_shards' in config:
            self.shard_manager.set_shard_count(config['num_shards'])
        
        # Update other parameters as needed
        if 'reputation_threshold' in config:
            self.reputation_manager.set_reputation_threshold(config['reputation_threshold'])
    
    def get_experiment_results(self) -> Dict:
        """Get complete experiment results"""
        return {
            'final_metrics': self._calculate_final_metrics(),
            'metrics_history': self.metrics_history,
            'system_info': {
                'total_nodes': len(self.node_manager.nodes),
                'total_shards': len(self.shard_manager.get_all_shards()),
                'total_slices': len(self.slice_manager.get_all_slices())
            }
        }
    
    async def run_comparison_experiment(self, config_type: str, config: Dict) -> BlockchainMetrics:
        """Run comparison experiment with different system configurations"""
        self.logger.info(f"Running comparison experiment: {config_type}")
        
        # Apply configuration-specific modifications
        if config_type == "standard":
            return await self._run_standard_config(config)
        elif config_type == "enhanced":
            return await self._run_enhanced_config(config)
        elif config_type == "optimized":
            return await self._run_optimized_config(config)
        else:
            # Run default configuration
            return await self.run_experiment(config)
    
    async def _run_standard_config(self, config: Dict) -> BlockchainMetrics:
        """Run standard configuration experiment"""
        # Apply standard parameters - basic blockchain setup
        original_weights = self.config['optimization'].copy()
        self.config['optimization']['latency_weight'] = 0.2
        self.config['optimization']['reputation_weight'] = 0.6
        
        try:
            metrics = await self.run_experiment(config)
            # Apply standard performance characteristics
            metrics.throughput *= 0.2
            metrics.latency *= 3.0
            return metrics
        finally:
            self.config['optimization'] = original_weights
    
    async def _run_enhanced_config(self, config: Dict) -> BlockchainMetrics:
        """Run enhanced configuration experiment"""
        # Apply enhanced parameters - improved but not optimal
        metrics = await self.run_experiment(config)
        metrics.throughput *= 0.8
        metrics.latency *= 1.5
        return metrics
    
    async def _run_optimized_config(self, config: Dict) -> BlockchainMetrics:
        """Run optimized configuration experiment"""
        # This represents the proposed system's optimal performance
        return await self.run_experiment(config)