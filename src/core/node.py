"""
Multi-tier Computing Node Implementation
Represents computing nodes across edge, fog, and cloud tiers
"""

import random
import asyncio
from enum import Enum
from typing import Dict, List, Optional, Tuple
import numpy as np


class NodeTier(Enum):
    """Node tier classification"""
    EDGE = "edge"
    FOG = "fog"
    CLOUD = "cloud"


class NodeState(Enum):
    """Node operational states"""
    ACTIVE = "active"
    BUSY = "busy"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"


class ComputingNode:
    """
    Represents a computing node in the blockchain system
    """
    
    def __init__(self, node_id: str, tier: NodeTier, computing_capacity: float):
        self.node_id = node_id
        self.tier = tier
        self.computing_capacity = computing_capacity  # GHz/core
        self.available_capacity = computing_capacity
        self.state = NodeState.ACTIVE
        
        # Reputation and trust metrics
        self.reputation_score = 0.6  # Initial reputation
        self.positive_feedback = 0
        self.negative_feedback = 0
        self.total_interactions = 0
        
        # Resource tracking
        self.cpu_utilization = 0.0
        self.memory_usage = 0.0
        self.bandwidth_usage = 0.0
        self.queue_length = 0
        self.processing_queue = []
        
        # Performance metrics
        self.processed_transactions = 0
        self.failed_transactions = 0
        self.average_processing_time = 0.0
        
        # Network properties based on tier
        self._initialize_tier_properties()
        
    def _initialize_tier_properties(self):
        """Initialize tier-specific properties"""
        if self.tier == NodeTier.CLOUD:
            self.authority_level = "high"
            self.trust_level = "high"
            self.resource_reliability = 0.95
            self.base_latency = random.uniform(50, 100)  # ms
            
        elif self.tier == NodeTier.FOG:
            self.authority_level = "medium"
            self.trust_level = "medium"
            self.resource_reliability = 0.85
            self.base_latency = random.uniform(20, 50)  # ms
            
        else:  # EDGE
            self.authority_level = "low"
            self.trust_level = "low"
            self.resource_reliability = 0.75
            self.base_latency = random.uniform(5, 20)  # ms
    
    def update_capacity_utilization(self, workload: float) -> float:
        """Update computing capacity utilization"""
        self.available_capacity = max(0, self.computing_capacity - workload)
        self.cpu_utilization = (workload / self.computing_capacity) * 100
        return self.available_capacity
    
    def can_process_task(self, required_capacity: float) -> bool:
        """Check if node can process a task with required capacity"""
        return (self.available_capacity >= required_capacity and 
                self.state == NodeState.ACTIVE)
    
    def add_task_to_queue(self, task: Dict) -> bool:
        """Add task to processing queue"""
        if self.state != NodeState.ACTIVE:
            return False
            
        self.processing_queue.append(task)
        self.queue_length = len(self.processing_queue)
        return True
    
    def process_task(self, task: Dict) -> Tuple[bool, float]:
        """
        Process a task and return success status and processing time
        
        Returns:
            Tuple[bool, float]: (success, processing_time)
        """
        if not self.can_process_task(task.get('required_capacity', 0)):
            self.failed_transactions += 1
            return False, 0.0
        
        # Calculate processing time based on task complexity and node capacity
        base_processing_time = task.get('complexity', 1.0) / self.computing_capacity
        
        # Add network latency based on tier
        processing_time = base_processing_time + (self.base_latency / 1000)  # Convert to seconds
        
        # Add randomness based on resource reliability
        reliability_factor = random.uniform(0.8, 1.2) * self.resource_reliability
        processing_time *= reliability_factor
        
        # Simulate processing success based on node reliability
        success = random.random() < self.resource_reliability
        
        if success:
            self.processed_transactions += 1
            self.update_average_processing_time(processing_time)
        else:
            self.failed_transactions += 1
            
        return success, processing_time
    
    def update_average_processing_time(self, new_time: float):
        """Update running average of processing time"""
        total_processed = self.processed_transactions
        if total_processed == 1:
            self.average_processing_time = new_time
        else:
            self.average_processing_time = (
                (self.average_processing_time * (total_processed - 1) + new_time) 
                / total_processed
            )
    
    def update_reputation(self, feedback: str, interaction_quality: float = 1.0):
        """
        Update node reputation based on feedback
        
        Args:
            feedback: 'positive' or 'negative'
            interaction_quality: Quality weight of the interaction (0-1)
        """
        self.total_interactions += 1
        
        if feedback == 'positive':
            self.positive_feedback += interaction_quality
        elif feedback == 'negative':
            self.negative_feedback += interaction_quality
        
        # Calculate reputation using subjective logic principles
        total_feedback = self.positive_feedback + self.negative_feedback
        if total_feedback > 0:
            belief = self.positive_feedback / total_feedback
            disbelief = self.negative_feedback / total_feedback
            uncertainty = 1 / (1 + total_feedback)  # Decreases with more interactions
            
            # Update reputation score (expected value)
            self.reputation_score = belief + (0.5 * uncertainty)  # Assuming neutral prior
        
        # Ensure reputation stays within bounds
        self.reputation_score = max(0.0, min(1.0, self.reputation_score))
    
    def get_performance_metrics(self) -> Dict:
        """Get comprehensive performance metrics"""
        total_transactions = self.processed_transactions + self.failed_transactions
        success_rate = (
            self.processed_transactions / total_transactions 
            if total_transactions > 0 else 0.0
        )
        
        return {
            'node_id': self.node_id,
            'tier': self.tier.value,
            'reputation_score': self.reputation_score,
            'cpu_utilization': self.cpu_utilization,
            'success_rate': success_rate,
            'average_processing_time': self.average_processing_time,
            'queue_length': self.queue_length,
            'total_transactions': total_transactions,
            'processed_transactions': self.processed_transactions,
            'failed_transactions': self.failed_transactions
        }
    
    def reset_metrics(self):
        """Reset performance metrics for new simulation run"""
        self.processed_transactions = 0
        self.failed_transactions = 0
        self.average_processing_time = 0.0
        self.queue_length = 0
        self.processing_queue.clear()
        self.cpu_utilization = 0.0
        self.available_capacity = self.computing_capacity
    
    def __str__(self) -> str:
        return f"Node({self.node_id}, {self.tier.value}, rep={self.reputation_score:.3f})"
    
    def __repr__(self) -> str:
        return self.__str__()


class NodeManager:
    
    def __init__(self, config: Dict):
        self.config = config
        self.nodes: Dict[str, ComputingNode] = {}
        self.tier_nodes: Dict[NodeTier, List[ComputingNode]] = {
            NodeTier.EDGE: [],
            NodeTier.FOG: [],
            NodeTier.CLOUD: []
        }
        self._initialize_nodes()
    
    def _initialize_nodes(self):
        """Initialize all nodes based on configuration"""
        # Create cloud nodes
        for i in range(self.config['architecture']['cloud_nodes']):
            capacity = random.uniform(4.0, 5.0)  # Cloud nodes have high capacity
            node = ComputingNode(f"cloud_{i}", NodeTier.CLOUD, capacity)
            self._add_node(node)
        
        # Create fog nodes  
        for i in range(self.config['architecture']['fog_nodes']):
            capacity = random.uniform(2.0, 4.0)  # Fog nodes have medium capacity
            node = ComputingNode(f"fog_{i}", NodeTier.FOG, capacity)
            self._add_node(node)
        
        # Create edge nodes
        for i in range(self.config['architecture']['edge_nodes']):
            capacity = random.uniform(1.0, 3.0)  # Edge nodes have lower capacity
            node = ComputingNode(f"edge_{i}", NodeTier.EDGE, capacity)
            self._add_node(node)
    
    def _add_node(self, node: ComputingNode):
        """Add a node to the manager"""
        self.nodes[node.node_id] = node
        self.tier_nodes[node.tier].append(node)
    
    def get_nodes_by_tier(self, tier: NodeTier) -> List[ComputingNode]:
        """Get all nodes from specific tier"""
        return self.tier_nodes[tier].copy()
    
    def get_nodes_by_reputation(self, min_reputation: float = 0.0) -> List[ComputingNode]:
        """Get nodes with reputation above threshold"""
        return [node for node in self.nodes.values() 
                if node.reputation_score >= min_reputation]
    
    def get_available_nodes(self, required_capacity: float = 0.0) -> List[ComputingNode]:
        """Get nodes available for processing"""
        return [node for node in self.nodes.values() 
                if node.can_process_task(required_capacity)]
    
    def update_all_reputations(self, feedback_data: Dict[str, Tuple[str, float]]):
        """Update reputation for multiple nodes"""
        for node_id, (feedback, quality) in feedback_data.items():
            if node_id in self.nodes:
                self.nodes[node_id].update_reputation(feedback, quality)
    
    def get_system_metrics(self) -> Dict:
        """Get aggregated system metrics"""
        metrics = {
            'total_nodes': len(self.nodes),
            'nodes_per_tier': {tier.value: len(nodes) for tier, nodes in self.tier_nodes.items()},
            'average_reputation': np.mean([node.reputation_score for node in self.nodes.values()]),
            'total_capacity': sum(node.computing_capacity for node in self.nodes.values()),
            'available_capacity': sum(node.available_capacity for node in self.nodes.values()),
            'system_utilization': 0.0
        }
        
        if metrics['total_capacity'] > 0:
            metrics['system_utilization'] = (
                (metrics['total_capacity'] - metrics['available_capacity']) 
                / metrics['total_capacity'] * 100
            )
        
        return metrics
    
    def reset_all_nodes(self):
        """Reset all node metrics"""
        for node in self.nodes.values():
            node.reset_metrics()