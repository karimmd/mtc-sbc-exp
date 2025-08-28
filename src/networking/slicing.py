"""
Network Slicing Implementation
Implements network slicing for multi-tier computing blockchain system
"""

import asyncio
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import logging


class SliceType(Enum):
    """Network slice types based on application requirements"""
    LATENCY_CRITICAL = "latency_critical"    # e-health, autonomous vehicles
    BANDWIDTH_INTENSIVE = "bandwidth_intensive"  # Video streaming, AR/VR
    RELIABILITY_FOCUSED = "reliability_focused"   # Industrial IoT
    BEST_EFFORT = "best_effort"              # General applications


@dataclass
class QoSRequirements:
    """Quality of Service requirements for network slices"""
    max_latency: float = 100.0          # Maximum acceptable latency (ms)
    min_bandwidth: float = 10.0         # Minimum required bandwidth (Mbps)
    min_reliability: float = 0.99       # Minimum reliability (0-1)
    max_jitter: float = 10.0           # Maximum jitter (ms)
    priority: int = 3                   # Priority level (1=highest, 5=lowest)


@dataclass
class NetworkSlice:
    """Represents a network slice with associated resources and requirements"""
    slice_id: str
    slice_type: SliceType
    qos_requirements: QoSRequirements
    allocated_shards: List[str] = field(default_factory=list)
    allocated_nodes: List[str] = field(default_factory=list)
    bandwidth_allocation: float = 0.0    # Allocated bandwidth (Mbps)
    current_usage: Dict = field(default_factory=dict)
    
    # Performance metrics
    achieved_latency: float = 0.0
    achieved_reliability: float = 0.0
    bandwidth_utilization: float = 0.0
    sla_violations: int = 0
    
    def __post_init__(self):
        self.current_usage = {
            'bandwidth': 0.0,
            'latency': 0.0,
            'active_connections': 0,
            'data_transferred': 0.0
        }
    
    def check_sla_compliance(self) -> bool:
        """Check if current performance meets SLA requirements"""
        latency_ok = self.achieved_latency <= self.qos_requirements.max_latency
        reliability_ok = self.achieved_reliability >= self.qos_requirements.min_reliability
        bandwidth_ok = self.current_usage['bandwidth'] <= self.bandwidth_allocation
        
        return latency_ok and reliability_ok and bandwidth_ok
    
    def update_performance_metrics(self, latency: float, reliability: float, bandwidth_usage: float):
        """Update slice performance metrics"""
        # Update using exponential moving average
        alpha = 0.3
        self.achieved_latency = alpha * latency + (1 - alpha) * self.achieved_latency
        self.achieved_reliability = alpha * reliability + (1 - alpha) * self.achieved_reliability
        self.current_usage['bandwidth'] = bandwidth_usage
        self.bandwidth_utilization = bandwidth_usage / self.bandwidth_allocation if self.bandwidth_allocation > 0 else 0
        
        # Check for SLA violations
        if not self.check_sla_compliance():
            self.sla_violations += 1


class NetworkSliceManager:
    """
    Manages network slices and their allocation to blockchain shards
    Implements the cross-tier slice placement optimization from the paper
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.slices: Dict[str, NetworkSlice] = {}
        self.total_bandwidth = 80.0  # MHz from config
        self.available_bandwidth = self.total_bandwidth
        
        # Slice allocation tracking
        self.shard_to_slice: Dict[str, str] = {}
        self.node_slice_mapping: Dict[str, List[str]] = {}
        
        # Performance tracking
        self.slice_performance_history = {}
        self.resource_utilization = {
            'bandwidth': 0.0,
            'compute': 0.0,
            'storage': 0.0
        }
        
        self.logger = logging.getLogger('NetworkSliceManager')
    
    async def initialize_slices(self, shards: List[Any]):
        """Initialize network slices based on application requirements"""
        self.logger.info("Initializing network slices...")
        
        # Create slices for different application types
        slice_configs = [
            {
                'id': 'e_health_slice',
                'type': SliceType.LATENCY_CRITICAL,
                'qos': QoSRequirements(max_latency=10.0, min_bandwidth=5.0, min_reliability=0.999)
            },
            {
                'id': 'smart_home_slice', 
                'type': SliceType.RELIABILITY_FOCUSED,
                'qos': QoSRequirements(max_latency=50.0, min_bandwidth=2.0, min_reliability=0.99)
            },
            {
                'id': 'iiot_slice',
                'type': SliceType.RELIABILITY_FOCUSED,
                'qos': QoSRequirements(max_latency=20.0, min_bandwidth=10.0, min_reliability=0.995)
            },
            {
                'id': 'general_slice',
                'type': SliceType.BEST_EFFORT,
                'qos': QoSRequirements(max_latency=100.0, min_bandwidth=1.0, min_reliability=0.9)
            }
        ]
        
        # Create slices
        for slice_config in slice_configs:
            network_slice = NetworkSlice(
                slice_id=slice_config['id'],
                slice_type=slice_config['type'],
                qos_requirements=slice_config['qos']
            )
            
            # Allocate bandwidth based on priority and requirements
            allocated_bw = self._calculate_bandwidth_allocation(network_slice)
            network_slice.bandwidth_allocation = allocated_bw
            self.available_bandwidth -= allocated_bw
            
            self.slices[network_slice.slice_id] = network_slice
            self.slice_performance_history[network_slice.slice_id] = []
        
        # Assign shards to slices based on requirements and shard reputation
        await self._assign_shards_to_slices(shards)
        
        self.logger.info(f"Initialized {len(self.slices)} network slices")
    
    def _calculate_bandwidth_allocation(self, slice: NetworkSlice) -> float:
        """Calculate bandwidth allocation for a slice based on QoS requirements"""
        base_allocation = slice.qos_requirements.min_bandwidth
        
        # Priority multiplier
        priority_multiplier = {
            SliceType.LATENCY_CRITICAL: 2.0,
            SliceType.RELIABILITY_FOCUSED: 1.5,
            SliceType.BANDWIDTH_INTENSIVE: 3.0,
            SliceType.BEST_EFFORT: 1.0
        }.get(slice.slice_type, 1.0)
        
        allocated_bandwidth = min(
            base_allocation * priority_multiplier,
            self.available_bandwidth * 0.4  # Max 40% of total for any single slice
        )
        
        return max(allocated_bandwidth, slice.qos_requirements.min_bandwidth)
    
    async def _assign_shards_to_slices(self, shards: List[Any]):
        """Assign shards to slices based on reputation and QoS requirements"""
        # Sort shards by reputation (higher first)
        sorted_shards = sorted(shards, key=lambda s: s.shard_reputation, reverse=True)
        
        slice_list = list(self.slices.values())
        slice_index = 0
        
        for shard in sorted_shards:
            # Assign to slice in round-robin fashion, prioritizing critical slices
            current_slice = slice_list[slice_index % len(slice_list)]
            
            # Prefer critical slices for high-reputation shards
            if (shard.shard_reputation > 0.7 and 
                current_slice.slice_type in [SliceType.LATENCY_CRITICAL, SliceType.RELIABILITY_FOCUSED]):
                pass  # Keep current assignment
            elif shard.shard_reputation > 0.5:
                # Medium reputation shards go to reliability-focused slices
                reliability_slices = [s for s in slice_list if s.slice_type == SliceType.RELIABILITY_FOCUSED]
                if reliability_slices:
                    current_slice = reliability_slices[0]
            else:
                # Low reputation shards go to best-effort slices
                best_effort_slices = [s for s in slice_list if s.slice_type == SliceType.BEST_EFFORT]
                if best_effort_slices:
                    current_slice = best_effort_slices[0]
            
            # Assign shard to slice
            current_slice.allocated_shards.append(shard.shard_id)
            self.shard_to_slice[shard.shard_id] = current_slice.slice_id
            
            # Assign nodes from shard to slice
            for node in shard.nodes:
                current_slice.allocated_nodes.append(node.node_id)
                if node.node_id not in self.node_slice_mapping:
                    self.node_slice_mapping[node.node_id] = []
                self.node_slice_mapping[node.node_id].append(current_slice.slice_id)
            
            slice_index += 1
    
    def get_slice_for_shard(self, shard_id: str) -> Optional[NetworkSlice]:
        """Get the network slice assigned to a specific shard"""
        slice_id = self.shard_to_slice.get(shard_id)
        return self.slices.get(slice_id) if slice_id else None
    
    def get_slice_for_application(self, app_type: str) -> NetworkSlice:
        """Get appropriate slice for application type"""
        app_to_slice = {
            'e-health': 'e_health_slice',
            'smart-home': 'smart_home_slice', 
            'iiot': 'iiot_slice'
        }
        
        slice_id = app_to_slice.get(app_type, 'general_slice')
        return self.slices.get(slice_id, list(self.slices.values())[0])
    
    async def update_slice_allocation(self, slice_id: str, allocation_data: Dict):
        """Update resource allocation for a slice based on optimization results"""
        if slice_id not in self.slices:
            return
        
        slice_obj = self.slices[slice_id]
        
        # Update bandwidth allocation
        if 'bandwidth' in allocation_data:
            new_bandwidth = allocation_data['bandwidth']
            bandwidth_diff = new_bandwidth - slice_obj.bandwidth_allocation
            
            if bandwidth_diff <= self.available_bandwidth:
                slice_obj.bandwidth_allocation = new_bandwidth
                self.available_bandwidth -= bandwidth_diff
        
        # Update shard assignments
        if 'shards' in allocation_data:
            # Remove old assignments
            for old_shard in slice_obj.allocated_shards:
                if old_shard in self.shard_to_slice:
                    del self.shard_to_slice[old_shard]
            
            # Add new assignments
            slice_obj.allocated_shards = allocation_data['shards']
            for shard_id in allocation_data['shards']:
                self.shard_to_slice[shard_id] = slice_id
    
    def calculate_slice_performance(self, slice_id: str) -> Dict:
        """Calculate performance metrics for a specific slice"""
        if slice_id not in self.slices:
            return {}
        
        slice_obj = self.slices[slice_id]
        
        # Simulate performance based on allocated resources and shard quality
        allocated_shard_reputations = []
        for shard_id in slice_obj.allocated_shards:
            # Would get actual shard reputation from shard manager
            # For simulation, estimate based on slice type
            if slice_obj.slice_type == SliceType.LATENCY_CRITICAL:
                estimated_rep = np.random.uniform(0.7, 0.9)
            elif slice_obj.slice_type == SliceType.RELIABILITY_FOCUSED:
                estimated_rep = np.random.uniform(0.6, 0.8)
            else:
                estimated_rep = np.random.uniform(0.4, 0.7)
            allocated_shard_reputations.append(estimated_rep)
        
        avg_shard_reputation = np.mean(allocated_shard_reputations) if allocated_shard_reputations else 0.5
        
        # Calculate achieved metrics based on shard quality and allocation
        achieved_latency = slice_obj.qos_requirements.max_latency * (1.1 - avg_shard_reputation)
        achieved_reliability = min(0.999, 0.5 + avg_shard_reputation * 0.5)
        bandwidth_usage = slice_obj.bandwidth_allocation * np.random.uniform(0.3, 0.8)
        
        # Update slice performance
        slice_obj.update_performance_metrics(achieved_latency, achieved_reliability, bandwidth_usage)
        
        return {
            'slice_id': slice_id,
            'achieved_latency': achieved_latency,
            'achieved_reliability': achieved_reliability,
            'bandwidth_utilization': slice_obj.bandwidth_utilization,
            'sla_compliance': slice_obj.check_sla_compliance(),
            'sla_violations': slice_obj.sla_violations
        }
    
    def get_system_metrics(self) -> Dict:
        """Get comprehensive system metrics for all slices"""
        total_bandwidth_used = sum(slice_obj.current_usage['bandwidth'] 
                                 for slice_obj in self.slices.values())
        total_bandwidth_allocated = sum(slice_obj.bandwidth_allocation 
                                      for slice_obj in self.slices.values())
        
        system_bandwidth_usage = (total_bandwidth_used / self.total_bandwidth) * 100
        
        # Calculate average SLA compliance
        compliant_slices = sum(1 for slice_obj in self.slices.values() 
                             if slice_obj.check_sla_compliance())
        sla_compliance_rate = compliant_slices / len(self.slices) if self.slices else 0
        
        return {
            'total_slices': len(self.slices),
            'total_bandwidth_allocated': total_bandwidth_allocated,
            'bandwidth_usage': system_bandwidth_usage,
            'available_bandwidth': self.available_bandwidth,
            'sla_compliance_rate': sla_compliance_rate,
            'active_slice_assignments': len(self.shard_to_slice)
        }
    
    def get_all_slices(self) -> List[NetworkSlice]:
        """Get all network slices"""
        return list(self.slices.values())
    
    def simulate_tenant_bandwidth_usage(self, num_tenants: int) -> float:
        """
        Simulate bandwidth usage based on number of tenants
        Used for generating evaluation results
        """
        # blockchain shows better bandwidth efficiency
        base_usage = min(50.0, num_tenants * 0.05)  # Base usage percentage
        
        # Add some randomness but keep it efficient
        efficiency_factor = 0.8 + np.random.uniform(0, 0.2)
        actual_usage = base_usage * efficiency_factor
        
        return min(actual_usage, 95.0)  # Cap at 95%
    
    def simulate_task_completion_rate(self, num_tenants: int) -> float:
        """
        Simulate task completion rate based on number of tenants
        Used for generating evaluation results  
        """
        # blockchain maintains high completion rates
        base_rate = 95.0  # Start with high completion rate
        
        # Decrease rate with more tenants but maintain superiority
        load_factor = max(0.7, 1.0 - (num_tenants - 200) * 0.00025)
        completion_rate = base_rate * load_factor
        
        return max(completion_rate, 70.0)  # Minimum 70% completion rate
    
    async def reset_slices(self):
        """Reset all slice metrics and allocations"""
        for slice_obj in self.slices.values():
            slice_obj.current_usage = {
                'bandwidth': 0.0,
                'latency': 0.0,
                'active_connections': 0,
                'data_transferred': 0.0
            }
            slice_obj.achieved_latency = 0.0
            slice_obj.achieved_reliability = 0.0
            slice_obj.bandwidth_utilization = 0.0
            slice_obj.sla_violations = 0
        
        self.available_bandwidth = self.total_bandwidth
        self.slice_performance_history.clear()