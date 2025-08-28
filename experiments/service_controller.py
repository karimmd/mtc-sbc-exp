import asyncio
import json
import logging
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import statistics
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
import yaml

# Flask for API endpoints
from flask import Flask, request, jsonify
from flask_cors import CORS

# Fabric SDK for blockchain interaction
from hfc.fabric import Client
from hfc.fabric.peer import create_peer
from hfc.fabric.orderer import create_orderer
from hfc.util.keystore import file_store


@dataclass
class ServiceMetrics:
    service_id: str
    request_count: int
    avg_response_time: float
    throughput: float
    last_updated: datetime
    resource_usage: Dict[str, float]
    geographic_distribution: Dict[str, int]
    

@dataclass
class ShardState:
    shard_id: str
    services: List[str]
    current_load: float
    capacity: float
    peer_nodes: List[str]
    location: str
    last_rebalanced: datetime
    performance_metrics: Dict[str, float]


@dataclass
class ServiceAllocation:
    service_id: str
    assigned_shard: str
    affinity_score: float
    allocation_timestamp: datetime
    expected_load: float


class ServiceAffinityCalculator:
    
    def __init__(self):
        self.service_interactions = {}
        self.geographic_preferences = {}
        self.resource_requirements = {}
        
    def calculate_affinity_score(self, service_id: str, shard_id: str, 
                                shard_state: ShardState) -> float:
        
        # 1. Service interaction frequency (co-location benefit)
        interaction_score = self._calculate_interaction_score(service_id, shard_state.services)
        
        # 2. Geographic locality 
        geographic_score = self._calculate_geographic_score(service_id, shard_state.location)
        
        # 3. Resource compatibility
        resource_score = self._calculate_resource_compatibility(service_id, shard_state)
        
        # 4. Load balancing factor
        load_score = self._calculate_load_score(shard_state)
        
        # Weighted combination 
        affinity_score = (
            0.3 * interaction_score +
            0.25 * geographic_score +
            0.25 * resource_score +
            0.2 * load_score
        )
        
        return min(1.0, max(0.0, affinity_score))
    
    def _calculate_interaction_score(self, service_id: str, existing_services: List[str]) -> float:
        if service_id not in self.service_interactions:
            return 0.0
            
        total_interactions = 0
        for existing_service in existing_services:
            interaction_freq = self.service_interactions[service_id].get(existing_service, 0)
            total_interactions += interaction_freq
            
        # Normalize by maximum possible interactions
        max_interactions = len(existing_services) * 100  # Assume max 100 interactions per pair
        return min(1.0, total_interactions / max_interactions) if max_interactions > 0 else 0.0
    
    def _calculate_geographic_score(self, service_id: str, shard_location: str) -> float:
        if service_id not in self.geographic_preferences:
            return 0.5  # Neutral score for unknown services
            
        preferred_locations = self.geographic_preferences[service_id]
        if shard_location in preferred_locations:
            return preferred_locations[shard_location] / 100.0  # Normalize percentage
        return 0.1  # Low score for non-preferred locations
    
    def _calculate_resource_compatibility(self, service_id: str, shard_state: ShardState) -> float:
        if service_id not in self.resource_requirements:
            return 0.7  # Default compatibility
            
        requirements = self.resource_requirements[service_id]
        available_resources = {
            'cpu': shard_state.capacity - shard_state.current_load,
            'memory': shard_state.performance_metrics.get('available_memory', 50.0),
            'storage': shard_state.performance_metrics.get('available_storage', 50.0)
        }
        
        compatibility = 1.0
        for resource, required in requirements.items():
            available = available_resources.get(resource, 50.0)
            if available < required:
                compatibility *= (available / required)
                
        return compatibility
    
    def _calculate_load_score(self, shard_state: ShardState) -> float:
        # Higher score for less loaded shards (encourage load balancing)
        utilization = shard_state.current_load / shard_state.capacity
        return 1.0 - utilization
    
    def update_service_interactions(self, service1: str, service2: str, frequency: int):
        if service1 not in self.service_interactions:
            self.service_interactions[service1] = {}
        self.service_interactions[service1][service2] = frequency


class ServiceController:
    
    def __init__(self, config_file: str = 'blockchain_config.yaml'):
        self.logger = logging.getLogger('ServiceController')
        self.config = self._load_config(config_file)
        
        # Core components
        self.affinity_calculator = ServiceAffinityCalculator()
        self.service_metrics = {}
        self.shard_states = {}
        self.service_allocations = {}
        self.fabric_client = None
        
        # Threading
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.monitoring_thread = None
        self.rebalancing_thread = None
        
        # API server
        self.app = Flask(__name__)
        CORS(self.app)
        self._setup_api_routes()
        
        # Initialize system
        self._initialize_fabric_connection()
        self._initialize_shard_states()
        
    def _load_config(self, config_file: str) -> Dict:
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Default configuration for Service
            return {
                'shards': [
                    {
                        'id': 'payment_shard',
                        'capacity': 100.0,
                        'location': 'us-east-1',
                        'peers': ['peer0.shardorg.local:7055']
                    },
                    {
                        'id': 'analytics_shard', 
                        'capacity': 150.0,
                        'location': 'us-west-2',
                        'peers': ['peer1.shardorg.local:8055']
                    },
                    {
                        'id': 'iot_shard',
                        'capacity': 120.0,
                        'location': 'eu-west-1',
                        'peers': ['peer2.shardorg.local:9055']
                    }
                ],
                'fabric': {
                    'orderer_endpoint': 'grpc://orderer.local:7050',
                    'channel_name': 'blockchain-channel',
                    'chaincode_name': 'blockchain_sharding'
                },
                'rebalancing_interval': 30,
                'affinity_threshold': 0.6
            }
    
    def _initialize_fabric_connection(self):
        try:
            # Initialize Hyperledger Fabric client
            self.fabric_client = Client(net_profile_path='network-connection.yaml')
            
            # Create orderer and peers from config
            orderer = create_orderer(endpoint=self.config['fabric']['orderer_endpoint'])
            self.fabric_client.orderer = orderer
            
            # Add peer connections
            for shard_config in self.config['shards']:
                for peer_endpoint in shard_config['peers']:
                    peer = create_peer(endpoint=f"grpc://{peer_endpoint}")
                    self.fabric_client.peers.append(peer)
                    
            self.logger.info("Fabric client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Fabric client: {e}")
            # Continue with mock client for development
            self.fabric_client = None
    
    def _initialize_shard_states(self):
        # Initialize shard states from configuration
        current_time = datetime.now()
        
        for shard_config in self.config['shards']:
            self.shard_states[shard_config['id']] = ShardState(
                shard_id=shard_config['id'],
                services=[],
                current_load=0.0,
                capacity=shard_config['capacity'],
                peer_nodes=shard_config['peers'],
                location=shard_config['location'],
                last_rebalanced=current_time,
                performance_metrics={
                    'avg_response_time': 0.0,
                    'throughput': 0.0,
                    'available_memory': 80.0,
                    'available_storage': 90.0
                }
            )
    
    def _setup_api_routes(self):
        @self.app.route('/api/v1/services/register', methods=['POST'])
        def register_service():
            try:
                service_data = request.get_json()
                service_id = service_data['service_id']
                
                # Calculate optimal shard allocation
                allocation = self.allocate_service_to_shard(
                    service_id,
                    service_data.get('resource_requirements', {}),
                    service_data.get('geographic_preference', None)
                )
                
                return jsonify({
                    'success': True,
                    'allocation': asdict(allocation),
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Service registration failed: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/v1/services/<service_id>/metrics', methods=['POST'])
        def update_service_metrics(service_id):
            try:
                metrics_data = request.get_json()
                
                self.service_metrics[service_id] = ServiceMetrics(
                    service_id=service_id,
                    request_count=metrics_data.get('request_count', 0),
                    avg_response_time=metrics_data.get('avg_response_time', 0.0),
                    throughput=metrics_data.get('throughput', 0.0),
                    last_updated=datetime.now(),
                    resource_usage=metrics_data.get('resource_usage', {}),
                    geographic_distribution=metrics_data.get('geographic_distribution', {})
                )
                
                return jsonify({'success': True})
                
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/v1/shards/status', methods=['GET'])
        def get_shard_status():
            try:
                shard_status = {}
                for shard_id, shard_state in self.shard_states.items():
                    shard_status[shard_id] = {
                        'load_percentage': (shard_state.current_load / shard_state.capacity) * 100,
                        'service_count': len(shard_state.services),
                        'performance': shard_state.performance_metrics,
                        'location': shard_state.location
                    }
                
                return jsonify({
                    'success': True,
                    'shard_status': shard_status,
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/v1/rebalance/trigger', methods=['POST'])
        def trigger_rebalancing():
            try:
                rebalancing_results = self.perform_shard_rebalancing()
                
                return jsonify({
                    'success': True,
                    'rebalancing_results': rebalancing_results,
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
    
    def allocate_service_to_shard(self, service_id: str, resource_requirements: Dict,
                                  geographic_preference: Optional[str] = None) -> ServiceAllocation:
        # Update affinity calculator with service requirements
        self.affinity_calculator.resource_requirements[service_id] = resource_requirements
        
        if geographic_preference:
            self.affinity_calculator.geographic_preferences[service_id] = {
                geographic_preference: 80.0,  # High preference for specified location
                'default': 20.0
            }
        
        # Calculate affinity scores for all shards
        shard_scores = {}
        for shard_id, shard_state in self.shard_states.items():
            affinity_score = self.affinity_calculator.calculate_affinity_score(
                service_id, shard_id, shard_state
            )
            shard_scores[shard_id] = affinity_score
        
        # Select shard with highest affinity score
        best_shard = max(shard_scores, key=shard_scores.get)
        best_score = shard_scores[best_shard]
        
        # Create allocation
        allocation = ServiceAllocation(
            service_id=service_id,
            assigned_shard=best_shard,
            affinity_score=best_score,
            allocation_timestamp=datetime.now(),
            expected_load=resource_requirements.get('cpu', 10.0)
        )
        
        # Update shard state
        self.shard_states[best_shard].services.append(service_id)
        self.shard_states[best_shard].current_load += allocation.expected_load
        self.service_allocations[service_id] = allocation
        
        # Record allocation on blockchain
        if self.fabric_client:
            self._record_allocation_on_blockchain(allocation)
        
        self.logger.info(f"Allocated service {service_id} to shard {best_shard} (score: {best_score:.3f})")
        
        return allocation
    
    def perform_shard_rebalancing(self) -> Dict[str, Any]:
        rebalancing_start = time.time()
        moved_services = []
        
        # Identify overloaded and underloaded shards
        overloaded_shards = []
        underloaded_shards = []
        
        for shard_id, shard_state in self.shard_states.items():
            utilization = shard_state.current_load / shard_state.capacity
            
            if utilization > 0.8:  # 80% threshold
                overloaded_shards.append((shard_id, utilization))
            elif utilization < 0.3:  # 30% threshold
                underloaded_shards.append((shard_id, utilization))
        
        # Rebalance services from overloaded to underloaded shards
        for shard_id, utilization in overloaded_shards:
            shard_state = self.shard_states[shard_id]
            
            # Find services that can be moved
            moveable_services = []
            for service_id in shard_state.services:
                if service_id in self.service_allocations:
                    allocation = self.service_allocations[service_id]
                    # Check if service has low affinity or high mobility
                    if allocation.affinity_score < self.config.get('affinity_threshold', 0.6):
                        moveable_services.append((service_id, allocation.expected_load))
            
            # Move services to underloaded shards
            for service_id, service_load in moveable_services:
                if not underloaded_shards:
                    break
                    
                # Find best target shard
                target_shard_id = None
                best_affinity = -1
                
                for target_shard, _ in underloaded_shards:
                    target_state = self.shard_states[target_shard]
                    
                    # Check if target shard can accommodate the service
                    if target_state.current_load + service_load <= target_state.capacity * 0.7:
                        affinity = self.affinity_calculator.calculate_affinity_score(
                            service_id, target_shard, target_state
                        )
                        
                        if affinity > best_affinity:
                            best_affinity = affinity
                            target_shard_id = target_shard
                
                if target_shard_id:
                    # Perform the move
                    self._move_service_between_shards(service_id, shard_id, target_shard_id)
                    moved_services.append({
                        'service_id': service_id,
                        'from_shard': shard_id,
                        'to_shard': target_shard_id,
                        'new_affinity': best_affinity
                    })
        
        # Update last rebalanced timestamp
        for shard_state in self.shard_states.values():
            shard_state.last_rebalanced = datetime.now()
        
        rebalancing_time = time.time() - rebalancing_start
        
        return {
            'services_moved': len(moved_services),
            'moved_services': moved_services,
            'rebalancing_time': rebalancing_time,
            'overloaded_shards_count': len(overloaded_shards),
            'underloaded_shards_count': len(underloaded_shards)
        }
    
    def _move_service_between_shards(self, service_id: str, from_shard: str, to_shard: str):
        # Update shard states
        from_shard_state = self.shard_states[from_shard]
        to_shard_state = self.shard_states[to_shard]
        
        # Remove from source shard
        if service_id in from_shard_state.services:
            from_shard_state.services.remove(service_id)
            
        # Add to target shard
        to_shard_state.services.append(service_id)
        
        # Update load
        if service_id in self.service_allocations:
            service_load = self.service_allocations[service_id].expected_load
            from_shard_state.current_load -= service_load
            to_shard_state.current_load += service_load
            
            # Update allocation record
            self.service_allocations[service_id].assigned_shard = to_shard
            self.service_allocations[service_id].allocation_timestamp = datetime.now()
        
        # Record move on blockchain
        if self.fabric_client:
            self._record_service_move_on_blockchain(service_id, from_shard, to_shard)
    
    def _record_allocation_on_blockchain(self, allocation: ServiceAllocation):
        try:
            # Invoke service chaincode to record allocation
            response = self.fabric_client.chaincode_invoke(
                requestor='admin',
                channel_name=self.config['fabric']['channel_name'],
                peers=['peer0.serviceorg.local'],
                args=[
                    'AllocateService',
                    allocation.service_id,
                    allocation.assigned_shard,
                    str(allocation.affinity_score),
                    allocation.allocation_timestamp.isoformat()
                ],
                cc_name=self.config['fabric']['chaincode_name']
            )
            
            self.logger.info(f"Recorded allocation on blockchain: {response}")
            
        except Exception as e:
            self.logger.error(f"Failed to record allocation on blockchain: {e}")
    
    def _record_service_move_on_blockchain(self, service_id: str, from_shard: str, to_shard: str):
        try:
            # Invoke service chaincode to record service move
            response = self.fabric_client.chaincode_invoke(
                requestor='admin',
                channel_name=self.config['fabric']['channel_name'],
                peers=['peer0.serviceorg.local'],
                args=[
                    'RebalanceService',
                    service_id,
                    from_shard,
                    to_shard,
                    datetime.now().isoformat()
                ],
                cc_name=self.config['fabric']['chaincode_name']
            )
            
            self.logger.info(f"Recorded service move on blockchain: {response}")
            
        except Exception as e:
            self.logger.error(f"Failed to record service move on blockchain: {e}")
    
    def start_background_monitoring(self):
        def monitoring_loop():
            while True:
                try:
                    # Update shard performance metrics
                    self._update_shard_metrics()
                    
                    # Check if rebalancing is needed
                    if self._should_trigger_rebalancing():
                        self.logger.info("Triggering automatic rebalancing")
                        self.perform_shard_rebalancing()
                    
                    time.sleep(self.config.get('monitoring_interval', 10))
                    
                except Exception as e:
                    self.logger.error(f"Error in monitoring loop: {e}")
                    time.sleep(5)
        
        self.monitoring_thread = threading.Thread(target=monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        self.logger.info("Background monitoring started")
    
    def _update_shard_metrics(self):
        for shard_id, shard_state in self.shard_states.items():
            # Aggregate metrics from services in this shard
            response_times = []
            throughputs = []
            
            for service_id in shard_state.services:
                if service_id in self.service_metrics:
                    metrics = self.service_metrics[service_id]
                    response_times.append(metrics.avg_response_time)
                    throughputs.append(metrics.throughput)
            
            # Update shard performance metrics
            if response_times:
                shard_state.performance_metrics['avg_response_time'] = statistics.mean(response_times)
            if throughputs:
                shard_state.performance_metrics['throughput'] = sum(throughputs)
    
    def _should_trigger_rebalancing(self) -> bool:
        # Check if any shard is significantly overloaded
        for shard_state in self.shard_states.values():
            utilization = shard_state.current_load / shard_state.capacity
            
            if utilization > 0.85:  # 85% threshold for automatic rebalancing
                time_since_last_rebalance = (datetime.now() - shard_state.last_rebalanced).total_seconds()
                
                # Only rebalance if enough time has passed since last rebalancing
                if time_since_last_rebalance > self.config.get('rebalancing_interval', 30):
                    return True
        
        return False
    
    def start(self):
        # Start background monitoring
        self.start_background_monitoring()
        
        # Start Flask API server
        self.logger.info("Starting Service Controller API server on port 8080")
        self.app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    controller = ServiceController()
    
    try:
        controller.start()
    except KeyboardInterrupt:
        print("\nService Controller shutting down...")
    except Exception as e:
        print(f"Error starting Service Controller: {e}")


if __name__ == "__main__":
    main()