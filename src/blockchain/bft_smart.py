"""
BFT-Smart Consensus Protocol Implementation for blockchain
Provides high fault tolerance and dynamic reconfiguration of replicas
"""

import asyncio
import time
import json
import logging
import hashlib
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import random
import numpy as np
from collections import defaultdict


class ConsensusPhase(Enum):
    PREPARE = "prepare"
    COMMIT = "commit"
    REPLY = "reply"
    VIEW_CHANGE = "view_change"


@dataclass
class BFTMessage:
    message_type: ConsensusPhase
    sender_id: int
    view: int
    sequence: int
    digest: str
    timestamp: float
    payload: Optional[Dict] = None
    signature: str = ""
    
    def to_dict(self) -> Dict:
        return {
            'type': self.message_type.value,
            'sender': self.sender_id,
            'view': self.view,
            'sequence': self.sequence,
            'digest': self.digest,
            'timestamp': self.timestamp,
            'payload': self.payload,
            'signature': self.signature
        }


@dataclass
class ConsensusState:
    current_view: int = 0
    current_sequence: int = 0
    is_primary: bool = False
    prepared_messages: Dict[int, List[BFTMessage]] = field(default_factory=dict)
    committed_messages: Dict[int, List[BFTMessage]] = field(default_factory=dict)
    pending_requests: List[Dict] = field(default_factory=list)
    last_reply_timestamp: float = 0.0
    view_change_timer: Optional[float] = None
    

@dataclass 
class ReplicaInfo:
    replica_id: int
    node_tier: str
    computing_capacity: float
    reputation_score: float
    is_active: bool = True
    last_heartbeat: float = 0.0
    response_times: List[float] = field(default_factory=list)
    

class BFTSmartConsensus:
    
    def __init__(self, replica_id: int, tier: str, config: Dict):
        self.replica_id = replica_id
        self.tier = tier
        self.config = config
        self.logger = logging.getLogger(f'BFT-Smart-{replica_id}')
        
        # Consensus parameters
        self.f = config.get('fault_tolerance', 1)  # Number of faulty replicas tolerated
        self.total_replicas = 3 * self.f + 1
        self.timeout_duration = config.get('timeout_ms', 5000) / 1000.0  # Convert to seconds
        
        # State management
        self.state = ConsensusState()
        self.replicas: Dict[int, ReplicaInfo] = {}
        self.message_log: List[BFTMessage] = []
        self.executed_sequences: Set[int] = set()
        
        # Performance metrics
        self.consensus_times: List[float] = []
        self.throughput_counter = 0
        self.last_throughput_time = time.time()
        
        # Dynamic reconfiguration support
        self.reconfiguration_threshold = config.get('reconfiguration_threshold', 0.7)
        self.tier_weights = {
            'edge': 1.0,
            'fog': 1.5,
            'cloud': 2.0
        }
        
    async def initialize_consensus_group(self, initial_replicas: List[ReplicaInfo]):
        """Initialize the BFT consensus group with replica information"""
        self.logger.info(f"Initializing BFT consensus group with {len(initial_replicas)} replicas")
        
        for replica in initial_replicas:
            self.replicas[replica.replica_id] = replica
            
        # Determine if this replica is primary
        self.state.is_primary = self._is_primary(self.replica_id, self.state.current_view)
        
        self.logger.info(f"Replica {self.replica_id} initialized as {'PRIMARY' if self.state.is_primary else 'BACKUP'}")
    
    def _is_primary(self, replica_id: int, view: int) -> bool:
        """Determine if a replica is primary for given view"""
        if not self.replicas:
            return False
        active_replicas = [r_id for r_id, r in self.replicas.items() if r.is_active]
        if not active_replicas:
            return False
        primary_id = active_replicas[view % len(active_replicas)]
        return replica_id == primary_id
    
    async def process_client_request(self, request: Dict) -> Dict:
        """
        Process client request through BFT consensus
        Returns result with consensus metadata
        """
        start_time = time.time()
        
        if not self.state.is_primary:
            return {
                'success': False,
                'error': 'Not primary replica',
                'redirect_to': self._get_current_primary()
            }
        
        try:
            # Create consensus request
            consensus_request = {
                'client_id': request.get('client_id', 'unknown'),
                'request_id': request.get('request_id', f"req_{int(time.time()*1000)}"),
                'operation': request.get('operation', 'transaction'),
                'payload': request.get('payload', {}),
                'timestamp': time.time()
            }
            
            # Add to pending requests
            self.state.pending_requests.append(consensus_request)
            
            # Execute consensus protocol
            consensus_result = await self._execute_consensus(consensus_request)
            
            # Record performance metrics
            processing_time = time.time() - start_time
            self.consensus_times.append(processing_time)
            
            return {
                'success': consensus_result.get('success', False),
                'result': consensus_result.get('result', {}),
                'consensus_time': processing_time,
                'sequence': consensus_result.get('sequence', -1),
                'view': self.state.current_view,
                'replicas_involved': len([r for r in self.replicas.values() if r.is_active])
            }
            
        except Exception as e:
            self.logger.error(f"Error processing client request: {e}")
            return {
                'success': False,
                'error': str(e),
                'consensus_time': time.time() - start_time
            }
    
    async def _execute_consensus(self, request: Dict) -> Dict:
        """Execute the three-phase BFT consensus protocol"""
        sequence = self.state.current_sequence
        self.state.current_sequence += 1
        
        # Phase 1: Prepare
        prepare_result = await self._phase_prepare(request, sequence)
        if not prepare_result['success']:
            return prepare_result
            
        # Phase 2: Commit  
        commit_result = await self._phase_commit(request, sequence)
        if not commit_result['success']:
            return commit_result
            
        # Phase 3: Execute and Reply
        reply_result = await self._phase_reply(request, sequence)
        
        return reply_result
    
    async def _phase_prepare(self, request: Dict, sequence: int) -> Dict:
        """Phase 1: Prepare phase of BFT consensus"""
        request_digest = self._compute_digest(request)
        
        prepare_msg = BFTMessage(
            message_type=ConsensusPhase.PREPARE,
            sender_id=self.replica_id,
            view=self.state.current_view,
            sequence=sequence,
            digest=request_digest,
            timestamp=time.time(),
            payload=request,
            signature=self._sign_message(request_digest)
        )
        
        # Broadcast prepare message to all replicas
        responses = await self._broadcast_message(prepare_msg)
        
        # Wait for 2f prepare responses (including self)
        required_responses = 2 * self.f
        valid_responses = 1  # Count self
        
        for response in responses:
            if self._verify_prepare_response(response, request_digest):
                valid_responses += 1
                if valid_responses >= required_responses:
                    break
        
        if valid_responses >= required_responses:
            self.state.prepared_messages[sequence] = [prepare_msg] + responses[:required_responses-1]
            return {'success': True, 'sequence': sequence}
        else:
            return {'success': False, 'error': 'Insufficient prepare responses'}
    
    async def _phase_commit(self, request: Dict, sequence: int) -> Dict:
        """Phase 2: Commit phase of BFT consensus"""
        request_digest = self._compute_digest(request)
        
        commit_msg = BFTMessage(
            message_type=ConsensusPhase.COMMIT,
            sender_id=self.replica_id,
            view=self.state.current_view,
            sequence=sequence,
            digest=request_digest,
            timestamp=time.time()
        )
        
        # Broadcast commit message
        responses = await self._broadcast_message(commit_msg)
        
        # Wait for 2f+1 commit responses (including self)
        required_responses = 2 * self.f + 1
        valid_responses = 1  # Count self
        
        for response in responses:
            if self._verify_commit_response(response, request_digest, sequence):
                valid_responses += 1
                if valid_responses >= required_responses:
                    break
        
        if valid_responses >= required_responses:
            self.state.committed_messages[sequence] = [commit_msg] + responses[:required_responses-1]
            return {'success': True, 'sequence': sequence}
        else:
            return {'success': False, 'error': 'Insufficient commit responses'}
    
    async def _phase_reply(self, request: Dict, sequence: int) -> Dict:
        """Phase 3: Execute request and send reply"""
        try:
            # Execute the actual operation
            execution_result = await self._execute_operation(request)
            
            # Mark sequence as executed
            self.executed_sequences.add(sequence)
            self.state.last_reply_timestamp = time.time()
            
            # Update throughput metrics
            self.throughput_counter += 1
            
            return {
                'success': True,
                'sequence': sequence,
                'result': execution_result
            }
            
        except Exception as e:
            self.logger.error(f"Error executing operation: {e}")
            return {
                'success': False,
                'error': str(e),
                'sequence': sequence
            }
    
    async def _execute_operation(self, request: Dict) -> Dict:
        """Execute the actual blockchain operation"""
        operation = request.get('operation', 'transaction')
        payload = request.get('payload', {})
        
        # Execute operation with tier-specific processing characteristics
        processing_delay = self._calculate_processing_delay(operation, payload)
        await asyncio.sleep(processing_delay)
        
        return {
            'operation': operation,
            'processed_at': time.time(),
            'tier': self.tier,
            'replica_id': self.replica_id,
            'processing_delay': processing_delay,
            'result_hash': self._compute_digest({'operation': operation, 'payload': payload})
        }
    
    def _calculate_processing_delay(self, operation: str, payload: Dict) -> float:
        """Calculate processing delay based on operation complexity and tier capacity"""
        base_delay = 0.001  # 1ms base delay
        
        # Operation complexity factor
        complexity_factors = {
            'transaction': 1.0,
            'smart_contract': 2.0,
            'cross_shard': 3.0,
            'reputation_update': 1.5
        }
        complexity = complexity_factors.get(operation, 1.0)
        
        # Tier capacity factor (edge has less capacity)
        tier_factors = {
            'cloud': 1.0,
            'fog': 1.2,
            'edge': 1.5
        }
        tier_factor = tier_factors.get(self.tier, 1.0)
        
        # Payload size factor
        payload_size = len(str(payload))
        size_factor = 1.0 + (payload_size / 1000.0)
        
        return base_delay * complexity * tier_factor * size_factor
    
    async def _broadcast_message(self, message: BFTMessage) -> List[BFTMessage]:
        """Broadcast message to all active replicas and collect responses"""
        responses = []
        
        # Handle network delays and collect responses from other replicas
        for replica_id, replica_info in self.replicas.items():
            if replica_id != self.replica_id and replica_info.is_active:
                # Network delay based on tier communication characteristics
                delay = self._calculate_network_delay(replica_info.node_tier)
                await asyncio.sleep(delay)
                
                # Collect replica response
                response = await self._collect_replica_response(replica_info, message)
                if response:
                    responses.append(response)
        
        return responses
    
    def _calculate_network_delay(self, target_tier: str) -> float:
        """Calculate network delay between tiers"""
        # Inter-tier communication delays (in seconds)
        delays = {
            ('cloud', 'cloud'): 0.001,
            ('cloud', 'fog'): 0.010,
            ('cloud', 'edge'): 0.050,
            ('fog', 'fog'): 0.005,
            ('fog', 'edge'): 0.020,
            ('edge', 'edge'): 0.002
        }
        
        # Get delay for communication between tiers
        tier_pair = tuple(sorted([self.tier, target_tier]))
        base_delay = delays.get(tier_pair, 0.050)
        
        # Add random jitter
        jitter = random.uniform(0.8, 1.2)
        return base_delay * jitter
    
    async def _collect_replica_response(self, replica: ReplicaInfo, message: BFTMessage) -> Optional[BFTMessage]:
        """Collect response from another replica in the network"""
        # Probability of response based on replica reputation and tier
        response_probability = min(0.95, replica.reputation_score * self.tier_weights.get(replica.node_tier, 1.0))
        
        if random.random() > response_probability:
            return None  # Replica didn't respond
        
        # Create response message
        response = BFTMessage(
            message_type=message.message_type,
            sender_id=replica.replica_id,
            view=message.view,
            sequence=message.sequence,
            digest=message.digest,
            timestamp=time.time(),
            signature=f"sim_sig_{replica.replica_id}"
        )
        
        return response
    
    def _verify_prepare_response(self, response: BFTMessage, expected_digest: str) -> bool:
        """Verify prepare response message"""
        return (response.message_type == ConsensusPhase.PREPARE and
                response.digest == expected_digest and
                response.view == self.state.current_view)
    
    def _verify_commit_response(self, response: BFTMessage, expected_digest: str, sequence: int) -> bool:
        """Verify commit response message"""
        return (response.message_type == ConsensusPhase.COMMIT and
                response.digest == expected_digest and
                response.sequence == sequence and
                response.view == self.state.current_view)
    
    def _compute_digest(self, data: Dict) -> str:
        """Compute cryptographic digest of data"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def _sign_message(self, message_digest: str) -> str:
        """Sign message digest (simplified simulation)"""
        return f"sig_{self.replica_id}_{message_digest[:8]}"
    
    def _get_current_primary(self) -> int:
        """Get current primary replica ID"""
        active_replicas = [r_id for r_id, r in self.replicas.items() if r.is_active]
        if not active_replicas:
            return -1
        return active_replicas[self.state.current_view % len(active_replicas)]
    
    async def handle_replica_failure(self, failed_replica_id: int):
        """Handle replica failure and trigger reconfiguration if needed"""
        if failed_replica_id in self.replicas:
            self.replicas[failed_replica_id].is_active = False
            self.logger.warning(f"Replica {failed_replica_id} marked as failed")
            
            # Check if reconfiguration is needed
            active_count = sum(1 for r in self.replicas.values() if r.is_active)
            if active_count < 2 * self.f + 1:
                await self._trigger_reconfiguration()
    
    async def _trigger_reconfiguration(self):
        """Trigger dynamic reconfiguration to maintain fault tolerance"""
        self.logger.info("Triggering dynamic reconfiguration")
        
        # Find available replacement replicas from other tiers
        available_replicas = self._find_available_replicas()
        
        for replica_info in available_replicas:
            if len([r for r in self.replicas.values() if r.is_active]) >= self.total_replicas:
                break
                
            self.replicas[replica_info.replica_id] = replica_info
            self.logger.info(f"Added replica {replica_info.replica_id} from {replica_info.node_tier} tier")
        
        # Update view to reflect reconfiguration
        self.state.current_view += 1
        self.state.is_primary = self._is_primary(self.replica_id, self.state.current_view)
    
    def _find_available_replicas(self) -> List[ReplicaInfo]:
        """Find available replicas for reconfiguration"""
        # Find available replicas across tiers based on reputation and capacity
        available = []
        
        for tier in ['cloud', 'fog', 'edge']:
            if len(available) >= 3:  # Limit new replicas
                break
                
            replica_id = len(self.replicas) + len(available) + 1000
            replica = ReplicaInfo(
                replica_id=replica_id,
                node_tier=tier,
                computing_capacity=random.uniform(1.0, 5.0),
                reputation_score=random.uniform(0.7, 1.0),
                is_active=True,
                last_heartbeat=time.time()
            )
            available.append(replica)
        
        return available
    
    def get_consensus_metrics(self) -> Dict:
        """Get consensus performance metrics"""
        active_replicas = sum(1 for r in self.replicas.values() if r.is_active)
        
        # Calculate average consensus time
        avg_consensus_time = np.mean(self.consensus_times) if self.consensus_times else 0.0
        
        # Calculate throughput (transactions per second)
        current_time = time.time()
        time_elapsed = current_time - self.last_throughput_time
        throughput = self.throughput_counter / max(time_elapsed, 1.0)
        
        return {
            'replica_id': self.replica_id,
            'tier': self.tier,
            'is_primary': self.state.is_primary,
            'current_view': self.state.current_view,
            'active_replicas': active_replicas,
            'total_replicas': len(self.replicas),
            'executed_sequences': len(self.executed_sequences),
            'avg_consensus_time': avg_consensus_time,
            'throughput_tps': throughput,
            'fault_tolerance': self.f,
            'last_reply_time': self.state.last_reply_timestamp
        }
    
    def reset_metrics(self):
        """Reset performance metrics"""
        self.consensus_times.clear()
        self.throughput_counter = 0
        self.last_throughput_time = time.time()