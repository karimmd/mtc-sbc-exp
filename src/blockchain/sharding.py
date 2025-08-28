"""
Blockchain Sharding Implementation
Implements the sharding mechanism described in the blockchain paper with reputation-based allocation
"""

import asyncio
import hashlib
import random
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict
import logging


class ShardState(Enum):
    """Shard operational states"""
    ACTIVE = "active"
    FORMING = "forming"
    REORGANIZING = "reorganizing"
    INACTIVE = "inactive"


@dataclass
class Transaction:
    """Represents a blockchain transaction"""
    tx_id: str
    timestamp: float
    sender: str
    receiver: str
    amount: float = 0.0
    data: Dict = field(default_factory=dict)
    size: float = 0.0  # KB
    complexity: float = 1.0
    application_type: str = "general"
    priority: str = "medium"
    
    def get_hash(self) -> str:
        """Generate transaction hash for shard assignment"""
        tx_data = f"{self.tx_id}{self.sender}{self.receiver}{self.amount}"
        return hashlib.sha256(tx_data.encode()).hexdigest()


@dataclass
class Block:
    """Represents a blockchain block"""
    block_id: str
    shard_id: str
    timestamp: float
    transactions: List[Transaction] = field(default_factory=list)
    previous_hash: str = ""
    merkle_root: str = ""
    validator_signatures: Dict[str, str] = field(default_factory=dict)
    consensus_achieved: bool = False
    
    def calculate_block_size(self) -> float:
        """Calculate total block size in KB"""
        return sum(tx.size for tx in self.transactions)
    
    def get_block_hash(self) -> str:
        """Generate block hash"""
        block_data = f"{self.block_id}{self.timestamp}{self.previous_hash}{self.merkle_root}"
        return hashlib.sha256(block_data.encode()).hexdigest()


class BFTSmartConsensus:
    """
    Simplified BFT-Smart consensus implementation for simulation
    Based on the BFT-Smart protocol mentioned in the paper
    """
    
    def __init__(self, shard_id: str, fault_tolerance: float = 0.33):
        self.shard_id = shard_id
        self.fault_tolerance = fault_tolerance  # Byzantine fault tolerance (f < n/3)
        self.consensus_round = 0
        
    async def reach_consensus(self, block: Block, validators: List[Any]) -> Tuple[bool, float]:
        """
        Simulate BFT-Smart consensus process
        
        Returns:
            Tuple[bool, float]: (consensus_achieved, consensus_time)
        """
        if not validators:
            return False, 0.0
        
        # Calculate required votes (2f + 1 where f is max Byzantine nodes)
        n = len(validators)
        f = int(n * self.fault_tolerance)
        required_votes = 2 * f + 1
        
        # Simulate consensus rounds
        consensus_time = 0.0
        votes = 0
        
        for validator in validators:
            # Simulate validator behavior based on reputation
            vote_probability = validator.reputation_score * 0.9 + 0.1  # Min 10% chance
            
            if random.random() < vote_probability:
                votes += 1
                # Add validator signature
                block.validator_signatures[validator.node_id] = f"sig_{validator.node_id}_{block.block_id}"
        
        # Check if consensus achieved
        consensus_achieved = votes >= required_votes
        
        # Consensus time based on network conditions and validator performance
        base_time = 0.5  # Base consensus time in seconds
        network_delay = sum(v.base_latency for v in validators) / len(validators) / 1000
        consensus_time = base_time + network_delay
        
        # Add extra time if consensus not achieved (requires additional rounds)
        if not consensus_achieved:
            consensus_time *= 2.0  # Penalty for failed consensus
        
        block.consensus_achieved = consensus_achieved
        self.consensus_round += 1
        
        return consensus_achieved, consensus_time


class Shard:
    """
    Represents a blockchain shard with associated nodes and transactions
    """
    
    def __init__(self, shard_id: str, max_nodes: int = 50):
        self.shard_id = shard_id
        self.max_nodes = max_nodes
        self.nodes: List[Any] = []
        self.state = ShardState.FORMING
        
        # Blockchain state
        self.blockchain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        self.transaction_pool_size = 0
        
        # Performance metrics
        self.processed_transactions = 0
        self.failed_transactions = 0
        self.average_block_time = 0.0
        self.total_throughput = 0.0
        
        # Consensus mechanism
        self.consensus = BFTSmartConsensus(shard_id)
        
        # Reputation tracking
        self.shard_reputation = 0.0
        self.reputation_history = []
        
        self.logger = logging.getLogger(f'Shard-{shard_id}')
    
    def add_node(self, node: Any) -> bool:
        """Add a node to the shard"""
        if len(self.nodes) >= self.max_nodes:
            return False
        
        if node not in self.nodes:
            self.nodes.append(node)
            self.update_shard_reputation()
            
            if len(self.nodes) >= 3:  # Minimum nodes for BFT
                self.state = ShardState.ACTIVE
            
            return True
        return False
    
    def remove_node(self, node: Any) -> bool:
        """Remove a node from the shard"""
        if node in self.nodes:
            self.nodes.remove(node)
            self.update_shard_reputation()
            
            if len(self.nodes) < 3:
                self.state = ShardState.FORMING
            
            return True
        return False
    
    def update_shard_reputation(self):
        """Update shard reputation based on constituent nodes"""
        if not self.nodes:
            self.shard_reputation = 0.0
            return
        
        total_reputation = sum(node.reputation_score for node in self.nodes)
        self.shard_reputation = total_reputation / len(self.nodes)
        
        self.reputation_history.append({
            'timestamp': len(self.reputation_history),
            'reputation': self.shard_reputation,
            'node_count': len(self.nodes)
        })
    
    def can_process_transaction(self, transaction: Transaction) -> bool:
        """Check if shard can process a transaction"""
        return (self.state == ShardState.ACTIVE and 
                len(self.nodes) >= 3 and
                self.transaction_pool_size < 1000)  # Max pool size
    
    async def add_transaction(self, transaction: Transaction) -> bool:
        """Add transaction to pending pool"""
        if not self.can_process_transaction(transaction):
            return False
        
        self.pending_transactions.append(transaction)
        self.transaction_pool_size += 1
        return True
    
    async def process_transaction(self, transaction: Transaction) -> Dict:
        """
        Process a single transaction through the shard
        
        Returns:
            Dict with processing results
        """
        start_time = asyncio.get_event_loop().time()
        
        if not await self.add_transaction(transaction):
            self.failed_transactions += 1
            return {
                'success': False,
                'processing_time': 0.0,
                'shard_id': self.shard_id,
                'nodes_involved': []
            }
        
        # Select nodes for transaction processing based on reputation
        selected_nodes = self._select_processing_nodes(transaction)
        
        # Simulate transaction processing
        processing_success = await self._simulate_transaction_processing(
            transaction, selected_nodes
        )
        
        processing_time = asyncio.get_event_loop().time() - start_time
        
        if processing_success:
            self.processed_transactions += 1
        else:
            self.failed_transactions += 1
        
        return {
            'success': processing_success,
            'processing_time': processing_time,
            'shard_id': self.shard_id,
            'nodes_involved': [node.node_id for node in selected_nodes]
        }
    
    def _select_processing_nodes(self, transaction: Transaction) -> List[Any]:
        """Select nodes for transaction processing based on reputation and capacity"""
        if not self.nodes:
            return []
        
        # Sort nodes by reputation and available capacity
        available_nodes = [node for node in self.nodes 
                          if node.can_process_task(transaction.complexity)]
        
        if not available_nodes:
            # If no nodes have capacity, select best reputation nodes anyway
            available_nodes = sorted(self.nodes, 
                                   key=lambda n: n.reputation_score, 
                                   reverse=True)[:3]
        
        # Select top nodes (at least 3 for BFT, up to 7 for efficiency)
        num_nodes = min(max(3, len(available_nodes)), 7)
        selected_nodes = sorted(available_nodes, 
                              key=lambda n: (n.reputation_score, n.available_capacity),
                              reverse=True)[:num_nodes]
        
        return selected_nodes
    
    async def _simulate_transaction_processing(self, transaction: Transaction, 
                                             nodes: List[Any]) -> bool:
        """Simulate transaction processing by selected nodes"""
        if not nodes:
            return False
        
        # Simulate parallel processing by nodes
        processing_results = []
        for node in nodes:
            success, proc_time = node.process_task({
                'required_capacity': transaction.complexity,
                'complexity': transaction.complexity
            })
            processing_results.append(success)
        
        # Require majority success for transaction to be valid
        successful_nodes = sum(processing_results)
        return successful_nodes >= (len(nodes) // 2 + 1)
    
    async def create_block(self) -> Optional[Block]:
        """Create a new block from pending transactions"""
        if not self.pending_transactions or self.state != ShardState.ACTIVE:
            return None
        
        # Select transactions for block (up to block size limit)
        max_block_size = 1024  # KB
        block_transactions = []
        current_size = 0
        
        for tx in self.pending_transactions[:]:  # Copy to avoid modification during iteration
            if current_size + tx.size <= max_block_size:
                block_transactions.append(tx)
                current_size += tx.size
                self.pending_transactions.remove(tx)
                self.transaction_pool_size -= 1
            
            if len(block_transactions) >= 100:  # Max transactions per block
                break
        
        if not block_transactions:
            return None
        
        # Create block
        block = Block(
            block_id=f"{self.shard_id}_block_{len(self.blockchain)}",
            shard_id=self.shard_id,
            timestamp=asyncio.get_event_loop().time(),
            transactions=block_transactions,
            previous_hash=self.blockchain[-1].get_block_hash() if self.blockchain else "genesis"
        )
        
        # Calculate merkle root (simplified)
        tx_hashes = [tx.get_hash() for tx in block_transactions]
        block.merkle_root = hashlib.sha256(str(tx_hashes).encode()).hexdigest()
        
        return block
    
    async def validate_block(self, block: Block) -> Tuple[bool, float]:
        """Validate block using BFT-Smart consensus"""
        if len(self.nodes) < 3:
            return False, 0.0
        
        # Run consensus algorithm
        consensus_achieved, consensus_time = await self.consensus.reach_consensus(
            block, self.nodes
        )
        
        if consensus_achieved:
            # Add block to blockchain
            self.blockchain.append(block)
            
            # Update performance metrics
            self.update_average_block_time(consensus_time)
            self.total_throughput += len(block.transactions)
            
            self.logger.debug(f"Block {block.block_id} validated and added to blockchain")
        
        return consensus_achieved, consensus_time
    
    def update_average_block_time(self, new_time: float):
        """Update running average of block creation time"""
        if len(self.blockchain) == 1:
            self.average_block_time = new_time
        else:
            # Running average
            n = len(self.blockchain)
            self.average_block_time = ((self.average_block_time * (n - 1)) + new_time) / n
    
    def get_shard_metrics(self) -> Dict:
        """Get comprehensive shard performance metrics"""
        total_transactions = self.processed_transactions + self.failed_transactions
        success_rate = (self.processed_transactions / total_transactions 
                       if total_transactions > 0 else 0.0)
        
        # Calculate throughput (transactions per second)
        if self.average_block_time > 0:
            avg_txs_per_block = np.mean([len(block.transactions) for block in self.blockchain]) if self.blockchain else 0
            throughput = avg_txs_per_block / self.average_block_time if self.average_block_time > 0 else 0
        else:
            throughput = 0.0
        
        return {
            'shard_id': self.shard_id,
            'state': self.state.value,
            'node_count': len(self.nodes),
            'shard_reputation': self.shard_reputation,
            'processed_transactions': self.processed_transactions,
            'failed_transactions': self.failed_transactions,
            'success_rate': success_rate,
            'blockchain_length': len(self.blockchain),
            'pending_transactions': len(self.pending_transactions),
            'average_block_time': self.average_block_time,
            'throughput': throughput,
            'total_throughput': self.total_throughput
        }
    
    def reset_metrics(self):
        """Reset shard performance metrics"""
        self.processed_transactions = 0
        self.failed_transactions = 0
        self.average_block_time = 0.0
        self.total_throughput = 0.0
        self.blockchain.clear()
        self.pending_transactions.clear()
        self.transaction_pool_size = 0
        self.consensus.consensus_round = 0


class ShardManager:
    """
    Manages all shards in the blockchain system
    Implements reputation-based shard formation and cross-shard communication
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.max_shards = config.get('architecture', {}).get('max_shards', 80)
        self.min_nodes_per_shard = 3
        self.max_nodes_per_shard = 50
        
        # Shard storage
        self.shards: Dict[str, Shard] = {}
        self.shard_count = 0
        self.active_shard_count = 0
        
        # Node assignment tracking
        self.node_to_shard: Dict[str, str] = {}
        
        # Performance tracking
        self.cross_shard_transactions = 0
        self.total_system_throughput = 0.0
        
        self.logger = logging.getLogger('ShardManager')
    
    async def initialize_shards(self, nodes: List[Any]):
        """Initialize shards with nodes based on reputation"""
        self.logger.info("Initializing blockchain shards...")
        
        # Start with a reasonable number of shards based on node count
        initial_shard_count = min(self.max_shards, max(1, len(nodes) // 20))
        
        # Create initial shards
        for i in range(initial_shard_count):
            shard_id = f"shard_{i}"
            shard = Shard(shard_id, self.max_nodes_per_shard)
            self.shards[shard_id] = shard
            self.shard_count += 1
        
        # Assign nodes to shards based on reputation and load balancing
        await self._assign_nodes_to_shards(nodes)
        
        self.logger.info(f"Initialized {self.shard_count} shards with {len(nodes)} nodes")
    
    async def _assign_nodes_to_shards(self, nodes: List[Any]):
        """Assign nodes to shards using reputation-based allocation"""
        # Sort nodes by reputation (higher reputation first)
        sorted_nodes = sorted(nodes, key=lambda n: n.reputation_score, reverse=True)
        
        # Distribute nodes across shards to ensure each shard has good reputation
        shard_list = list(self.shards.values())
        
        for i, node in enumerate(sorted_nodes):
            # Select shard with current lowest reputation or fewest nodes
            target_shard = min(shard_list, 
                             key=lambda s: (s.shard_reputation, len(s.nodes)))
            
            if target_shard.add_node(node):
                self.node_to_shard[node.node_id] = target_shard.shard_id
                
                # Update active shard count
                if target_shard.state == ShardState.ACTIVE:
                    self.active_shard_count = sum(1 for s in self.shards.values() 
                                                if s.state == ShardState.ACTIVE)
    
    async def select_shard_for_transaction(self, transaction: Transaction) -> Shard:
        """
        Select appropriate shard for transaction processing
        Uses consistent hashing with reputation weighting
        """
        if not self.shards:
            raise ValueError("No shards available")
        
        # Use transaction hash for consistent shard selection
        tx_hash = transaction.get_hash()
        hash_value = int(tx_hash[:8], 16)  # Use first 8 hex chars
        
        # Get active shards sorted by ID for consistency
        active_shards = [s for s in self.shards.values() if s.state == ShardState.ACTIVE]
        
        if not active_shards:
            # If no active shards, use any available shard
            active_shards = list(self.shards.values())
        
        # Select shard using weighted selection (reputation + hash)
        if len(active_shards) == 1:
            return active_shards[0]
        
        # Weighted selection based on reputation and load
        shard_weights = []
        for shard in active_shards:
            reputation_weight = shard.shard_reputation * 0.7
            load_weight = (1.0 - min(shard.transaction_pool_size / 1000, 1.0)) * 0.3
            total_weight = reputation_weight + load_weight
            shard_weights.append(total_weight)
        
        # Normalize weights
        total_weight = sum(shard_weights)
        if total_weight > 0:
            shard_weights = [w / total_weight for w in shard_weights]
            
            # Select based on hash and weights
            selection_value = (hash_value % 1000) / 1000.0
            cumulative_weight = 0.0
            
            for i, weight in enumerate(shard_weights):
                cumulative_weight += weight
                if selection_value <= cumulative_weight:
                    return active_shards[i]
        
        # Fallback: select based on hash modulo
        shard_index = hash_value % len(active_shards)
        return active_shards[shard_index]
    
    def set_shard_count(self, new_count: int):
        """Dynamically adjust number of shards"""
        if new_count <= 0 or new_count > self.max_shards:
            return
        
        current_count = len(self.shards)
        
        if new_count > current_count:
            # Add new shards
            for i in range(current_count, new_count):
                shard_id = f"shard_{i}"
                shard = Shard(shard_id, self.max_nodes_per_shard)
                self.shards[shard_id] = shard
                self.shard_count += 1
        
        elif new_count < current_count:
            # Remove excess shards (keep the best performing ones)
            shards_to_keep = sorted(self.shards.values(), 
                                  key=lambda s: (s.shard_reputation, s.processed_transactions),
                                  reverse=True)[:new_count]
            
            new_shards = {}
            for shard in shards_to_keep:
                new_shards[shard.shard_id] = shard
            
            self.shards = new_shards
            self.shard_count = new_count
        
        # Update active shard count
        self.active_shard_count = sum(1 for s in self.shards.values() 
                                    if s.state == ShardState.ACTIVE)
    
    def get_all_shards(self) -> List[Shard]:
        """Get all shards"""
        return list(self.shards.values())
    
    def get_active_shards(self) -> List[Shard]:
        """Get only active shards"""
        return [shard for shard in self.shards.values() 
                if shard.state == ShardState.ACTIVE]
    
    def get_shard_by_node(self, node_id: str) -> Optional[Shard]:
        """Get shard containing specific node"""
        shard_id = self.node_to_shard.get(node_id)
        return self.shards.get(shard_id) if shard_id else None
    
    def get_system_metrics(self) -> Dict:
        """Get aggregated system metrics across all shards"""
        total_processed = sum(shard.processed_transactions for shard in self.shards.values())
        total_failed = sum(shard.failed_transactions for shard in self.shards.values())
        total_transactions = total_processed + total_failed
        
        system_throughput = sum(shard.total_throughput for shard in self.shards.values())
        
        avg_reputation = np.mean([shard.shard_reputation for shard in self.shards.values()]) if self.shards else 0.0
        
        return {
            'total_shards': self.shard_count,
            'active_shards': self.active_shard_count,
            'total_processed_transactions': total_processed,
            'total_failed_transactions': total_failed,
            'system_success_rate': total_processed / total_transactions if total_transactions > 0 else 0.0,
            'system_throughput': system_throughput,
            'average_shard_reputation': avg_reputation,
            'cross_shard_transactions': self.cross_shard_transactions
        }
    
    async def reset_shards(self):
        """Reset all shards"""
        for shard in self.shards.values():
            shard.reset_metrics()
        
        self.cross_shard_transactions = 0
        self.total_system_throughput = 0.0