"""
Subjective Logic-based Reputation System
Implements the reputation mechanism described in the blockchain paper using subjective logic principles
"""

import numpy as np
import asyncio
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import logging


@dataclass
class SubjectiveOpinion:
    """
    Represents a subjective opinion in the reputation system
    Based on the subjective logic model: O = {belief, disbelief, uncertainty}
    """
    belief: float = 0.0        # β: belief in trustworthiness
    disbelief: float = 0.0     # δ: disbelief in trustworthiness  
    uncertainty: float = 1.0   # I: uncertainty/indecision
    
    def __post_init__(self):
        """Ensure opinion components sum to 1.0"""
        total = self.belief + self.disbelief + self.uncertainty
        if abs(total - 1.0) > 1e-10:
            # Normalize if not exactly 1.0
            self.belief /= total
            self.disbelief /= total
            self.uncertainty /= total
    
    @property
    def expectation(self) -> float:
        """Calculate expected reputation value"""
        # Using neutral prior assumption (μ = 0.5)
        return self.belief + (0.5 * self.uncertainty)
    
    def update_with_feedback(self, positive_feedback: float, negative_feedback: float):
        """
        Update opinion based on new feedback
        
        Args:
            positive_feedback: Amount of positive feedback
            negative_feedback: Amount of negative feedback
        """
        total_feedback = positive_feedback + negative_feedback
        
        if total_feedback > 0:
            self.belief = positive_feedback / total_feedback
            self.disbelief = negative_feedback / total_feedback
            # Uncertainty decreases with more evidence
            self.uncertainty = 1.0 / (1.0 + total_feedback)
            
            # Renormalize to ensure sum = 1.0
            self.__post_init__()
    
    def combine_with(self, other: 'SubjectiveOpinion', weight: float = 0.5) -> 'SubjectiveOpinion':
        """
        Combine this opinion with another using consensus operator
        
        Args:
            other: Another subjective opinion
            weight: Weight for combining (0.5 = equal weight)
            
        Returns:
            Combined subjective opinion
        """
        # Consensus operator for combining independent opinions
        new_belief = weight * self.belief + (1 - weight) * other.belief
        new_disbelief = weight * self.disbelief + (1 - weight) * other.disbelief
        new_uncertainty = weight * self.uncertainty + (1 - weight) * other.uncertainty
        
        return SubjectiveOpinion(new_belief, new_disbelief, new_uncertainty)


class PeerInteractionHistory:
    """Tracks interaction history between peers for reputation calculation"""
    
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.positive_interactions = 0.0
        self.negative_interactions = 0.0
        self.interaction_quality_sum = 0.0
        self.total_interactions = 0
        self.interaction_history = []  # Store recent interactions
        self.last_update_time = 0.0
        
    def add_interaction(self, is_positive: bool, quality: float = 1.0, timestamp: float = 0.0):
        """Add new interaction to history"""
        if is_positive:
            self.positive_interactions += quality
        else:
            self.negative_interactions += quality
            
        self.interaction_quality_sum += quality
        self.total_interactions += 1
        
        # Store interaction details
        self.interaction_history.append({
            'timestamp': timestamp,
            'positive': is_positive,
            'quality': quality
        })
        
        # Keep only recent interactions (last 100)
        if len(self.interaction_history) > 100:
            self.interaction_history.pop(0)
        
        self.last_update_time = timestamp
    
    def get_feedback_totals(self) -> Tuple[float, float]:
        """Get total positive and negative feedback"""
        return self.positive_interactions, self.negative_interactions
    
    def get_interaction_reliability(self) -> float:
        """Calculate link reliability based on interaction history"""
        if self.total_interactions == 0:
            return 0.5  # Neutral assumption
        
        success_rate = self.positive_interactions / (
            self.positive_interactions + self.negative_interactions
        )
        
        # Reliability increases with more successful interactions
        confidence = min(1.0, self.total_interactions / 10.0)
        return success_rate * confidence + 0.5 * (1 - confidence)


class ReputationManager:
    """
    Manages reputation system for all nodes using subjective logic
    Implements the reputation model described in the blockchain paper
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.reputation_threshold = config.get('architecture', {}).get('reputation_threshold', 0.5)
        self.penalty_factor = config.get('reputation', {}).get('penalty_factor', 0.1)
        self.indecision_impact = config.get('reputation', {}).get('indecision_impact', 0.5)
        
        # Reputation storage
        self.node_opinions: Dict[str, Dict[str, SubjectiveOpinion]] = defaultdict(dict)
        self.node_reputations: Dict[str, float] = {}
        self.interaction_histories: Dict[Tuple[str, str], PeerInteractionHistory] = {}
        
        # Performance tracking
        self.reputation_evolution = defaultdict(list)
        self.global_reputation_average = 0.0
        
        self.logger = logging.getLogger('ReputationManager')
    
    async def initialize_node_reputations(self, nodes: List[Any]):
        """Initialize reputation scores for all nodes"""
        self.logger.info("Initializing node reputations...")
        
        for node in nodes:
            # Initialize with default reputation based on tier
            if hasattr(node, 'tier'):
                if node.tier.value == 'cloud':
                    initial_rep = 0.8  # Cloud nodes start with high reputation
                elif node.tier.value == 'fog':
                    initial_rep = 0.6  # Fog nodes start with medium reputation
                else:  # edge
                    initial_rep = 0.5  # Edge nodes start with neutral reputation
            else:
                initial_rep = 0.6  # Default initial reputation
            
            self.node_reputations[node.node_id] = initial_rep
            
            # Initialize subjective opinions for this node
            self.node_opinions[node.node_id] = {}
    
    def calculate_node_reputation(self, node_id: str) -> float:
        """
        Calculate overall reputation for a node based on subjective opinions from other nodes
        Implements Equation (2) from the paper
        """
        if node_id not in self.node_opinions:
            return 0.5  # Default neutral reputation
        
        total_reputation = 0.0
        opinion_count = 0
        
        # Aggregate opinions from all other nodes
        for observer_id, opinion in self.node_opinions[node_id].items():
            if observer_id != node_id:  # Don't include self-opinion
                reputation_contribution = opinion.expectation
                total_reputation += reputation_contribution
                opinion_count += 1
        
        if opinion_count > 0:
            average_reputation = total_reputation / opinion_count
        else:
            average_reputation = 0.5  # Default if no opinions
        
        # Store and return reputation
        self.node_reputations[node_id] = average_reputation
        return average_reputation
    
    def update_peer_opinion(self, observer_id: str, target_id: str, 
                          positive_feedback: float, negative_feedback: float,
                          link_quality: float = 1.0):
        """
        Update opinion of observer about target based on interaction
        Implements Equation (1) from the paper
        """
        # Get or create interaction history
        interaction_key = (observer_id, target_id)
        if interaction_key not in self.interaction_histories:
            self.interaction_histories[interaction_key] = PeerInteractionHistory(target_id)
        
        history = self.interaction_histories[interaction_key]
        
        # Update interaction history
        if positive_feedback > negative_feedback:
            history.add_interaction(True, positive_feedback)
        else:
            history.add_interaction(False, negative_feedback)
        
        # Get feedback totals
        pos_feedback, neg_feedback = history.get_feedback_totals()
        
        # Calculate subjective opinion components
        total_feedback = pos_feedback + neg_feedback
        
        if total_feedback > 0:
            # Calculate belief and disbelief (normalized)
            uncertainty = 1.0 - link_quality  # Link quality affects uncertainty
            belief = (1 - uncertainty) * (pos_feedback / total_feedback)
            disbelief = (1 - uncertainty) * (neg_feedback / total_feedback)
        else:
            # No feedback yet - start with neutral opinion
            belief = 0.0
            disbelief = 0.0
            uncertainty = 1.0
        
        # Create/update subjective opinion
        opinion = SubjectiveOpinion(belief, disbelief, uncertainty)
        
        # Store opinion
        if target_id not in self.node_opinions:
            self.node_opinions[target_id] = {}
        self.node_opinions[target_id][observer_id] = opinion
        
        # Recalculate overall reputation for target
        self.calculate_node_reputation(target_id)
    
    async def update_batch_reputations(self, feedback_data: Dict[str, List[Tuple[str, float]]]):
        """
        Update reputations for multiple nodes based on batch feedback
        
        Args:
            feedback_data: Dict mapping node_id to list of (feedback_type, quality) tuples
        """
        for node_id, feedback_list in feedback_data.items():
            pos_feedback = 0.0
            neg_feedback = 0.0
            
            for feedback_type, quality in feedback_list:
                if feedback_type == 'positive':
                    pos_feedback += quality
                else:
                    neg_feedback += quality
            
            # Update opinions from multiple observers (simulate distributed feedback)
            num_observers = min(10, max(3, int(np.sqrt(len(feedback_list)))))
            
            for i in range(num_observers):
                observer_id = f"observer_{i}"
                link_quality = np.random.uniform(0.7, 1.0)  # Simulate link quality
                
                # Distribute feedback among observers
                obs_pos = pos_feedback / num_observers + np.random.uniform(0, 0.1)
                obs_neg = neg_feedback / num_observers + np.random.uniform(0, 0.1)
                
                self.update_peer_opinion(observer_id, node_id, obs_pos, obs_neg, link_quality)
    
    def get_high_reputation_nodes(self, min_reputation: float = None) -> List[str]:
        """
        Get nodes with reputation above threshold
        Implements constraint (C7) from the paper
        """
        if min_reputation is None:
            min_reputation = self.reputation_threshold
        
        high_rep_nodes = []
        for node_id, reputation in self.node_reputations.items():
            if reputation >= min_reputation:
                high_rep_nodes.append(node_id)
        
        return high_rep_nodes
    
    def apply_reputation_penalty(self, node_id: str) -> float:
        """
        Apply penalty to low-reputation nodes
        Implements the penalization mechanism from response to reviewer comments
        """
        current_reputation = self.node_reputations.get(node_id, 0.5)
        
        if current_reputation < self.reputation_threshold:
            penalized_reputation = current_reputation - self.penalty_factor
            self.node_reputations[node_id] = max(0.0, penalized_reputation)
            return self.node_reputations[node_id]
        
        return current_reputation
    
    def calculate_shard_reputation(self, node_ids: List[str]) -> float:
        """
        Calculate average reputation for a shard based on its nodes
        Implements Equation (3) from the paper
        """
        if not node_ids:
            return 0.0
        
        total_reputation = sum(self.node_reputations.get(node_id, 0.5) for node_id in node_ids)
        return total_reputation / len(node_ids)
    
    def calculate_voting_weight(self, node_id: str, shard_node_ids: List[str]) -> float:
        """
        Calculate voting weight for a node within its shard
        Implements Equation (4) from the paper
        """
        node_reputation = self.node_reputations.get(node_id, 0.5)
        
        # Calculate total reputation of all nodes in shard
        total_shard_reputation = sum(
            self.node_reputations.get(nid, 0.5) for nid in shard_node_ids
        )
        
        if total_shard_reputation > 0:
            return node_reputation / total_shard_reputation
        else:
            return 1.0 / len(shard_node_ids)  # Equal weight if no reputation data
    
    def should_exclude_node(self, node_id: str) -> bool:
        """
        Determine if node should be excluded based on reputation
        Implements binary exclusion rule from reviewer response
        """
        reputation = self.node_reputations.get(node_id, 0.5)
        return reputation < self.reputation_threshold
    
    def get_system_reputation_metrics(self) -> Dict:
        """Get comprehensive reputation system metrics"""
        if not self.node_reputations:
            return {'average_reputation': 0.0, 'total_nodes': 0}
        
        reputations = list(self.node_reputations.values())
        
        return {
            'average_reputation': np.mean(reputations),
            'min_reputation': np.min(reputations),
            'max_reputation': np.max(reputations),
            'reputation_std': np.std(reputations),
            'high_reputation_nodes': len(self.get_high_reputation_nodes()),
            'total_nodes': len(self.node_reputations),
            'nodes_above_threshold': sum(1 for rep in reputations if rep >= self.reputation_threshold)
        }
    
    def simulate_peer_behavior(self, node_id: str, behavior_type: str, iteration: int):
        """
        Simulate different peer behaviors for reputation evolution analysis
        Used for generating Figure eval_repu_iteration.pdf data
        """
        if behavior_type == "honest":
            # Honest peer consistently gets positive feedback
            pos_feedback = 1.0 + np.random.uniform(0, 0.2)
            neg_feedback = np.random.uniform(0, 0.1)
            
        elif behavior_type == "malicious":
            # Malicious peer - feedback changes over time
            if iteration < 300:
                # Initially appears trustworthy
                pos_feedback = 0.8 + np.random.uniform(0, 0.2)
                neg_feedback = 0.2 + np.random.uniform(0, 0.1)
            elif iteration < 600:
                # Malicious behavior detected
                pos_feedback = 0.1 + np.random.uniform(0, 0.1)
                neg_feedback = 1.5 + np.random.uniform(0, 0.5)
            else:
                # Continues malicious behavior
                pos_feedback = 0.05
                neg_feedback = 2.0 + np.random.uniform(0, 0.5)
                
        elif behavior_type == "compromised":
            # Compromised peer - temporary drop then recovery
            if iteration < 200:
                # Normal behavior initially
                pos_feedback = 0.8 + np.random.uniform(0, 0.2)
                neg_feedback = 0.3 + np.random.uniform(0, 0.1)
            elif 200 <= iteration < 500:
                # Compromised period
                pos_feedback = 0.2 + np.random.uniform(0, 0.1)
                neg_feedback = 1.0 + np.random.uniform(0, 0.3)
            else:
                # Recovery period
                recovery_factor = (iteration - 500) / 500  # Gradual recovery
                pos_feedback = 0.2 + recovery_factor * 0.8 + np.random.uniform(0, 0.1)
                neg_feedback = 1.0 - recovery_factor * 0.7 + np.random.uniform(0, 0.1)
        
        else:
            # Default behavior
            pos_feedback = 0.6
            neg_feedback = 0.4
        
        # Apply feedback through multiple observers
        num_observers = 5
        for i in range(num_observers):
            observer_id = f"sim_observer_{i}"
            link_quality = np.random.uniform(0.8, 1.0)
            
            self.update_peer_opinion(
                observer_id, node_id, 
                pos_feedback / num_observers, 
                neg_feedback / num_observers, 
                link_quality
            )
        
        # Record reputation evolution
        current_reputation = self.calculate_node_reputation(node_id)
        self.reputation_evolution[node_id].append({
            'iteration': iteration,
            'reputation': current_reputation,
            'behavior_type': behavior_type
        })
        
        return current_reputation
    
    def get_reputation_evolution_data(self) -> Dict[str, List[Dict]]:
        """Get reputation evolution data for plotting"""
        return dict(self.reputation_evolution)
    
    def set_reputation_threshold(self, threshold: float):
        """Set reputation threshold for node selection"""
        self.reputation_threshold = threshold
        self.logger.info(f"Reputation threshold updated to {threshold}")
    
    async def reset_reputations(self):
        """Reset all reputation data"""
        self.node_opinions.clear()
        self.node_reputations.clear()
        self.interaction_histories.clear()
        self.reputation_evolution.clear()
        self.global_reputation_average = 0.0