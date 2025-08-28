"""
Resource Optimization Implementation
Implements the MINLP optimization problem using ADMM (Alternating Direction Method of Multipliers)
Based on the reputation-aware ADMM algorithm from the blockchain paper
"""

import numpy as np
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging
from scipy.optimize import minimize
from collections import defaultdict
import time


@dataclass
class OptimizationResult:
    """Container for optimization results"""
    objective_value: float = 0.0
    slice_allocations: Dict[str, Dict] = None
    resource_allocations: Dict[str, float] = None
    convergence_achieved: bool = False
    iterations: int = 0
    
    def __post_init__(self):
        if self.slice_allocations is None:
            self.slice_allocations = {}
        if self.resource_allocations is None:
            self.resource_allocations = {}


class LagrangianOptimizer:
    """
    Implements Lagrangian dual decomposition for the MINLP problem
    Based on the optimization formulation in the paper
    """
    
    def __init__(self, weights: Dict[str, float]):
        self.w1 = weights.get('latency_weight', 0.4)      # Latency weight
        self.w2 = weights.get('reputation_weight', 0.4)   # Reputation weight  
        self.w3 = weights.get('cost_weight', 0.2)         # Cost weight
        
        # Lagrange multipliers
        self.lambda_bandwidth = defaultdict(float)  # Bandwidth constraints
        self.lambda_compute = defaultdict(float)    # Compute constraints
        self.lambda_reputation = defaultdict(float) # Reputation constraints
        
        # Step sizes for iterative updates
        self.alpha = 0.01  # Initial step size
        self.max_iterations = 100
        self.convergence_threshold = 1e-6
        
        self.logger = logging.getLogger('LagrangianOptimizer')
    
    def calculate_objective_function(self, system_state: Dict, 
                                   allocations: Dict) -> float:
        """
        Calculate the objective function value
        Implements F0 from the paper: minimize (w1 * delay - w2 * reputation + w3 * cost)
        """
        total_delay = 0.0
        total_reputation = 0.0
        total_cost = 0.0
        
        # Calculate delay component
        for slice_id, allocation in allocations.items():
            processing_delay = allocation.get('processing_delay', 0.0)
            completion_delay = allocation.get('completion_delay', 0.0)
            block_delay = allocation.get('block_verification_delay', 0.0)
            
            total_delay += processing_delay + completion_delay + block_delay
        
        # Calculate reputation component (sum of shard reputations)
        for shard in system_state.get('shards', []):
            total_reputation += shard.shard_reputation
        
        # Calculate cost component (resource usage costs)
        for node in system_state.get('nodes', []):
            compute_usage = node.computing_capacity - node.available_capacity
            total_cost += compute_usage * 0.1  # Cost per unit of compute
        
        # Combined objective (minimize)
        objective = self.w1 * total_delay - self.w2 * total_reputation + self.w3 * total_cost
        
        return objective
    
    def update_lagrange_multipliers(self, constraint_violations: Dict) -> float:
        """
        Update Lagrange multipliers using iterative ascent
        Implements the iterative update equations from the paper
        """
        max_violation = 0.0
        
        # Update bandwidth constraint multipliers
        for link_id, violation in constraint_violations.get('bandwidth', {}).items():
            old_lambda = self.lambda_bandwidth[link_id]
            self.lambda_bandwidth[link_id] = max(0, old_lambda + self.alpha * violation)
            max_violation = max(max_violation, abs(violation))
        
        # Update compute constraint multipliers
        for node_id, violation in constraint_violations.get('compute', {}).items():
            old_lambda = self.lambda_compute[node_id]
            self.lambda_compute[node_id] = max(0, old_lambda + self.alpha * violation)
            max_violation = max(max_violation, abs(violation))
        
        # Update reputation constraint multipliers
        for node_id, violation in constraint_violations.get('reputation', {}).items():
            old_lambda = self.lambda_reputation[node_id]
            self.lambda_reputation[node_id] = max(0, old_lambda + self.alpha * violation)
            max_violation = max(max_violation, abs(violation))
        
        return max_violation
    
    def calculate_constraint_violations(self, system_state: Dict, 
                                      allocations: Dict) -> Dict:
        """Calculate constraint violations for Lagrange multiplier updates"""
        violations = {
            'bandwidth': {},
            'compute': {},
            'reputation': {}
        }
        
        # Check bandwidth constraints (C1, C2)
        link_usage = defaultdict(float)
        for slice_id, allocation in allocations.items():
            for link_id, usage in allocation.get('link_usage', {}).items():
                link_usage[link_id] += usage
        
        for link_id, usage in link_usage.items():
            # Assume max capacity per link
            max_capacity = 80.0  # MHz from config
            violation = usage - max_capacity
            if violation > 0:
                violations['bandwidth'][link_id] = violation
        
        # Check compute constraints (C3)
        for node in system_state.get('nodes', []):
            required_capacity = node.computing_capacity - node.available_capacity
            if required_capacity > node.computing_capacity:
                violations['compute'][node.node_id] = required_capacity - node.computing_capacity
        
        # Check reputation constraints (C7)
        average_reputation = np.mean([node.reputation_score 
                                    for node in system_state.get('nodes', [])]) if system_state.get('nodes') else 0.5
        
        for node in system_state.get('nodes', []):
            if node.reputation_score < average_reputation:
                violations['reputation'][node.node_id] = average_reputation - node.reputation_score
        
        return violations
    
    async def optimize(self, system_state: Dict) -> OptimizationResult:
        """
        Run the optimization algorithm using Lagrangian dual decomposition
        """
        self.logger.info("Starting Lagrangian optimization...")
        
        # Initialize allocations
        current_allocations = self._initialize_allocations(system_state)
        best_objective = float('inf')
        best_allocations = current_allocations.copy()
        
        for iteration in range(self.max_iterations):
            # Calculate current objective
            current_objective = self.calculate_objective_function(system_state, current_allocations)
            
            # Calculate constraint violations
            violations = self.calculate_constraint_violations(system_state, current_allocations)
            
            # Update Lagrange multipliers
            max_violation = self.update_lagrange_multipliers(violations)
            
            # Update allocations based on dual variables
            current_allocations = self._update_allocations(system_state, current_allocations)
            
            # Check for improvement
            if current_objective < best_objective:
                best_objective = current_objective
                best_allocations = current_allocations.copy()
            
            # Check convergence
            if max_violation < self.convergence_threshold:
                self.logger.info(f"Optimization converged after {iteration + 1} iterations")
                return OptimizationResult(
                    objective_value=best_objective,
                    slice_allocations=self._convert_to_slice_allocations(best_allocations),
                    resource_allocations=self._convert_to_resource_allocations(best_allocations),
                    convergence_achieved=True,
                    iterations=iteration + 1
                )
            
            # Adapt step size
            if iteration > 10 and iteration % 10 == 0:
                self.alpha *= 0.95  # Decrease step size for better convergence
        
        self.logger.warning(f"Optimization did not converge after {self.max_iterations} iterations")
        return OptimizationResult(
            objective_value=best_objective,
            slice_allocations=self._convert_to_slice_allocations(best_allocations),
            resource_allocations=self._convert_to_resource_allocations(best_allocations),
            convergence_achieved=False,
            iterations=self.max_iterations
        )
    
    def _initialize_allocations(self, system_state: Dict) -> Dict:
        """Initialize allocation variables"""
        allocations = {}
        
        for i, shard in enumerate(system_state.get('shards', [])):
            allocations[shard.shard_id] = {
                'data_rate': 10.0 + np.random.uniform(0, 5),  # r_j
                'compute_resources': 1.0 + np.random.uniform(0, 1),  # c_j
                'processing_delay': 0.0,
                'completion_delay': 0.0,
                'block_verification_delay': 0.0,
                'link_usage': {f'link_{i}': np.random.uniform(5, 15)},
                'assigned_nodes': [node.node_id for node in shard.nodes[:3]]  # Select top 3 nodes
            }
        
        return allocations
    
    def _update_allocations(self, system_state: Dict, current_allocations: Dict) -> Dict:
        """Update allocations based on current dual variables"""
        updated_allocations = current_allocations.copy()
        
        for shard_id, allocation in updated_allocations.items():
            # Update data rates based on bandwidth multipliers
            total_lambda_bw = sum(self.lambda_bandwidth.values())
            if total_lambda_bw > 0:
                # Adjust data rate inversely with congestion
                adjustment_factor = 1.0 / (1.0 + total_lambda_bw * 0.1)
                allocation['data_rate'] *= adjustment_factor
            
            # Update compute resource allocation
            total_lambda_compute = sum(self.lambda_compute.values())
            if total_lambda_compute > 0:
                adjustment_factor = 1.0 / (1.0 + total_lambda_compute * 0.1)
                allocation['compute_resources'] *= adjustment_factor
            
            # Recalculate delays based on new allocations
            allocation['processing_delay'] = self._calculate_processing_delay(allocation)
            allocation['completion_delay'] = self._calculate_completion_delay(allocation)
            allocation['block_verification_delay'] = self._calculate_block_verification_delay(allocation)
        
        return updated_allocations
    
    def _calculate_processing_delay(self, allocation: Dict) -> float:
        """Calculate processing delay D_j,k^proc"""
        queue_length = 5.0  # Assume average queue length
        compute_capacity = allocation['compute_resources']
        
        if compute_capacity > 0:
            return (queue_length + allocation['compute_resources']) / compute_capacity
        else:
            return 100.0  # High penalty for no compute resources
    
    def _calculate_completion_delay(self, allocation: Dict) -> float:
        """Calculate service completion delay D_j,k^comp"""
        processing_delay = allocation.get('processing_delay', 0.0)
        data_size = 100.0  # Assume fixed data size (KB)
        data_rate = allocation['data_rate']
        
        transmission_delay = data_size / data_rate if data_rate > 0 else 100.0
        return processing_delay + transmission_delay
    
    def _calculate_block_verification_delay(self, allocation: Dict) -> float:
        """Calculate block verification delay D_j,k^block"""
        block_size = 512.0  # KB
        data_rate = allocation['data_rate']
        compute_resources = allocation['compute_resources']
        
        verification_time = (block_size / data_rate) + (1.0 / compute_resources) if data_rate > 0 and compute_resources > 0 else 50.0
        return verification_time
    
    def _convert_to_slice_allocations(self, allocations: Dict) -> Dict[str, Dict]:
        """Convert internal allocations to slice allocation format"""
        slice_allocations = {}
        
        # Group allocations by slice (simplified mapping)
        slice_mapping = {
            'shard_0': 'e_health_slice',
            'shard_1': 'smart_home_slice',
            'shard_2': 'iiot_slice'
        }
        
        for shard_id, allocation in allocations.items():
            slice_id = slice_mapping.get(shard_id, 'general_slice')
            
            if slice_id not in slice_allocations:
                slice_allocations[slice_id] = {
                    'bandwidth': 0.0,
                    'shards': [],
                    'nodes': [],
                    'total_delay': 0.0
                }
            
            slice_allocations[slice_id]['bandwidth'] += allocation['data_rate']
            slice_allocations[slice_id]['shards'].append(shard_id)
            slice_allocations[slice_id]['nodes'].extend(allocation.get('assigned_nodes', []))
            slice_allocations[slice_id]['total_delay'] += (
                allocation.get('processing_delay', 0) +
                allocation.get('completion_delay', 0) +
                allocation.get('block_verification_delay', 0)
            )
        
        return slice_allocations
    
    def _convert_to_resource_allocations(self, allocations: Dict) -> Dict[str, float]:
        """Convert to resource allocation format"""
        resource_allocations = {}
        
        for shard_id, allocation in allocations.items():
            resource_allocations[f"{shard_id}_bandwidth"] = allocation['data_rate']
            resource_allocations[f"{shard_id}_compute"] = allocation['compute_resources']
        
        return resource_allocations


class ADMMOptimizer:
    """
    ADMM (Alternating Direction Method of Multipliers) implementation
    Based on the reputation-aware ADMM algorithm from Algorithm 1 in the paper
    """
    
    def __init__(self, weights: Dict[str, float]):
        self.w1 = weights.get('latency_weight', 0.4)      # Latency weight
        self.w2 = weights.get('reputation_weight', 0.4)   # Reputation weight  
        self.w3 = weights.get('cost_weight', 0.2)         # Cost weight
        
        # ADMM parameters
        self.rho = 1.0  # Penalty parameter
        self.tolerance = 1e-4
        self.max_iterations = 1000
        self.beta = 0.5  # Reputation bias parameter
        
        # ADMM variables
        self.x_primal = {}  # Primal variables (r, c, chi, gamma)
        self.z_auxiliary = {}  # Auxiliary variables
        self.u_dual = {}  # Scaled dual variables
        
        self.logger = logging.getLogger('ADMMOptimizer')
    
    async def optimize(self, system_state: Dict) -> OptimizationResult:
        """
        Execute the reputation-aware ADMM algorithm from Algorithm 1
        """
        self.logger.info("Starting ADMM optimization...")
        start_time = time.time()
        
        # Initialize variables (Line 1-2 in Algorithm 1)
        self._initialize_admm_variables(system_state)
        
        for iteration in range(self.max_iterations):
            # Step 1: Projection onto constraint set (Lines 4-11)
            self._projection_stage(system_state)
            
            # Step 2: Proximal gradient update (Line 13) 
            self._proximal_update_stage()
            
            # Step 3: Dual variable update (Line 15)
            self._dual_update_stage()
            
            # Compute residuals (Lines 17-18)
            primal_residual, dual_residual = self._compute_residuals()
            
            # Adaptive penalty parameter (Lines 19-23)
            self._adaptive_penalty_update(primal_residual, dual_residual)
            
            # Check convergence
            if self._check_convergence(primal_residual, dual_residual):
                self.logger.info(f"ADMM converged after {iteration + 1} iterations")
                
                # Binary recovery via minimum-cost assignment (Lines 25-29)
                binary_assignments = self._reputation_guided_binary_recovery()
                
                # Resource optimization with fixed assignments (Line 31)
                final_result = self._optimize_resources_with_assignments(binary_assignments)
                
                return OptimizationResult(
                    objective_value=final_result['objective'],
                    slice_allocations=final_result['slice_allocations'],
                    resource_allocations=final_result['resource_allocations'],
                    convergence_achieved=True,
                    iterations=iteration + 1
                )
        
        self.logger.warning(f"ADMM did not converge after {self.max_iterations} iterations")
        binary_assignments = self._reputation_guided_binary_recovery()
        final_result = self._optimize_resources_with_assignments(binary_assignments)
        
        return OptimizationResult(
            objective_value=final_result['objective'],
            slice_allocations=final_result['slice_allocations'],
            resource_allocations=final_result['resource_allocations'],
            convergence_achieved=False,
            iterations=self.max_iterations
        )
    
    def _initialize_admm_variables(self, system_state: Dict):
        """Initialize ADMM variables (Algorithm 1, Lines 1-2)"""
        num_services = len(system_state.get('shards', []))
        num_nodes = len(system_state.get('nodes', []))
        num_shards = len(system_state.get('shards', []))
        
        # Initialize primal variables
        self.x_primal = {
            'r': np.random.uniform(5, 15, num_services),  # Data rates
            'c': np.random.uniform(0.5, 2.0, num_services),  # Compute resources
            'chi': np.random.uniform(0, 1, (num_nodes, num_shards)),  # Shard assignments
            'gamma': np.random.uniform(0, 10, 1)[0]  # Epigraph variable
        }
        
        # Initialize auxiliary variables
        self.z_auxiliary = {
            'r': np.copy(self.x_primal['r']),
            'c': np.copy(self.x_primal['c']), 
            'chi': np.copy(self.x_primal['chi']),
            'gamma': self.x_primal['gamma']
        }
        
        # Initialize dual variables
        self.u_dual = {
            'r': np.zeros_like(self.x_primal['r']),
            'c': np.zeros_like(self.x_primal['c']),
            'chi': np.zeros_like(self.x_primal['chi']),
            'gamma': 0.0
        }
    
    def _projection_stage(self, system_state: Dict):
        """Projection stage (Algorithm 1, Lines 4-11)"""
        # Resource allocation projection (Lines 4-8)
        self._project_resource_variables(system_state)
        
        # Shard assignment projection (Lines 9-10)
        self._project_shard_assignments()
        
        # Epigraph variable update (Line 11)
        self._update_epigraph_variable()
    
    def _project_resource_variables(self, system_state: Dict):
        """Project resource allocation variables onto constraint set"""
        # Project data rates (r_j >= 0, bandwidth constraints)
        for j in range(len(self.x_primal['r'])):
            target = self.z_auxiliary['r'][j] - self.u_dual['r'][j]
            # Apply constraints (C1, C2, C5)
            self.x_primal['r'][j] = max(0, min(target, 20.0))  # Max 20 for bandwidth limit
        
        # Project compute resources (c_j >= 0, capacity constraints) 
        for j in range(len(self.x_primal['c'])):
            target = self.z_auxiliary['c'][j] - self.u_dual['c'][j]
            # Apply constraints (C3, C4, C5)
            self.x_primal['c'][j] = max(0, min(target, 5.0))  # Max 5 GHz capacity
    
    def _project_shard_assignments(self):
        """Project shard assignments onto probability simplex (Lines 9-10)"""
        for n in range(self.x_primal['chi'].shape[0]):
            target_row = self.z_auxiliary['chi'][n, :] - self.u_dual['chi'][n, :]
            # Project onto simplex: sum to 1, values in [0,1]
            self.x_primal['chi'][n, :] = self._project_onto_simplex(target_row)
    
    def _project_onto_simplex(self, vector: np.ndarray) -> np.ndarray:
        """Project vector onto probability simplex"""
        # Euclidean projection onto probability simplex
        sorted_v = np.sort(vector)[::-1]
        cumsum_v = np.cumsum(sorted_v)
        
        rho_vals = sorted_v + (1.0 - cumsum_v) / np.arange(1, len(vector) + 1)
        rho = np.max(np.where(rho_vals > 0, rho_vals, 0))
        
        projected = np.maximum(vector - rho, 0)
        return projected / max(np.sum(projected), 1e-10)  # Normalize
    
    def _update_epigraph_variable(self):
        """Update epigraph variable (Line 11)"""
        # Compute current objective value
        current_objective = self._compute_objective_value()
        target = self.z_auxiliary['gamma'] - self.u_dual['gamma']
        
        # Epigraph constraint: gamma >= F0
        self.x_primal['gamma'] = max(0, max(current_objective, target))
    
    def _proximal_update_stage(self):
        """Proximal gradient update stage (Algorithm 1, Line 13)"""
        # Compute gradients of F1 
        gradients = self._compute_objective_gradients()
        
        # Proximal update: z = (x + u) - (1/rho) * grad_F1
        self.z_auxiliary['r'] = (self.x_primal['r'] + self.u_dual['r']) - (1.0/self.rho) * gradients['r']
        self.z_auxiliary['c'] = (self.x_primal['c'] + self.u_dual['c']) - (1.0/self.rho) * gradients['c']
        self.z_auxiliary['chi'] = (self.x_primal['chi'] + self.u_dual['chi']) - (1.0/self.rho) * gradients['chi']
        self.z_auxiliary['gamma'] = (self.x_primal['gamma'] + self.u_dual['gamma']) - (1.0/self.rho) * gradients['gamma']
    
    def _dual_update_stage(self):
        """Dual variable update stage (Algorithm 1, Line 15)"""
        self.u_dual['r'] += (self.x_primal['r'] - self.z_auxiliary['r'])
        self.u_dual['c'] += (self.x_primal['c'] - self.z_auxiliary['c'])
        self.u_dual['chi'] += (self.x_primal['chi'] - self.z_auxiliary['chi'])
        self.u_dual['gamma'] += (self.x_primal['gamma'] - self.z_auxiliary['gamma'])
    
    def _compute_residuals(self) -> Tuple[float, float]:
        """Compute primal and dual residuals (Lines 17-18)"""
        # Primal residual: ||x - z||
        primal_residual = (
            np.linalg.norm(self.x_primal['r'] - self.z_auxiliary['r']) +
            np.linalg.norm(self.x_primal['c'] - self.z_auxiliary['c']) +
            np.linalg.norm(self.x_primal['chi'] - self.z_auxiliary['chi']) +
            abs(self.x_primal['gamma'] - self.z_auxiliary['gamma'])
        )
        
        # Dual residual: rho * ||z^k - z^{k-1}||
        # Simplified: assume previous z is stored
        dual_residual = self.rho * primal_residual  # Approximation
        
        return primal_residual, dual_residual
    
    def _adaptive_penalty_update(self, primal_residual: float, dual_residual: float):
        """Adaptive penalty parameter update (Lines 19-23)"""
        if primal_residual > 10 * dual_residual:
            self.rho *= 2.0  # Increase penalty
        elif dual_residual > 10 * primal_residual:
            self.rho /= 2.0  # Decrease penalty
        
        # Keep penalty in reasonable bounds
        self.rho = max(0.01, min(self.rho, 100.0))
    
    def _check_convergence(self, primal_residual: float, dual_residual: float) -> bool:
        """Check ADMM convergence criteria"""
        return primal_residual < self.tolerance and dual_residual < self.tolerance
    
    def _reputation_guided_binary_recovery(self) -> Dict:
        """Reputation-guided binary recovery (Algorithm 1, Lines 25-29)"""
        assignments = {}
        
        # Compute cost matrix with reputation bias (Line 27)
        num_nodes, num_shards = self.x_primal['chi'].shape
        cost_matrix = np.zeros((num_nodes, num_shards))
        
        for n in range(num_nodes):
            for k in range(num_shards):
                # Gradient term
                gradient_term = self._compute_assignment_gradient(n, k)
                
                # Reputation bias term
                reputation_score = 0.5 + 0.5 * np.random.random()  # Simulated reputation
                normalized_reputation = (reputation_score - 0.0) / (1.0 - 0.0)  # Normalize
                reputation_bias = -self.beta * normalized_reputation
                
                cost_matrix[n, k] = gradient_term + reputation_bias
        
        # Solve minimum-cost assignment (simplified greedy approach)
        for k in range(num_shards):
            # Find best node for each shard
            best_node = np.argmin(cost_matrix[:, k])
            assignments[f"shard_{k}"] = [f"node_{best_node}"]
            cost_matrix[best_node, :] = float('inf')  # Prevent double assignment
        
        return assignments
    
    def _optimize_resources_with_assignments(self, assignments: Dict) -> Dict:
        """Optimize resources with fixed binary assignments (Line 31)"""
        # Use current resource allocations
        objective_value = self._compute_objective_value()
        
        # Convert to slice allocations
        slice_allocations = {}
        for shard_id, nodes in assignments.items():
            slice_name = f"slice_{shard_id}"
            slice_allocations[slice_name] = {
                'assigned_nodes': nodes,
                'bandwidth_allocation': self.x_primal['r'][int(shard_id.split('_')[1])] if shard_id.split('_')[1].isdigit() else 10.0,
                'compute_allocation': self.x_primal['c'][int(shard_id.split('_')[1])] if shard_id.split('_')[1].isdigit() else 1.0
            }
        
        # Convert to resource allocations
        resource_allocations = {}
        for i, rate in enumerate(self.x_primal['r']):
            resource_allocations[f"service_{i}_bandwidth"] = rate
        for i, compute in enumerate(self.x_primal['c']):
            resource_allocations[f"service_{i}_compute"] = compute
            
        return {
            'objective': objective_value,
            'slice_allocations': slice_allocations,
            'resource_allocations': resource_allocations
        }
    
    def _compute_objective_value(self) -> float:
        """Compute current objective function value"""
        # Simplified objective computation
        total_delay = np.sum(self.x_primal['c'] / np.maximum(self.x_primal['r'], 0.1))
        total_reputation = np.sum(self.x_primal['chi']) * 0.7  # Assume average reputation
        total_cost = np.sum(self.x_primal['c']) * 0.1
        
        return self.w1 * total_delay - self.w2 * total_reputation + self.w3 * total_cost
    
    def _compute_objective_gradients(self) -> Dict:
        """Compute gradients of objective function F1"""
        gradients = {}
        
        # Gradient w.r.t. data rates (r)
        gradients['r'] = -self.w1 * self.x_primal['c'] / np.maximum(self.x_primal['r']**2, 0.01)
        
        # Gradient w.r.t. compute resources (c) 
        gradients['c'] = self.w1 / np.maximum(self.x_primal['r'], 0.1) + self.w3 * 0.1
        
        # Gradient w.r.t. shard assignments (chi)
        gradients['chi'] = -self.w2 * np.ones_like(self.x_primal['chi']) * 0.7
        
        # Gradient w.r.t. epigraph variable (gamma)
        gradients['gamma'] = 1.0  # From -gamma objective
        
        return gradients
    
    def _compute_assignment_gradient(self, node_idx: int, shard_idx: int) -> float:
        """Compute gradient of F1 w.r.t. chi[n,k]"""
        # Simplified gradient computation
        return -self.w2 * 0.7  # Reputation contribution


class ResourceOptimizer:
    """
    Main resource optimizer implementing the MINLP problem using ADMM
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.optimization_weights = config.get('optimization', {})
        
        # Initialize ADMM optimizer
        self.admm_optimizer = ADMMOptimizer(self.optimization_weights)
        
        # Keep Lagrangian optimizer for comparison
        self.lagrangian_optimizer = LagrangianOptimizer(self.optimization_weights)
        
        # Performance tracking
        self.optimization_history = []
        self.convergence_statistics = {
            'total_runs': 0,
            'converged_runs': 0,
            'average_iterations': 0.0
        }
        
        self.logger = logging.getLogger('ResourceOptimizer')
    
    async def optimize(self, system_state: Dict) -> Dict:
        """
        Run the complete resource optimization process using ADMM
        
        Args:
            system_state: Current state of nodes, shards, and slices
            
        Returns:
            Dict containing optimization results
        """
        self.logger.info("Starting ADMM-based resource optimization...")
        
        # Run ADMM optimization (primary method)
        result = await self.admm_optimizer.optimize(system_state)
        
        # Fallback to Lagrangian if ADMM fails
        if not result.convergence_achieved:
            self.logger.warning("ADMM did not converge, falling back to Lagrangian dual decomposition")
            result = await self.lagrangian_optimizer.optimize(system_state)
        
        # Update statistics
        self._update_statistics(result)
        
        # Store optimization history
        self.optimization_history.append({
            'timestamp': len(self.optimization_history),
            'objective_value': result.objective_value,
            'convergence': result.convergence_achieved,
            'iterations': result.iterations
        })
        
        return {
            'slice_allocations': result.slice_allocations,
            'resource_allocations': result.resource_allocations,
            'optimization_metrics': {
                'objective_value': result.objective_value,
                'convergence_achieved': result.convergence_achieved,
                'iterations': result.iterations,
                'improvement_ratio': self._calculate_improvement_ratio(result)
            }
        }
    
    def _update_statistics(self, result: OptimizationResult):
        """Update convergence statistics"""
        self.convergence_statistics['total_runs'] += 1
        
        if result.convergence_achieved:
            self.convergence_statistics['converged_runs'] += 1
        
        # Update average iterations
        total_iterations = (self.convergence_statistics['average_iterations'] * 
                           (self.convergence_statistics['total_runs'] - 1) + 
                           result.iterations)
        self.convergence_statistics['average_iterations'] = (
            total_iterations / self.convergence_statistics['total_runs']
        )
    
    def _calculate_improvement_ratio(self, result: OptimizationResult) -> float:
        """Calculate improvement ratio compared to previous optimizations"""
        if len(self.optimization_history) == 0:
            return 1.0
        
        previous_objective = self.optimization_history[-1]['objective_value']
        if previous_objective != 0:
            return abs(result.objective_value) / abs(previous_objective)
        else:
            return 1.0
    
    def get_optimization_statistics(self) -> Dict:
        """Get comprehensive optimization statistics"""
        if not self.optimization_history:
            return {'no_data': True}
        
        objectives = [entry['objective_value'] for entry in self.optimization_history]
        iterations = [entry['iterations'] for entry in self.optimization_history]
        
        return {
            'total_optimizations': len(self.optimization_history),
            'convergence_rate': (self.convergence_statistics['converged_runs'] / 
                               self.convergence_statistics['total_runs']) * 100,
            'average_objective_value': np.mean(objectives),
            'best_objective_value': np.min(objectives),
            'average_iterations': np.mean(iterations),
            'objective_improvement_trend': self._calculate_trend(objectives),
            'recent_performance': objectives[-5:] if len(objectives) >= 5 else objectives
        }
    
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction for objective values"""
        if len(values) < 2:
            return 'insufficient_data'
        
        # Linear regression to find trend
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        if slope < -0.01:
            return 'improving'  # Decreasing objective (better)
        elif slope > 0.01:
            return 'degrading'  # Increasing objective (worse)
        else:
            return 'stable'
    
    def reset_optimization_history(self):
        """Reset optimization history and statistics"""
        self.optimization_history.clear()
        self.convergence_statistics = {
            'total_runs': 0,
            'converged_runs': 0,
            'average_iterations': 0.0
        }
        
        # Reset Lagrange multipliers
        self.lagrangian_optimizer.lambda_bandwidth.clear()
        self.lagrangian_optimizer.lambda_compute.clear()
        self.lagrangian_optimizer.lambda_reputation.clear()