import numpy as np
import asyncio
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass
import time


@dataclass
class OptimizationResult:
    """Optimization result container"""
    objective_value: float = 0.0
    primal_variables: Dict = None
    dual_variables: Dict = None
    convergence_achieved: bool = False
    iterations: int = 0
    solve_time: float = 0.0
    
    def __post_init__(self):
        if self.primal_variables is None:
            self.primal_variables = {}
        if self.dual_variables is None:
            self.dual_variables = {}


class ADMMSolver:
    """ADMM-based optimization solver"""
    
    def __init__(self, config: Dict):
        self.rho = config.get('penalty_parameter', 1.0)
        self.tolerance = config.get('tolerance', 1e-4)
        self.max_iterations = config.get('max_iterations', 1000)
        self.alpha = config.get('step_size', 0.01)
        self.beta = config.get('bias_parameter', 0.5)
        
        self.x_primal = {}
        self.z_auxiliary = {}
        self.u_dual = {}
        
        self.logger = logging.getLogger('ADMMSolver')
        self.iteration_history = []
        
    async def solve(self, problem_data: Dict) -> OptimizationResult:
        """Execute ADMM algorithm"""
        self.logger.info("Starting ADMM optimization")
        start_time = time.time()
        
        self._initialize_variables(problem_data)
        
        for iteration in range(self.max_iterations):
            # Step 1: Projection stage
            self._projection_step(problem_data)
            
            # Step 2: Proximal update
            self._proximal_step(problem_data)
            
            # Step 3: Dual update
            self._dual_step()
            
            # Compute residuals
            primal_residual, dual_residual = self._compute_residuals()
            
            # Adaptive penalty
            self._update_penalty(primal_residual, dual_residual)
            
            # Store iteration info
            self.iteration_history.append({
                'iteration': iteration,
                'primal_residual': primal_residual,
                'dual_residual': dual_residual,
                'objective': self._compute_objective_value()
            })
            
            # Check convergence
            if self._check_convergence(primal_residual, dual_residual):
                solve_time = time.time() - start_time
                binary_solution = self._binary_recovery()
                
                return OptimizationResult(
                    objective_value=self._compute_objective_value(),
                    primal_variables=self.x_primal.copy(),
                    dual_variables=self.u_dual.copy(),
                    convergence_achieved=True,
                    iterations=iteration + 1,
                    solve_time=solve_time
                )
        
        solve_time = time.time() - start_time
        return OptimizationResult(
            objective_value=self._compute_objective_value(),
            primal_variables=self.x_primal.copy(),
            dual_variables=self.u_dual.copy(),
            convergence_achieved=False,
            iterations=self.max_iterations,
            solve_time=solve_time
        )
    
    def _initialize_variables(self, problem_data: Dict):
        """Initialize ADMM variables"""
        num_services = problem_data['num_services']
        num_nodes = problem_data['num_nodes']
        num_shards = problem_data['num_shards']
        
        self.x_primal = {
            'r': np.random.uniform(5, 15, num_services),
            'c': np.random.uniform(0.5, 2.0, num_services),
            'chi': np.random.uniform(0, 1, (num_nodes, num_shards)),
            'gamma': np.random.uniform(0, 10, 1)[0]
        }
        
        self.z_auxiliary = {
            'r': np.copy(self.x_primal['r']),
            'c': np.copy(self.x_primal['c']),
            'chi': np.copy(self.x_primal['chi']),
            'gamma': self.x_primal['gamma']
        }
        
        self.u_dual = {
            'r': np.zeros_like(self.x_primal['r']),
            'c': np.zeros_like(self.x_primal['c']),
            'chi': np.zeros_like(self.x_primal['chi']),
            'gamma': 0.0
        }
    
    def _projection_step(self, problem_data: Dict):
        """Projection onto constraint set"""
        # Resource allocation constraints
        for j in range(len(self.x_primal['r'])):
            target = self.z_auxiliary['r'][j] - self.u_dual['r'][j]
            self.x_primal['r'][j] = max(0, min(target, 20.0))
        
        for j in range(len(self.x_primal['c'])):
            target = self.z_auxiliary['c'][j] - self.u_dual['c'][j]
            self.x_primal['c'][j] = max(0, min(target, 5.0))
        
        # Assignment constraints
        for n in range(self.x_primal['chi'].shape[0]):
            target_row = self.z_auxiliary['chi'][n, :] - self.u_dual['chi'][n, :]
            self.x_primal['chi'][n, :] = self._project_simplex(target_row)
        
        # Objective constraint
        current_obj = self._compute_objective_value()
        target = self.z_auxiliary['gamma'] - self.u_dual['gamma']
        self.x_primal['gamma'] = max(0, max(current_obj, target))
    
    def _project_simplex(self, vector: np.ndarray) -> np.ndarray:
        """Project onto probability simplex"""
        sorted_v = np.sort(vector)[::-1]
        cumsum_v = np.cumsum(sorted_v)
        
        rho_vals = sorted_v + (1.0 - cumsum_v) / np.arange(1, len(vector) + 1)
        rho = np.max(np.where(rho_vals > 0, rho_vals, 0))
        
        projected = np.maximum(vector - rho, 0)
        return projected / max(np.sum(projected), 1e-10)
    
    def _proximal_step(self, problem_data: Dict):
        """Proximal gradient update"""
        gradients = self._compute_gradients()
        
        self.z_auxiliary['r'] = (self.x_primal['r'] + self.u_dual['r']) - (1.0/self.rho) * gradients['r']
        self.z_auxiliary['c'] = (self.x_primal['c'] + self.u_dual['c']) - (1.0/self.rho) * gradients['c']
        self.z_auxiliary['chi'] = (self.x_primal['chi'] + self.u_dual['chi']) - (1.0/self.rho) * gradients['chi']
        self.z_auxiliary['gamma'] = (self.x_primal['gamma'] + self.u_dual['gamma']) - (1.0/self.rho) * gradients['gamma']
    
    def _dual_step(self):
        """Dual variable update"""
        self.u_dual['r'] += (self.x_primal['r'] - self.z_auxiliary['r'])
        self.u_dual['c'] += (self.x_primal['c'] - self.z_auxiliary['c'])
        self.u_dual['chi'] += (self.x_primal['chi'] - self.z_auxiliary['chi'])
        self.u_dual['gamma'] += (self.x_primal['gamma'] - self.z_auxiliary['gamma'])
    
    def _compute_residuals(self) -> Tuple[float, float]:
        """Compute primal and dual residuals"""
        primal_residual = (
            np.linalg.norm(self.x_primal['r'] - self.z_auxiliary['r']) +
            np.linalg.norm(self.x_primal['c'] - self.z_auxiliary['c']) +
            np.linalg.norm(self.x_primal['chi'] - self.z_auxiliary['chi']) +
            abs(self.x_primal['gamma'] - self.z_auxiliary['gamma'])
        )
        
        dual_residual = self.rho * primal_residual
        return primal_residual, dual_residual
    
    def _update_penalty(self, primal_residual: float, dual_residual: float):
        """Adaptive penalty parameter update"""
        if primal_residual > 10 * dual_residual:
            self.rho *= 2.0
        elif dual_residual > 10 * primal_residual:
            self.rho /= 2.0
        
        self.rho = max(0.01, min(self.rho, 100.0))
    
    def _check_convergence(self, primal_residual: float, dual_residual: float) -> bool:
        """Check convergence criteria"""
        return primal_residual < self.tolerance and dual_residual < self.tolerance
    
    def _compute_objective_value(self) -> float:
        """Compute current objective function value"""
        total_delay = np.sum(self.x_primal['c'] / np.maximum(self.x_primal['r'], 0.1))
        total_reputation = np.sum(self.x_primal['chi']) * 0.7
        total_cost = np.sum(self.x_primal['c']) * 0.1
        
        return 0.4 * total_delay - 0.4 * total_reputation + 0.2 * total_cost
    
    def _compute_gradients(self) -> Dict:
        """Compute objective function gradients"""
        gradients = {}
        
        gradients['r'] = -0.4 * self.x_primal['c'] / np.maximum(self.x_primal['r']**2, 0.01)
        gradients['c'] = 0.4 / np.maximum(self.x_primal['r'], 0.1) + 0.2 * 0.1
        gradients['chi'] = -0.4 * np.ones_like(self.x_primal['chi']) * 0.7
        gradients['gamma'] = 1.0
        
        return gradients
    
    def _binary_recovery(self) -> Dict:
        """Recovery binary assignments using reputation-guided approach"""
        assignments = {}
        
        num_nodes, num_shards = self.x_primal['chi'].shape
        cost_matrix = np.zeros((num_nodes, num_shards))
        
        for n in range(num_nodes):
            for k in range(num_shards):
                gradient_term = -0.4 * 0.7
                reputation_score = 0.5 + 0.5 * np.random.random()
                normalized_reputation = reputation_score
                reputation_bias = -self.beta * normalized_reputation
                
                cost_matrix[n, k] = gradient_term + reputation_bias
        
        for k in range(num_shards):
            best_node = np.argmin(cost_matrix[:, k])
            assignments[f"shard_{k}"] = [f"node_{best_node}"]
            cost_matrix[best_node, :] = float('inf')
        
        return assignments
    
    def get_convergence_statistics(self) -> Dict:
        """Get convergence analysis"""
        if not self.iteration_history:
            return {'no_data': True}
        
        primal_residuals = [entry['primal_residual'] for entry in self.iteration_history]
        dual_residuals = [entry['dual_residual'] for entry in self.iteration_history]
        objectives = [entry['objective'] for entry in self.iteration_history]
        
        return {
            'total_iterations': len(self.iteration_history),
            'final_primal_residual': primal_residuals[-1],
            'final_dual_residual': dual_residuals[-1],
            'objective_progression': objectives,
            'residual_reduction_rate': primal_residuals[0] / primal_residuals[-1] if primal_residuals[-1] > 0 else float('inf')
        }