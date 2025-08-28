"""
Complexity analysis and performance evaluation
"""

import numpy as np
import time
from typing import Dict, List, Tuple
import logging
from dataclasses import dataclass
import matplotlib.pyplot as plt


@dataclass
class ComplexityMetrics:
    """Complexity measurement container"""
    problem_size: int
    execution_time: float
    memory_usage: float
    iterations: int
    convergence_rate: float
    constraint_evaluations: int


class ComplexityAnalyzer:
    """Analyzes algorithmic complexity and performance"""
    
    def __init__(self):
        self.logger = logging.getLogger('ComplexityAnalyzer')
        self.measurements = []
        
    def measure_algorithmic_complexity(self, algorithm_func, problem_sizes: List[int], 
                                     iterations: int = 5) -> List[ComplexityMetrics]:
        """Measure complexity across different problem sizes"""
        results = []
        
        for size in problem_sizes:
            self.logger.info(f"Analyzing complexity for problem size: {size}")
            
            size_measurements = []
            for i in range(iterations):
                start_time = time.time()
                start_memory = self._get_memory_usage()
                
                # Execute algorithm
                problem_data = self._generate_problem_instance(size)
                result = algorithm_func(problem_data)
                
                end_time = time.time()
                end_memory = self._get_memory_usage()
                
                metrics = ComplexityMetrics(
                    problem_size=size,
                    execution_time=end_time - start_time,
                    memory_usage=end_memory - start_memory,
                    iterations=result.get('iterations', 0),
                    convergence_rate=result.get('convergence_rate', 0.0),
                    constraint_evaluations=result.get('constraint_evals', 0)
                )
                size_measurements.append(metrics)
            
            # Average measurements
            avg_metrics = self._average_metrics(size_measurements)
            results.append(avg_metrics)
            self.measurements.extend(size_measurements)
        
        return results
    
    def analyze_convergence_properties(self, algorithm_func, problem_data: Dict, 
                                     tolerance_levels: List[float]) -> Dict:
        """Analyze convergence behavior under different tolerances"""
        convergence_analysis = {}
        
        for tolerance in tolerance_levels:
            self.logger.info(f"Testing convergence with tolerance: {tolerance}")
            
            # Modify problem tolerance
            modified_data = problem_data.copy()
            modified_data['tolerance'] = tolerance
            
            start_time = time.time()
            result = algorithm_func(modified_data)
            end_time = time.time()
            
            convergence_analysis[tolerance] = {
                'converged': result.get('convergence_achieved', False),
                'iterations': result.get('iterations', 0),
                'final_residual': result.get('final_residual', float('inf')),
                'execution_time': end_time - start_time,
                'objective_value': result.get('objective_value', 0.0)
            }
        
        return convergence_analysis
    
    def theoretical_complexity_bounds(self, problem_size: int) -> Dict:
        """Compute theoretical complexity bounds"""
        n = problem_size  # Number of services
        m = int(n * 1.5)  # Number of constraints (estimated)
        
        # Theoretical bounds for different components
        bounds = {
            'admm_per_iteration': f"O(n^2) = O({n**2})",
            'projection_complexity': f"O(n*m) = O({n*m})", 
            'dual_update_complexity': f"O(n) = O({n})",
            'convergence_iterations': f"O(√n) = O({int(np.sqrt(n))})",
            'overall_complexity': f"O(n^2.5) = O({n**2.5:.0f})",
            'memory_complexity': f"O(n^2) = O({n**2})"
        }
        
        return bounds
    
    def empirical_complexity_fitting(self) -> Dict:
        """Fit empirical complexity curves to measurement data"""
        if not self.measurements:
            return {'error': 'No measurement data available'}
        
        sizes = [m.problem_size for m in self.measurements]
        times = [m.execution_time for m in self.measurements]
        memories = [m.memory_usage for m in self.measurements]
        iterations = [m.iterations for m in self.measurements]
        
        # Fit polynomial curves
        time_fit = np.polyfit(np.log(sizes), np.log(times), 1)
        memory_fit = np.polyfit(np.log(sizes), np.log(memories), 1)
        iter_fit = np.polyfit(np.log(sizes), np.log(iterations), 1)
        
        return {
            'time_complexity_exponent': time_fit[0],
            'time_complexity_constant': np.exp(time_fit[1]),
            'memory_complexity_exponent': memory_fit[0],
            'memory_complexity_constant': np.exp(memory_fit[1]),
            'iteration_complexity_exponent': iter_fit[0],
            'iteration_complexity_constant': np.exp(iter_fit[1]),
            'empirical_time_formula': f"T(n) ≈ {np.exp(time_fit[1]):.3f} * n^{time_fit[0]:.2f}",
            'empirical_memory_formula': f"M(n) ≈ {np.exp(memory_fit[1]):.3f} * n^{memory_fit[0]:.2f}"
        }
    
    def scalability_analysis(self, max_problem_size: int, step_size: int) -> Dict:
        """Analyze scalability characteristics"""
        problem_sizes = list(range(step_size, max_problem_size + 1, step_size))
        
        scalability_metrics = {
            'linear_scalability': [],
            'quadratic_scalability': [],
            'actual_measurements': []
        }
        
        base_time = None
        base_size = problem_sizes[0]
        
        for size in problem_sizes:
            # Generate theoretical scalability predictions
            linear_prediction = size / base_size
            quadratic_prediction = (size / base_size) ** 2
            
            scalability_metrics['linear_scalability'].append(linear_prediction)
            scalability_metrics['quadratic_scalability'].append(quadratic_prediction)
            
            # Find actual measurement for this size
            actual_measurement = next((m for m in self.measurements if m.problem_size == size), None)
            if actual_measurement:
                if base_time is None:
                    base_time = actual_measurement.execution_time
                actual_ratio = actual_measurement.execution_time / base_time
                scalability_metrics['actual_measurements'].append(actual_ratio)
            else:
                scalability_metrics['actual_measurements'].append(None)
        
        return {
            'problem_sizes': problem_sizes,
            'scalability_metrics': scalability_metrics,
            'scalability_classification': self._classify_scalability(scalability_metrics)
        }
    
    def optimization_efficiency_analysis(self) -> Dict:
        """Analyze optimization efficiency metrics"""
        if not self.measurements:
            return {'error': 'No measurement data'}
        
        convergence_rates = [m.convergence_rate for m in self.measurements if m.convergence_rate > 0]
        iteration_counts = [m.iterations for m in self.measurements]
        
        efficiency_metrics = {
            'average_convergence_rate': np.mean(convergence_rates) if convergence_rates else 0.0,
            'convergence_rate_std': np.std(convergence_rates) if convergence_rates else 0.0,
            'average_iterations': np.mean(iteration_counts),
            'iteration_std': np.std(iteration_counts),
            'convergence_efficiency': len([r for r in convergence_rates if r > 0.95]) / len(convergence_rates) if convergence_rates else 0.0
        }
        
        return efficiency_metrics
    
    def _generate_problem_instance(self, size: int) -> Dict:
        """Generate test problem instance of given size"""
        return {
            'num_services': size,
            'num_nodes': int(size * 0.8),
            'num_shards': min(size, 20),
            'num_links': int(size * 0.6),
            'tolerance': 1e-4,
            'max_iterations': 1000,
            'problem_data': np.random.randn(size, size)
        }
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def _average_metrics(self, metrics_list: List[ComplexityMetrics]) -> ComplexityMetrics:
        """Average multiple complexity measurements"""
        if not metrics_list:
            return ComplexityMetrics(0, 0.0, 0.0, 0, 0.0, 0)
        
        return ComplexityMetrics(
            problem_size=metrics_list[0].problem_size,
            execution_time=np.mean([m.execution_time for m in metrics_list]),
            memory_usage=np.mean([m.memory_usage for m in metrics_list]),
            iterations=int(np.mean([m.iterations for m in metrics_list])),
            convergence_rate=np.mean([m.convergence_rate for m in metrics_list]),
            constraint_evaluations=int(np.mean([m.constraint_evaluations for m in metrics_list]))
        )
    
    def _classify_scalability(self, metrics: Dict) -> str:
        """Classify scalability behavior"""
        actual = metrics['actual_measurements']
        linear = metrics['linear_scalability']
        quadratic = metrics['quadratic_scalability']
        
        # Remove None values
        valid_indices = [i for i, v in enumerate(actual) if v is not None]
        if len(valid_indices) < 2:
            return 'insufficient_data'
        
        actual_clean = [actual[i] for i in valid_indices]
        linear_clean = [linear[i] for i in valid_indices]
        quadratic_clean = [quadratic[i] for i in valid_indices]
        
        # Compute correlation with theoretical curves
        linear_corr = np.corrcoef(actual_clean, linear_clean)[0, 1]
        quadratic_corr = np.corrcoef(actual_clean, quadratic_clean)[0, 1]
        
        if linear_corr > quadratic_corr and linear_corr > 0.8:
            return 'linear_scalable'
        elif quadratic_corr > 0.8:
            return 'quadratic_scalable'
        else:
            return 'complex_scalability'
    
    def generate_complexity_report(self) -> Dict:
        """Generate comprehensive complexity analysis report"""
        if not self.measurements:
            return {'error': 'No measurement data available'}
        
        empirical_analysis = self.empirical_complexity_fitting()
        efficiency_analysis = self.optimization_efficiency_analysis()
        scalability_analysis = self.scalability_analysis(max([m.problem_size for m in self.measurements]), 5)
        
        return {
            'measurement_summary': {
                'total_measurements': len(self.measurements),
                'problem_size_range': (min(m.problem_size for m in self.measurements),
                                     max(m.problem_size for m in self.measurements)),
                'time_range': (min(m.execution_time for m in self.measurements),
                             max(m.execution_time for m in self.measurements))
            },
            'empirical_complexity': empirical_analysis,
            'optimization_efficiency': efficiency_analysis,
            'scalability_characteristics': scalability_analysis,
            'theoretical_bounds': self.theoretical_complexity_bounds(max(m.problem_size for m in self.measurements))
        }