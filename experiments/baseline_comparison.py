# Blockchain System Performance Comparison Framework

import asyncio
import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
import time
import statistics

class BaselineRunner:
    """Base class for running blockchain system configurations"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.results = []
        
    async def initialize_system(self):
        """Initialize the blockchain system"""
        pass
    
    async def run_experiment(self, workload: Dict) -> Dict:
        """Run a single experiment with given workload"""
        pass
    
    async def cleanup(self):
        """Cleanup resources after experiment"""
        pass

class OptimizedConfigRunner(BaselineRunner):
    """Runner for optimized blockchain configuration"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.system_type = "optimized"
        
    async def initialize_system(self):
        self.logger.info("Initializing optimized blockchain configuration")
        await asyncio.sleep(2)  # Simulate initialization
        
    async def run_experiment(self, workload: Dict) -> Dict:
        transaction_count = workload.get('transactions', 1000)
        shard_count = workload.get('shards', 20)
        
        # Simulate optimized performance
        base_tps = 750
        scaling_factor = min(1.8, 1 + (shard_count - 20) * 0.04)
        throughput = base_tps * scaling_factor
        
        latency = max(35, 45 - (shard_count - 20) * 0.5)
        
        return {
            'throughput': throughput,
            'latency': latency,
            'transactions_processed': transaction_count,
            'success_rate': 0.98,
            'resource_utilization': 0.75
        }

class StandardConfigRunner(BaselineRunner):
    """Runner for standard blockchain configuration"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.system_type = "standard"
        
    async def initialize_system(self):
        self.logger.info("Initializing standard blockchain configuration")
        await asyncio.sleep(3)  # Simulate initialization
        
    async def run_experiment(self, workload: Dict) -> Dict:
        transaction_count = workload.get('transactions', 1000)
        shard_count = workload.get('shards', 20)
        
        # Simulate standard performance
        base_tps = 435
        scaling_factor = min(1.1, 1 + (shard_count - 20) * 0.005)
        throughput = base_tps * scaling_factor
        
        latency = max(150, 180 - (shard_count - 20) * 0.1)
        
        return {
            'throughput': throughput,
            'latency': latency,
            'transactions_processed': transaction_count,
            'success_rate': 0.92,
            'resource_utilization': 0.82
        }

class EnhancedConfigRunner(BaselineRunner):
    """Runner for enhanced blockchain configuration"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.system_type = "enhanced"
        
    async def initialize_system(self):
        self.logger.info("Initializing enhanced blockchain configuration")
        await asyncio.sleep(2.5)  # Simulate initialization
        
    async def run_experiment(self, workload: Dict) -> Dict:
        transaction_count = workload.get('transactions', 1000)
        shard_count = workload.get('shards', 20)
        
        # Simulate enhanced performance
        base_tps = 706
        scaling_factor = min(1.4, 1 + (shard_count - 20) * 0.02)
        throughput = base_tps * scaling_factor
        
        latency = max(80, 95 - (shard_count - 20) * 0.3)
        
        return {
            'throughput': throughput,
            'latency': latency,
            'transactions_processed': transaction_count,
            'success_rate': 0.95,
            'resource_utilization': 0.68
        }

class BasicConfigRunner(BaselineRunner):
    """Runner for basic blockchain configuration"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.system_type = "basic"
        
    async def initialize_system(self):
        self.logger.info("Initializing basic blockchain configuration")
        await asyncio.sleep(4)  # Simulate initialization
        
    async def run_experiment(self, workload: Dict) -> Dict:
        transaction_count = workload.get('transactions', 1000)
        shard_count = workload.get('shards', 20)
        
        # Simulate basic performance
        base_tps = 280
        scaling_factor = min(0.9, 1 - (shard_count - 20) * 0.01)
        throughput = base_tps * scaling_factor
        
        latency = max(200, 250 + (shard_count - 20) * 0.5)
        
        return {
            'throughput': throughput,
            'latency': latency,
            'transactions_processed': transaction_count,
            'success_rate': 0.88,
            'resource_utilization': 0.92
        }

class PerformanceComparison:
    """Framework for comparing different blockchain configurations"""
    
    def __init__(self, output_dir: str = "results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize system runners
        self.runners = {
            'optimized': OptimizedConfigRunner(),
            'standard': StandardConfigRunner(),
            'enhanced': EnhancedConfigRunner(),
            'basic': BasicConfigRunner()
        }
        
    async def run_comparison_suite(self, workloads: List[Dict]) -> Dict:
        """Run comparison across all systems with given workloads"""
        
        results = {
            'experiment_metadata': {
                'timestamp': time.time(),
                'systems_tested': list(self.runners.keys()),
                'workload_count': len(workloads)
            },
            'results': {}
        }
        
        for system_name, runner in self.runners.items():
            self.logger.info(f"Testing {system_name} configuration")
            
            await runner.initialize_system()
            system_results = []
            
            for i, workload in enumerate(workloads):
                self.logger.info(f"Running workload {i+1}/{len(workloads)} for {system_name}")
                
                try:
                    result = await runner.run_experiment(workload)
                    result['workload_id'] = i
                    result['workload_params'] = workload
                    system_results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error in {system_name} workload {i}: {e}")
                    continue
            
            await runner.cleanup()
            results['results'][system_name] = system_results
            
        return results
    
    def analyze_results(self, results: Dict) -> Dict:
        """Analyze comparison results"""
        
        analysis = {
            'summary': {},
            'comparative_metrics': {},
            'performance_rankings': {}
        }
        
        # Calculate summary statistics for each system
        for system_name, system_results in results['results'].items():
            if not system_results:
                continue
                
            throughputs = [r['throughput'] for r in system_results]
            latencies = [r['latency'] for r in system_results]
            success_rates = [r['success_rate'] for r in system_results]
            
            analysis['summary'][system_name] = {
                'avg_throughput': statistics.mean(throughputs),
                'max_throughput': max(throughputs),
                'avg_latency': statistics.mean(latencies),
                'min_latency': min(latencies),
                'avg_success_rate': statistics.mean(success_rates),
                'total_experiments': len(system_results)
            }
        
        # Comparative analysis
        if len(analysis['summary']) > 1:
            systems = list(analysis['summary'].keys())
            
            # Throughput comparison
            throughput_ranking = sorted(systems, 
                                      key=lambda x: analysis['summary'][x]['avg_throughput'], 
                                      reverse=True)
            
            # Latency comparison (lower is better)
            latency_ranking = sorted(systems, 
                                   key=lambda x: analysis['summary'][x]['avg_latency'])
            
            analysis['performance_rankings'] = {
                'throughput_ranking': throughput_ranking,
                'latency_ranking': latency_ranking
            }
            
            # Performance ratios
            best_throughput = analysis['summary'][throughput_ranking[0]]['avg_throughput']
            best_latency = analysis['summary'][latency_ranking[0]]['avg_latency']
            
            analysis['comparative_metrics'] = {}
            for system in systems:
                summary = analysis['summary'][system]
                analysis['comparative_metrics'][system] = {
                    'throughput_ratio': summary['avg_throughput'] / best_throughput,
                    'latency_ratio': summary['avg_latency'] / best_latency,
                    'overall_score': (
                        (summary['avg_throughput'] / best_throughput) * 0.4 +
                        (best_latency / summary['avg_latency']) * 0.4 +
                        summary['avg_success_rate'] * 0.2
                    )
                }
        
        return analysis
    
    def save_results(self, results: Dict, analysis: Dict, filename_prefix: str = "comparison"):
        """Save results and analysis to files"""
        
        timestamp = int(time.time())
        
        # Save raw results
        results_file = self.output_dir / f"{filename_prefix}_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Save analysis
        analysis_file = self.output_dir / f"{filename_prefix}_analysis_{timestamp}.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2)
        
        # Create CSV summary
        csv_data = []
        for system_name, summary in analysis['summary'].items():
            row = {
                'system': system_name,
                **summary
            }
            if system_name in analysis.get('comparative_metrics', {}):
                row.update(analysis['comparative_metrics'][system_name])
            csv_data.append(row)
        
        if csv_data:
            df = pd.DataFrame(csv_data)
            csv_file = self.output_dir / f"{filename_prefix}_summary_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
        
        self.logger.info(f"Results saved to {self.output_dir}")
        return {
            'results_file': str(results_file),
            'analysis_file': str(analysis_file),
            'csv_file': str(csv_file) if csv_data else None
        }

async def main():
    """Main execution function"""
    
    # Define test workloads
    workloads = [
        {'transactions': 2000, 'shards': 20, 'duration': 300},
        {'transactions': 4000, 'shards': 40, 'duration': 300},
        {'transactions': 6000, 'shards': 60, 'duration': 300},
        {'transactions': 8000, 'shards': 80, 'duration': 300}
    ]
    
    # Run comparison
    comparison = PerformanceComparison()
    
    print("Starting blockchain configuration comparison...")
    results = await comparison.run_comparison_suite(workloads)
    
    print("Analyzing results...")
    analysis = comparison.analyze_results(results)
    
    print("Saving results...")
    files = comparison.save_results(results, analysis)
    
    print(f"Comparison completed. Files saved:")
    for file_type, file_path in files.items():
        if file_path:
            print(f"  {file_type}: {file_path}")
    
    # Print summary
    print("\nPerformance Summary:")
    for system, summary in analysis['summary'].items():
        print(f"\n{system.upper()}:")
        print(f"  Avg Throughput: {summary['avg_throughput']:.1f} TPS")
        print(f"  Avg Latency: {summary['avg_latency']:.1f} ms")
        print(f"  Success Rate: {summary['avg_success_rate']:.2%}")

if __name__ == "__main__":
    asyncio.run(main())