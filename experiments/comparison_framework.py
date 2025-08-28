# Blockchain Configuration Comparison Framework

import asyncio
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time
import logging
import statistics
from dataclasses import dataclass, asdict

@dataclass
class ExperimentConfig:
    """Configuration for a single experiment"""
    name: str
    transaction_count: int
    shard_count: int
    duration: int
    workload_type: str = "mixed"
    target_tps: int = 1000

@dataclass
class PerformanceMetrics:
    """Performance metrics from an experiment"""
    throughput: float
    latency: float
    cpu_utilization: float
    memory_utilization: float
    success_rate: float
    resource_efficiency: float
    network_overhead: float

@dataclass
class SystemResult:
    """Results from a system configuration"""
    system_name: str
    config: ExperimentConfig
    metrics: PerformanceMetrics
    timestamp: float
    raw_measurements: Dict

class ComparisonFramework:
    """Framework for comparing different blockchain system configurations"""
    
    def __init__(self, output_dir: str = "comparison_results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.results: List[SystemResult] = []
        
        # Configuration profiles
        self.system_profiles = {
            'optimized': {
                'base_throughput': 750,
                'latency_coefficient': 0.85,
                'scaling_efficiency': 1.8,
                'cpu_efficiency': 0.75,
                'memory_efficiency': 0.78
            },
            'standard': {
                'base_throughput': 435,
                'latency_coefficient': 1.4,
                'scaling_efficiency': 1.1,
                'cpu_efficiency': 0.82,
                'memory_efficiency': 0.85
            },
            'enhanced': {
                'base_throughput': 706,
                'latency_coefficient': 1.1,
                'scaling_efficiency': 1.4,
                'cpu_efficiency': 0.68,
                'memory_efficiency': 0.72
            },
            'basic': {
                'base_throughput': 280,
                'latency_coefficient': 1.8,
                'scaling_efficiency': 0.9,
                'cpu_efficiency': 0.92,
                'memory_efficiency': 0.95
            }
        }
    
    def simulate_system_performance(self, system_name: str, config: ExperimentConfig) -> PerformanceMetrics:
        """Simulate performance metrics for a system configuration"""
        
        if system_name not in self.system_profiles:
            raise ValueError(f"Unknown system: {system_name}")
        
        profile = self.system_profiles[system_name]
        
        # Calculate throughput with scaling
        base_tps = profile['base_throughput']
        shard_factor = min(config.shard_count / 20.0, 4.0)  # Cap scaling at 4x
        scaling_efficiency = profile['scaling_efficiency']
        
        throughput = base_tps * (1 + (shard_factor - 1) * (scaling_efficiency - 1))
        throughput = max(throughput, base_tps * 0.5)  # Minimum performance floor
        
        # Calculate latency
        base_latency = 50  # Base latency in ms
        latency_coeff = profile['latency_coefficient']
        load_factor = config.target_tps / throughput if throughput > 0 else 1
        
        latency = base_latency * latency_coeff * (1 + max(0, load_factor - 0.7) * 2)
        
        # Resource utilization
        load_ratio = min(config.target_tps / throughput, 1.0) if throughput > 0 else 1.0
        cpu_util = profile['cpu_efficiency'] * load_ratio + np.random.normal(0, 0.05)
        memory_util = profile['memory_efficiency'] * load_ratio + np.random.normal(0, 0.03)
        
        # Success rate (decreases with overload)
        success_rate = max(0.85, 0.98 - max(0, load_ratio - 0.8) * 0.5)
        
        # Resource efficiency (throughput per resource unit)
        resource_efficiency = throughput / (cpu_util + memory_util) if (cpu_util + memory_util) > 0 else 0
        
        # Network overhead (increases with shard count)
        network_overhead = 10 + config.shard_count * 0.5 + np.random.normal(0, 2)
        
        return PerformanceMetrics(
            throughput=round(max(0, throughput), 2),
            latency=round(max(10, latency), 2),
            cpu_utilization=round(max(0, min(1, cpu_util)), 3),
            memory_utilization=round(max(0, min(1, memory_util)), 3),
            success_rate=round(max(0, min(1, success_rate)), 3),
            resource_efficiency=round(max(0, resource_efficiency), 2),
            network_overhead=round(max(0, network_overhead), 2)
        )
    
    async def run_experiment(self, system_name: str, config: ExperimentConfig) -> SystemResult:
        """Run a single experiment for a system configuration"""
        
        self.logger.info(f"Running experiment: {config.name} on {system_name}")
        
        # Simulate experiment runtime
        await asyncio.sleep(0.1)  # Simulate processing time
        
        # Get performance metrics
        metrics = self.simulate_system_performance(system_name, config)
        
        # Create raw measurements dictionary
        raw_measurements = {
            'experiment_id': f"{system_name}_{config.name}_{int(time.time())}",
            'start_time': time.time(),
            'end_time': time.time() + config.duration,
            'config_params': asdict(config),
            'system_profile': self.system_profiles[system_name].copy()
        }
        
        return SystemResult(
            system_name=system_name,
            config=config,
            metrics=metrics,
            timestamp=time.time(),
            raw_measurements=raw_measurements
        )
    
    async def run_comparison_suite(self, systems: List[str], configs: List[ExperimentConfig]) -> None:
        """Run full comparison suite across all systems and configurations"""
        
        self.logger.info(f"Starting comparison suite: {len(systems)} systems, {len(configs)} configs")
        
        # Reset results
        self.results = []
        
        for system in systems:
            if system not in self.system_profiles:
                self.logger.warning(f"Skipping unknown system: {system}")
                continue
                
            for config in configs:
                try:
                    result = await self.run_experiment(system, config)
                    self.results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error in experiment {config.name} on {system}: {e}")
                    continue
        
        self.logger.info(f"Completed {len(self.results)} experiments")
    
    def analyze_results(self) -> Dict:
        """Analyze comparison results and generate insights"""
        
        if not self.results:
            return {"error": "No results to analyze"}
        
        analysis = {
            'experiment_summary': {
                'total_experiments': len(self.results),
                'systems_tested': list(set(r.system_name for r in self.results)),
                'unique_configs': list(set(r.config.name for r in self.results))
            },
            'system_performance': {},
            'configuration_analysis': {},
            'comparative_insights': {},
            'recommendations': []
        }
        
        # Analyze by system
        for system in analysis['experiment_summary']['systems_tested']:
            system_results = [r for r in self.results if r.system_name == system]
            
            if not system_results:
                continue
            
            # Calculate averages
            avg_metrics = {
                'throughput': statistics.mean(r.metrics.throughput for r in system_results),
                'latency': statistics.mean(r.metrics.latency for r in system_results),
                'cpu_utilization': statistics.mean(r.metrics.cpu_utilization for r in system_results),
                'memory_utilization': statistics.mean(r.metrics.memory_utilization for r in system_results),
                'success_rate': statistics.mean(r.metrics.success_rate for r in system_results),
                'resource_efficiency': statistics.mean(r.metrics.resource_efficiency for r in system_results)
            }
            
            # Calculate performance ranges
            perf_ranges = {
                'throughput_range': (
                    min(r.metrics.throughput for r in system_results),
                    max(r.metrics.throughput for r in system_results)
                ),
                'latency_range': (
                    min(r.metrics.latency for r in system_results),
                    max(r.metrics.latency for r in system_results)
                )
            }
            
            analysis['system_performance'][system] = {
                **avg_metrics,
                **perf_ranges,
                'experiment_count': len(system_results)
            }
        
        # Configuration analysis
        for config_name in analysis['experiment_summary']['unique_configs']:
            config_results = [r for r in self.results if r.config.name == config_name]
            
            if not config_results:
                continue
            
            # Performance by system for this configuration
            config_performance = {}
            for result in config_results:
                config_performance[result.system_name] = {
                    'throughput': result.metrics.throughput,
                    'latency': result.metrics.latency,
                    'resource_efficiency': result.metrics.resource_efficiency
                }
            
            analysis['configuration_analysis'][config_name] = config_performance
        
        # Comparative insights
        if len(analysis['system_performance']) > 1:
            systems = list(analysis['system_performance'].keys())
            
            # Best performing system by metric
            best_throughput = max(systems, key=lambda s: analysis['system_performance'][s]['throughput'])
            best_latency = min(systems, key=lambda s: analysis['system_performance'][s]['latency'])
            best_efficiency = max(systems, key=lambda s: analysis['system_performance'][s]['resource_efficiency'])
            
            analysis['comparative_insights'] = {
                'best_throughput_system': best_throughput,
                'best_latency_system': best_latency,
                'most_efficient_system': best_efficiency
            }
            
            # Performance ratios
            baseline_system = systems[0]  # Use first system as baseline
            baseline_perf = analysis['system_performance'][baseline_system]
            
            performance_ratios = {}
            for system in systems:
                if system == baseline_system:
                    continue
                    
                sys_perf = analysis['system_performance'][system]
                performance_ratios[system] = {
                    'throughput_ratio': sys_perf['throughput'] / baseline_perf['throughput'],
                    'latency_ratio': sys_perf['latency'] / baseline_perf['latency'],
                    'efficiency_ratio': sys_perf['resource_efficiency'] / baseline_perf['resource_efficiency']
                }
            
            analysis['comparative_insights']['performance_ratios'] = performance_ratios
        
        # Generate recommendations
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        return analysis
    
    def _generate_recommendations(self, analysis: Dict) -> List[str]:
        """Generate recommendations based on analysis results"""
        recommendations = []
        
        if 'system_performance' not in analysis or len(analysis['system_performance']) < 2:
            return recommendations
        
        systems_perf = analysis['system_performance']
        
        # Find system with best overall performance
        overall_scores = {}
        for system, perf in systems_perf.items():
            # Weighted score: throughput (40%) + efficiency (30%) - latency penalty (20%) + success rate (10%)
            score = (
                perf['throughput'] * 0.4 + 
                perf['resource_efficiency'] * 0.3 +
                (1000 / max(perf['latency'], 1)) * 0.2 +  # Inverse latency
                perf['success_rate'] * 100 * 0.1
            )
            overall_scores[system] = score
        
        best_overall = max(overall_scores.keys(), key=lambda k: overall_scores[k])
        recommendations.append(f"For overall performance, consider '{best_overall}' configuration")
        
        # Specific use case recommendations
        best_throughput = max(systems_perf.keys(), key=lambda k: systems_perf[k]['throughput'])
        if best_throughput != best_overall:
            recommendations.append(f"For maximum throughput workloads, '{best_throughput}' performs best")
        
        best_latency = min(systems_perf.keys(), key=lambda k: systems_perf[k]['latency'])
        if best_latency != best_overall:
            recommendations.append(f"For latency-sensitive applications, '{best_latency}' is optimal")
        
        most_efficient = max(systems_perf.keys(), key=lambda k: systems_perf[k]['resource_efficiency'])
        if most_efficient != best_overall:
            recommendations.append(f"For resource-constrained environments, '{most_efficient}' is most efficient")
        
        return recommendations
    
    def save_results(self, analysis: Dict, filename_prefix: str = "blockchain_comparison") -> Dict[str, str]:
        """Save comparison results and analysis"""
        
        timestamp = int(time.time())
        
        # Save raw results
        raw_data = []
        for result in self.results:
            row = {
                'system': result.system_name,
                'config': result.config.name,
                'transaction_count': result.config.transaction_count,
                'shard_count': result.config.shard_count,
                'duration': result.config.duration,
                'throughput': result.metrics.throughput,
                'latency': result.metrics.latency,
                'cpu_utilization': result.metrics.cpu_utilization,
                'memory_utilization': result.metrics.memory_utilization,
                'success_rate': result.metrics.success_rate,
                'resource_efficiency': result.metrics.resource_efficiency,
                'network_overhead': result.metrics.network_overhead,
                'timestamp': result.timestamp
            }
            raw_data.append(row)
        
        # Save as CSV
        csv_file = self.output_dir / f"{filename_prefix}_raw_results_{timestamp}.csv"
        if raw_data:
            df = pd.DataFrame(raw_data)
            df.to_csv(csv_file, index=False)
        
        # Save analysis as JSON
        analysis_file = self.output_dir / f"{filename_prefix}_analysis_{timestamp}.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2)
        
        # Create summary CSV
        summary_data = []
        if 'system_performance' in analysis:
            for system, perf in analysis['system_performance'].items():
                summary_data.append({
                    'system': system,
                    **perf
                })
        
        summary_file = self.output_dir / f"{filename_prefix}_summary_{timestamp}.csv"
        if summary_data:
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_csv(summary_file, index=False)
        
        return {
            'raw_results': str(csv_file),
            'analysis': str(analysis_file),
            'summary': str(summary_file)
        }
    
    def print_summary(self, analysis: Dict):
        """Print a formatted summary of the comparison results"""
        
        print("\n" + "="*60)
        print("BLOCKCHAIN SYSTEM COMPARISON RESULTS")
        print("="*60)
        
        if 'experiment_summary' in analysis:
            summary = analysis['experiment_summary']
            print(f"\nExperiment Overview:")
            print(f"  Total Experiments: {summary['total_experiments']}")
            print(f"  Systems Tested: {', '.join(summary['systems_tested'])}")
            print(f"  Configurations: {', '.join(summary['unique_configs'])}")
        
        if 'system_performance' in analysis:
            print(f"\nSystem Performance Summary:")
            print(f"{'System':<12} {'Throughput':<12} {'Latency':<10} {'Efficiency':<12} {'Success Rate':<12}")
            print("-" * 60)
            
            for system, perf in analysis['system_performance'].items():
                print(f"{system:<12} {perf['throughput']:<12.1f} {perf['latency']:<10.1f} "
                      f"{perf['resource_efficiency']:<12.2f} {perf['success_rate']:<12.1%}")
        
        if 'comparative_insights' in analysis and analysis['comparative_insights']:
            insights = analysis['comparative_insights']
            print(f"\nKey Insights:")
            if 'best_throughput_system' in insights:
                print(f"  Best Throughput: {insights['best_throughput_system']}")
            if 'best_latency_system' in insights:
                print(f"  Best Latency: {insights['best_latency_system']}")
            if 'most_efficient_system' in insights:
                print(f"  Most Efficient: {insights['most_efficient_system']}")
        
        if 'recommendations' in analysis and analysis['recommendations']:
            print(f"\nRecommendations:")
            for i, rec in enumerate(analysis['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        print("\n" + "="*60)

async def main():
    """Main execution function for comparison framework"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Define test configurations
    test_configs = [
        ExperimentConfig("small_scale", 2000, 20, 300, "mixed", 500),
        ExperimentConfig("medium_scale", 5000, 40, 300, "mixed", 1200),
        ExperimentConfig("large_scale", 8000, 60, 300, "mixed", 2000),
        ExperimentConfig("max_scale", 12000, 80, 300, "mixed", 3000)
    ]
    
    # Define systems to compare
    systems_to_test = ['optimized', 'standard', 'enhanced', 'basic']
    
    # Run comparison
    framework = ComparisonFramework()
    
    logger.info("Starting blockchain system comparison...")
    await framework.run_comparison_suite(systems_to_test, test_configs)
    
    logger.info("Analyzing results...")
    analysis = framework.analyze_results()
    
    logger.info("Saving results...")
    saved_files = framework.save_results(analysis)
    
    # Print results
    framework.print_summary(analysis)
    
    print(f"\nResults saved to:")
    for file_type, file_path in saved_files.items():
        print(f"  {file_type}: {file_path}")

if __name__ == "__main__":
    asyncio.run(main())