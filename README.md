Testbed implementation for MTC-SBC

```
├── bft-smart/
│   ├── Dockerfile
│   ├── config/
│   │   └── bftsmart.config
│   ├── entrypoint.sh
│   ├── java/
│   │   ├── BFTSmartOrderer.java
│   │   └── TransactionBatch.java
│   └── pom.xml
├── chaincodes/
│   ├── reputation/
│   │   ├── go.mod
│   │   └── reputation.go
│   └── sharding/
│       ├── go.mod
│       ├── sharding.go
│       └── blockchain_sharding.go
├── config/
│   ├── hardware-specifications.yaml
│   ├── service_config.yaml
│   └── blockchain_deployment.yaml
├── deployment/
│   └── multi-tier-infrastructure.yaml
├── experiments/
│   ├── baseline_comparison.py
│   ├── comparison_framework.py
│   ├── data_collector.py
│   ├── integrated_evaluation.py
│   ├── service_controller.py
│   └── simple_evaluation.py
├── optimization/
│   ├── __init__.py
│   ├── admm_solver.py
│   ├── complexity_analysis.py
│   ├── requirements.txt
│   └── rsoc_constraints.py
├── network/
│   ├── compose-blockchain.yaml
│   ├── compose-services.yaml
│   ├── configtx.yaml
│   └── network-slicing-manifest.yaml
├── scripts/
│   ├── deploy-sharding-network.sh
│   ├── deploy_blockchain.sh
│   ├── integration-deployment-test.sh
│   └── reputation-integration-test.sh
├── src/
│   ├── blockchain/
│   │   ├── bft_smart.py
│   │   ├── hyperledger_fabric.py
│   │   └── sharding.py
│   ├── core/
│   │   ├── data_collector.py
│   │   ├── node.py
│   │   └── blockchain_manager.py
│   ├── networking/
│   │   ├── flask_services.py
│   │   └── slicing.py
│   ├── optimization/
│   │   └── resource_optimizer.py
│   └── reputation/
│       └── subjective_logic.py
├── Dockerfile.python
├── docker-compose.blockchain.yml
├── docker-compose.yml
├── requirements.txt
├── run_experiments.py
└── setup.py
```