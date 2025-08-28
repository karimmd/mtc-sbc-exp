#!/bin/bash

# blockchain Blockchain Deployment Script
# Deploys the multi-tier blockchain infrastructure

set -e

echo "=== blockchain Blockchain Deployment ==="
echo "Initializing multi-tier blockchain..."

# Configuration
BLOCKCHAIN_CONFIG="config/blockchain_deployment.yaml"
FABRIC_VERSION="2.4.0"
DOCKER_COMPOSE_FILE="docker-compose.blockchain.yml"

# Check prerequisites
echo "Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting." >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting." >&2; exit 1; }

# Create necessary directories
echo "Setting up directory structure..."
mkdir -p fabric-config/{cloud-peer,fog-peer,edge-peer}/msp
mkdir -p data/experimental_results
mkdir -p logs/blockchain
mkdir -p monitoring/{prometheus,grafana/dashboards}

# Generate Fabric network configuration
echo "Generating Hyperledger Fabric network configuration..."
python3 scripts/generate_fabric_config.py

# Pull Docker images
echo "Pulling Hyperledger Fabric Docker images..."
docker pull hyperledger/fabric-peer:$FABRIC_VERSION
docker pull hyperledger/fabric-orderer:$FABRIC_VERSION
docker pull hyperledger/fabric-ca:$FABRIC_VERSION

# Start the blockchain infrastructure
echo "Starting blockchain infrastructure..."
docker-compose -f $DOCKER_COMPOSE_FILE up -d

# Wait for fabric network to be ready
echo "Waiting for Fabric network to initialize..."
sleep 30

# Create channels and deploy chaincode
echo "Setting up blockchain channels and deploying chaincode..."
./scripts/setup_fabric_network.sh

# Initialize reputation system
echo "Initializing reputation management system..."
python3 src/reputation/initialize_reputation.py

# Start baseline systems for comparison
echo "Starting baseline comparison systems..."
# Additional deployments can be configured here
echo "Main blockchain deployment completed"

# Verify blockchain deployment
echo "Verifying blockchain deployment..."
python3 scripts/verify_blockchain.py

echo "=== Blockchain deployment completed successfully ==="
echo "Access points:"
echo "  - Grafana Dashboard: http://localhost:3000"
echo "  - Prometheus Metrics: http://localhost:9090"
echo "  - blockchain API: http://localhost:8080"
echo ""
echo "Run experiments with: ./scripts/run_experiments.sh"