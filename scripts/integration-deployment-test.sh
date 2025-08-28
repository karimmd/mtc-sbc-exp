#!/bin/bash

# blockchain Integration Test Script

set -e

echo "blockchain Integration Test"
echo "Timestamp: $(date)"

# Configuration
COMPOSE_FILE="docker-compose.yml"
NETWORK_NAME="blockchain-network"
RESULTS_DIR="./results/integration_test_$(date +%Y%m%d_%H%M%S)"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "Step 1: Environment Setup"
echo "=========================="

# Check Docker and Docker Compose
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "Docker environment verified"

# Check if previous containers are running
if docker ps -a --format "table {{.Names}}" | grep -q "blockchain"; then
    echo "Stopping existing blockchain containers..."
    docker-compose -f "$COMPOSE_FILE" down --volumes --remove-orphans
fi

echo ""
echo "Step 2: Network Infrastructure Deployment"
echo "=========================================="

# Start the blockchain network first (orderer, peers, CA)
echo "Starting Hyperledger Fabric network..."
docker-compose -f "$COMPOSE_FILE" up -d ca.local orderer.local

# Wait for CA to be ready
echo "Waiting for Certificate Authority to be ready..."
timeout=60
while ! docker exec ca.local fabricca-client version &>/dev/null && [ $timeout -gt 0 ]; do
    sleep 2
    timeout=$((timeout-2))
    echo -n "."
done
echo ""

if [ $timeout -le 0 ]; then
    echo "Certificate Authority failed to start within timeout"
    exit 1
fi

echo "Certificate Authority is ready"

# Start peers
echo "Starting Hyperledger Fabric peers..."
docker-compose -f "$COMPOSE_FILE" up -d peer0.cloud.local peer0.fog.local peer0.edge.local

# Wait for peers to be ready
sleep 10
echo "Hyperledger Fabric network is ready"

echo ""
echo "Step 3: Python Service Integration"
echo "=================================="

# Start Python services with emulated multi-tier environment
echo "Starting Python experimental framework..."
docker-compose -f "$COMPOSE_FILE" up -d blockchain-cloud-service blockchain-fog-service blockchain-edge-service

# Wait for Python services to be ready
echo "Waiting for Python services to initialize..."
sleep 15

# Check service health
echo "Checking service health..."
for service in blockchain-cloud-service blockchain-fog-service blockchain-edge-service; do
    if docker ps --filter "name=$service" --filter "status=running" | grep -q "$service"; then
        echo "$service is running"
        
        # Get service logs to verify initialization
        docker logs "$service" > "$RESULTS_DIR/${service}_startup.log" 2>&1
    else
        echo "$service failed to start"
        docker logs "$service" > "$RESULTS_DIR/${service}_error.log" 2>&1
        exit 1
    fi
done

echo ""
echo "Step 4: Integration Testing"
echo "==========================="

# Start the main controller which runs integrated evaluation
echo "Starting integrated evaluation..."
docker-compose -f "$COMPOSE_FILE" up -d blockchain-controller

# Monitor the evaluation process
echo "Monitoring integration test execution..."
timeout=300  # 5 minutes timeout for evaluation
start_time=$(date +%s)

while docker ps --filter "name=blockchain-controller" --filter "status=running" | grep -q blockchain-controller && [ $timeout -gt 0 ]; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    echo "Integration test running... (${elapsed}s elapsed)"
    
    # Show recent logs
    docker logs --tail 5 blockchain-controller 2>/dev/null | grep -v "^$" || true
    
    sleep 10
    timeout=$((timeout-10))
done

echo ""
echo "Step 5: Results Collection"
echo "=========================="

# Collect container logs
echo "Collecting container logs..."
for container in blockchain-controller blockchain-cloud-service blockchain-fog-service blockchain-edge-service; do
    if docker ps -a --filter "name=$container" | grep -q "$container"; then
        echo "Collecting logs from $container..."
        docker logs "$container" > "$RESULTS_DIR/${container}_full.log" 2>&1
    fi
done

# Copy results from controller container
echo "Copying evaluation results..."
if docker ps -a --filter "name=blockchain-controller" | grep -q blockchain-controller; then
    # Try to copy results directory from container
    docker cp blockchain-controller:/app/results/. "$RESULTS_DIR/evaluation_results/" 2>/dev/null || {
        echo "⚠️  Could not copy results from container (this is expected if container exited)"
    }
fi

# Check if integration test completed successfully
if docker logs blockchain-controller 2>&1 | grep -q "Integration evaluation completed successfully"; then
    echo "Integration test PASSED"
    test_result="PASSED"
else
    echo "Integration test FAILED"
    test_result="FAILED"
fi

echo ""
echo "Step 6: Network Verification"
echo "============================"

# Verify network connectivity between components
echo "Verifying network connectivity..."

# Check if all containers can communicate
network_test_results=""

# Test Python service to peer connectivity
for tier in cloud fog edge; do
    service_name="blockchain-${tier}-service"
    peer_name="peer0.${tier}.local"
    
    if docker exec "$service_name" ping -c 1 "$peer_name" &>/dev/null; then
        echo "$service_name can reach $peer_name"
        network_test_results+="\n$service_name <-> $peer_name: CONNECTED"
    else
        echo "$service_name cannot reach $peer_name"
        network_test_results+="\n$service_name <-> $peer_name: FAILED"
    fi
done

echo ""
echo "Step 7: Final Report"
echo "===================="

# Generate summary report
cat > "$RESULTS_DIR/integration_test_summary.txt" << EOF
blockchain Integration Deployment Test Results
===========================================
Test Date: $(date)
Test Result: $test_result

Infrastructure:
- Hyperledger Fabric Network: DEPLOYED
- Python Multi-Tier Emulation: DEPLOYED
- Docker Network: $NETWORK_NAME

Components Tested:
- Certificate Authority (ca.local)
- Orderer Service (orderer.local)
- Cloud Peer (peer0.cloud.local)
- Fog Peer (peer0.fog.local) 
- Edge Peer (peer0.edge.local)
- Python Cloud Service Emulator
- Python Fog Service Emulator
- Python Edge Service Emulator
- Integration Controller

Network Connectivity Tests:$network_test_results

Results Location: $RESULTS_DIR
EOF

echo "Integration test completed!"
echo "Results saved to: $RESULTS_DIR"
echo ""
echo "=== SUMMARY ==="
echo "Test Result: $test_result"
echo "Components: $(docker ps --filter 'name=blockchain' --format '{{.Names}}' | wc -l) containers running"
echo "Logs: Available in $RESULTS_DIR"
echo ""

# Clean up option
read -p "Do you want to stop the containers? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping containers..."
    docker-compose -f "$COMPOSE_FILE" down --volumes
    echo "Cleanup completed"
else
    echo "Containers left running. Use 'docker-compose down' to stop them later."
fi

echo ""
echo "Integration deployment test completed"