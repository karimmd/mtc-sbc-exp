#!/bin/bash

# blockchain Reputation System Integration Verification
# Verifies subjective logic reputation system operation and blockchain integration

set -e

echo "=== blockchain Reputation System Integration Verification ==="

# Configuration
NODES=("shard-000" "shard-001" "shard-002" "shard-003" "shard-004")
RESULTS_FILE="reputation-verification-results.log"
CHAINCODE_LOG="chaincode-invocation.log"

# Initialize results file
echo "blockchain Reputation System Verification - $(date)" > $RESULTS_FILE
echo "=================================================" >> $RESULTS_FILE

# Function to verify chaincode deployment
verify_chaincode_deployment() {
    echo "Verifying reputation chaincode deployment across shards..." | tee -a $RESULTS_FILE
    
    for node in "${NODES[@]}"; do
        echo "Checking chaincode on ${node}-channel..." >> $RESULTS_FILE
        
        # Query chaincode to verify deployment
        QUERY_RESULT=$(peer chaincode query \
            -C ${node}-channel \
            -n reputation \
            -c '{"function":"GetChainInfo","Args":[]}' \
            2>&1 | tee -a $CHAINCODE_LOG)
        
        if echo "$QUERY_RESULT" | grep -q "Error"; then
            echo "FAILED: Reputation chaincode not deployed on ${node}" >> $RESULTS_FILE
        else
            echo "VERIFIED: Reputation chaincode operational on ${node}" >> $RESULTS_FILE
        fi
    done
    echo "" >> $RESULTS_FILE
}

# Function to verify subjective logic operations
verify_subjective_logic() {
    echo "Verifying subjective logic reputation calculations..." | tee -a $RESULTS_FILE
    
    # Initialize test node with known reputation values
    NODE_ID="test-validator-001"
    INITIAL_BELIEF="0.6"
    INITIAL_DISBELIEF="0.2" 
    INITIAL_UNCERTAINTY="0.2"
    
    echo "Initializing ${NODE_ID} with SL triplet: (${INITIAL_BELIEF}, ${INITIAL_DISBELIEF}, ${INITIAL_UNCERTAINTY})" >> $RESULTS_FILE
    
    peer chaincode invoke \
        -o orderer.local:7050 \
        -C shard-000-channel \
        -n reputation \
        -c "{\"function\":\"InitializeNodeWithSL\",\"Args\":[\"${NODE_ID}\",\"fog\",\"${INITIAL_BELIEF}\",\"${INITIAL_DISBELIEF}\",\"${INITIAL_UNCERTAINTY}\"]}" \
        2>&1 | tee -a $CHAINCODE_LOG
    
    sleep 2
    
    # Query and verify the stored values
    STORED_REPUTATION=$(peer chaincode query \
        -C shard-000-channel \
        -n reputation \
        -c "{\"function\":\"GetNodeReputation\",\"Args\":[\"${NODE_ID}\"]}" \
        2>/dev/null)
    
    if [ ! -z "$STORED_REPUTATION" ]; then
        echo "Stored reputation data: $STORED_REPUTATION" >> $RESULTS_FILE
        
        # Parse subjective logic components
        STORED_BELIEF=$(echo $STORED_REPUTATION | jq -r '.reputationSL.belief // "null"')
        STORED_DISBELIEF=$(echo $STORED_REPUTATION | jq -r '.reputationSL.disbelief // "null"')
        STORED_UNCERTAINTY=$(echo $STORED_REPUTATION | jq -r '.reputationSL.uncertainty // "null"')
        
        echo "Retrieved SL components: belief=${STORED_BELIEF}, disbelief=${STORED_DISBELIEF}, uncertainty=${STORED_UNCERTAINTY}" >> $RESULTS_FILE
        
        # Verify mathematical constraints: b + d + u = 1
        if [ "$STORED_BELIEF" != "null" ] && [ "$STORED_DISBELIEF" != "null" ] && [ "$STORED_UNCERTAINTY" != "null" ]; then
            TOTAL=$(echo "$STORED_BELIEF + $STORED_DISBELIEF + $STORED_UNCERTAINTY" | bc -l)
            echo "Sum verification: ${STORED_BELIEF} + ${STORED_DISBELIEF} + ${STORED_UNCERTAINTY} = ${TOTAL}" >> $RESULTS_FILE
        fi
    else
        echo "ERROR: Failed to retrieve reputation data for ${NODE_ID}" >> $RESULTS_FILE
    fi
    echo "" >> $RESULTS_FILE
}

# Function to test feedback processing
verify_feedback_processing() {
    echo "Verifying feedback processing and reputation updates..." | tee -a $RESULTS_FILE
    
    NODE_ID="test-validator-002"
    
    # Initialize node
    peer chaincode invoke \
        -o orderer.local:7050 \
        -C shard-001-channel \
        -n reputation \
        -c "{\"function\":\"InitializeNode\",\"Args\":[\"${NODE_ID}\",\"edge\",\"0.5\",\"0\",\"0\"]}" \
        2>&1 >> $CHAINCODE_LOG
    
    sleep 2
    
    # Record initial reputation
    INITIAL_REP=$(peer chaincode query \
        -C shard-001-channel \
        -n reputation \
        -c "{\"function\":\"GetNodeReputation\",\"Args\":[\"${NODE_ID}\"]}" \
        2>/dev/null)
    
    echo "Initial reputation state: $INITIAL_REP" >> $RESULTS_FILE
    
    # Submit positive feedback
    peer chaincode invoke \
        -o orderer.local:7050 \
        -C shard-001-channel \
        -n reputation \
        -c "{\"function\":\"SubmitFeedback\",\"Args\":[\"${NODE_ID}\",\"feedback-tx-001\",\"true\",\"consensus_validation\"]}" \
        2>&1 >> $CHAINCODE_LOG
    
    sleep 2
    
    # Record updated reputation
    UPDATED_REP=$(peer chaincode query \
        -C shard-001-channel \
        -n reputation \
        -c "{\"function\":\"GetNodeReputation\",\"Args\":[\"${NODE_ID}\"]}" \
        2>/dev/null)
    
    echo "Updated reputation state: $UPDATED_REP" >> $RESULTS_FILE
    
    # Compare reputation values
    if [ ! -z "$INITIAL_REP" ] && [ ! -z "$UPDATED_REP" ]; then
        INITIAL_SUCCESS=$(echo $INITIAL_REP | jq -r '.successCount // 0')
        UPDATED_SUCCESS=$(echo $UPDATED_REP | jq -r '.successCount // 0')
        
        echo "Success count change: ${INITIAL_SUCCESS} -> ${UPDATED_SUCCESS}" >> $RESULTS_FILE
        
        if [ "$UPDATED_SUCCESS" -gt "$INITIAL_SUCCESS" ]; then
            echo "VERIFIED: Positive feedback correctly processed" >> $RESULTS_FILE
        else
            echo "ERROR: Positive feedback processing failed" >> $RESULTS_FILE
        fi
    fi
    echo "" >> $RESULTS_FILE
}

# Function to test cross-shard reputation queries
verify_cross_shard_integration() {
    echo "Verifying cross-shard reputation integration..." | tee -a $RESULTS_FILE
    
    VALIDATOR_ID="cross-shard-validator"
    
    # Register validator in multiple shards
    for i in {0..2}; do
        SHARD_ID="shard-$(printf "%03d" $i)"
        
        echo "Registering ${VALIDATOR_ID} in ${SHARD_ID}" >> $RESULTS_FILE
        
        peer chaincode invoke \
            -o orderer.local:7050 \
            -C ${SHARD_ID}-channel \
            -n reputation \
            -c "{\"function\":\"RegisterValidator\",\"Args\":[\"${VALIDATOR_ID}\",\"fog\",\"${SHARD_ID}\"]}" \
            2>&1 >> $CHAINCODE_LOG
    done
    
    sleep 3
    
    # Query validator presence across shards
    for i in {0..2}; do
        SHARD_ID="shard-$(printf "%03d" $i)"
        
        VALIDATOR_DATA=$(peer chaincode query \
            -C ${SHARD_ID}-channel \
            -n reputation \
            -c "{\"function\":\"GetNodeReputation\",\"Args\":[\"${VALIDATOR_ID}\"]}" \
            2>/dev/null)
        
        if [ ! -z "$VALIDATOR_DATA" ] && [ "$VALIDATOR_DATA" != "null" ]; then
            echo "VERIFIED: ${VALIDATOR_ID} found in ${SHARD_ID}" >> $RESULTS_FILE
        else
            echo "ERROR: ${VALIDATOR_ID} not found in ${SHARD_ID}" >> $RESULTS_FILE
        fi
    done
    echo "" >> $RESULTS_FILE
}

# Function to verify reputation-based allocation
verify_reputation_based_allocation() {
    echo "Verifying reputation-based shard allocation..." | tee -a $RESULTS_FILE
    
    # Create transaction requiring minimum reputation threshold
    TX_ID="allocation-test-tx"
    MIN_REPUTATION="0.7"
    REQUIRED_VALIDATORS="3"
    
    echo "Testing allocation for TX ${TX_ID} with min reputation ${MIN_REPUTATION}" >> $RESULTS_FILE
    
    ALLOCATION_RESULT=$(peer chaincode invoke \
        -o orderer.local:7050 \
        -C shard-000-channel \
        -n sharding \
        -c "{\"function\":\"AllocateShardByReputation\",\"Args\":[\"${TX_ID}\",\"${MIN_REPUTATION}\",\"${REQUIRED_VALIDATORS}\"]}" \
        2>&1)
    
    echo "Allocation invocation result: $ALLOCATION_RESULT" >> $RESULTS_FILE
    
    sleep 2
    
    # Query allocation results
    ALLOCATION_DATA=$(peer chaincode query \
        -C shard-000-channel \
        -n sharding \
        -c "{\"function\":\"GetShardAllocation\",\"Args\":[\"${TX_ID}\"]}" \
        2>/dev/null)
    
    if [ ! -z "$ALLOCATION_DATA" ] && [ "$ALLOCATION_DATA" != "null" ]; then
        echo "Allocation data retrieved: $ALLOCATION_DATA" >> $RESULTS_FILE
        
        # Parse allocated nodes
        ALLOCATED_NODES=$(echo $ALLOCATION_DATA | jq -r '.allocatedNodes[]?' 2>/dev/null | wc -l)
        if [ "$ALLOCATED_NODES" -ge "$REQUIRED_VALIDATORS" ]; then
            echo "VERIFIED: Allocation met validator requirements (${ALLOCATED_NODES} >= ${REQUIRED_VALIDATORS})" >> $RESULTS_FILE
        else
            echo "WARNING: Insufficient validators allocated (${ALLOCATED_NODES} < ${REQUIRED_VALIDATORS})" >> $RESULTS_FILE
        fi
    else
        echo "ERROR: Failed to retrieve allocation data" >> $RESULTS_FILE
    fi
    echo "" >> $RESULTS_FILE
}

# Function to test system under load
verify_performance_characteristics() {
    echo "Verifying reputation system performance characteristics..." | tee -a $RESULTS_FILE
    
    START_TIME=$(date +%s)
    OPERATIONS_COUNT=0
    SUCCESS_COUNT=0
    
    echo "Starting performance verification for 30 seconds..." >> $RESULTS_FILE
    
    # Run operations for 30 seconds
    while [ $(($(date +%s) - START_TIME)) -lt 30 ]; do
        NODE_ID="perf-test-$(date +%s%N | cut -c14-19)"
        TX_ID="perf-tx-$(date +%s%N | cut -c14-19)"
        
        # Initialize node
        peer chaincode invoke \
            -o orderer.local:7050 \
            -C shard-000-channel \
            -n reputation \
            -c "{\"function\":\"InitializeNode\",\"Args\":[\"${NODE_ID}\",\"fog\",\"0.6\",\"0\",\"0\"]}" \
            2>/dev/null
        
        # Submit feedback
        FEEDBACK_RESULT=$(peer chaincode invoke \
            -o orderer.local:7050 \
            -C shard-000-channel \
            -n reputation \
            -c "{\"function\":\"SubmitFeedback\",\"Args\":[\"${NODE_ID}\",\"${TX_ID}\",\"true\",\"performance_test\"]}" \
            2>&1)
        
        ((OPERATIONS_COUNT++))
        
        if ! echo "$FEEDBACK_RESULT" | grep -q "Error"; then
            ((SUCCESS_COUNT++))
        fi
        
        # Rate limiting
        sleep 0.1
    done
    
    ELAPSED_TIME=$(($(date +%s) - START_TIME))
    SUCCESS_RATE=$(echo "scale=2; $SUCCESS_COUNT * 100 / $OPERATIONS_COUNT" | bc -l)
    OPERATIONS_PER_SECOND=$(echo "scale=2; $OPERATIONS_COUNT / $ELAPSED_TIME" | bc -l)
    
    echo "Performance results:" >> $RESULTS_FILE
    echo "  Total operations: $OPERATIONS_COUNT" >> $RESULTS_FILE
    echo "  Successful operations: $SUCCESS_COUNT" >> $RESULTS_FILE
    echo "  Success rate: ${SUCCESS_RATE}%" >> $RESULTS_FILE
    echo "  Operations per second: $OPERATIONS_PER_SECOND" >> $RESULTS_FILE
    echo "  Test duration: ${ELAPSED_TIME} seconds" >> $RESULTS_FILE
    echo "" >> $RESULTS_FILE
}

# Function to generate verification summary
generate_verification_summary() {
    echo "=== Verification Summary ===" >> $RESULTS_FILE
    echo "Verification completed: $(date)" >> $RESULTS_FILE
    echo "" >> $RESULTS_FILE
    
    # Count verification results
    VERIFIED_COUNT=$(grep -c "VERIFIED:" $RESULTS_FILE || echo "0")
    ERROR_COUNT=$(grep -c "ERROR:" $RESULTS_FILE || echo "0")
    WARNING_COUNT=$(grep -c "WARNING:" $RESULTS_FILE || echo "0")
    
    echo "Results summary:" >> $RESULTS_FILE
    echo "  Verified operations: $VERIFIED_COUNT" >> $RESULTS_FILE
    echo "  Errors encountered: $ERROR_COUNT" >> $RESULTS_FILE
    echo "  Warnings issued: $WARNING_COUNT" >> $RESULTS_FILE
    echo "" >> $RESULTS_FILE
    
    echo "Log files generated:" >> $RESULTS_FILE
    echo "  Verification results: $RESULTS_FILE" >> $RESULTS_FILE
    echo "  Chaincode invocations: $CHAINCODE_LOG" >> $RESULTS_FILE
    
    # Display summary to console
    echo "Verification completed. Results written to: $RESULTS_FILE"
    echo "Verified operations: $VERIFIED_COUNT | Errors: $ERROR_COUNT | Warnings: $WARNING_COUNT"
}

# Main verification execution
main() {
    echo "Starting blockchain reputation system integration verification..."
    echo "Results will be logged to: $RESULTS_FILE"
    echo ""
    
    verify_chaincode_deployment
    verify_subjective_logic
    verify_feedback_processing
    verify_cross_shard_integration
    verify_reputation_based_allocation
    verify_performance_characteristics
    generate_verification_summary
    
    echo "Integration verification completed."
}

# Execute verification
main "$@"