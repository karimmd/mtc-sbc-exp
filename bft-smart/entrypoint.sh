#!/bin/bash

# BFT-Smart Orderer Entry Point Script

set -e

echo "Starting BFT-Smart Consensus Orderer for blockchain"
echo "Replica ID: ${BFT_REPLICA_ID}"
echo "Tier Location: ${BFT_TIER_LOCATION}"

# Validate required environment variables
if [ -z "$BFT_REPLICA_ID" ]; then
    echo "ERROR: BFT_REPLICA_ID environment variable is required"
    exit 1
fi

if [ -z "$BFT_TIER_LOCATION" ]; then
    echo "ERROR: BFT_TIER_LOCATION environment variable is required"
    exit 1
fi

# Wait for configuration file to be available
if [ ! -f "${BFT_CONFIG_PATH:-/app/config/bftsmart.config}" ]; then
    echo "Waiting for BFT-Smart configuration..."
    sleep 10
fi

# Generate cryptographic keys if they don't exist
if [ ! -f "/app/keys/replica${BFT_REPLICA_ID}.key" ]; then
    echo "Generating cryptographic keys for replica ${BFT_REPLICA_ID}..."
    java -cp /app/bftsmart-orderer.jar bftsmart.communication.client.netty.NettyClientServerCommunicationSystemServerSide \
         -r ${BFT_REPLICA_ID} -generateKeys
fi

# Create logs directory structure
mkdir -p /app/logs/consensus/replica${BFT_REPLICA_ID}

# Start BFT-Smart orderer with proper JVM settings for the tier
if [ "$BFT_TIER_LOCATION" = "cloud" ]; then
    # Cloud tier: High performance settings
    JVM_OPTS="-Xms2g -Xmx4g -XX:+UseG1GC"
elif [ "$BFT_TIER_LOCATION" = "fog" ]; then
    # Fog tier: Medium performance settings
    JVM_OPTS="-Xms1g -Xmx2g -XX:+UseG1GC"
else
    # Edge tier: Resource-constrained settings
    JVM_OPTS="-Xms512m -Xmx1g -XX:+UseSerialGC"
fi

echo "Starting BFT-Smart orderer with JVM options: $JVM_OPTS"

# Start the BFT-Smart orderer
exec java $JVM_OPTS \
    -Djava.security.properties=/app/config/java.security \
    -Dlogback.configurationFile=/app/config/logback.xml \
    -Dbftsmart.communication.defaultkeys=true \
    -Djava.util.logging.config.file=/app/config/logging.properties \
    -cp /app/bftsmart-orderer.jar \
    org.blockchain.consensus.bftsmart.BFTSmartOrderer \
    ${BFT_REPLICA_ID} \
    ${BFT_TIER_LOCATION}