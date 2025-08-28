# BFT-Smart Integration Dockerfile for blockchain
FROM openjdk:8-jdk-slim

# Install Maven for building BFT-Smart integration
RUN apt-get update && apt-get install -y \
    maven \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Maven configuration and source code
COPY pom.xml .
COPY java/ ./src/main/java/org/blockchain/consensus/bftsmart/

# Download BFT-Smart library (version 1.2.0)
RUN wget -P /tmp https://github.com/bft-smart/library/releases/download/v1.2.0/bftsmart-library-1.2.0.jar \
    && mvn install:install-file \
       -Dfile=/tmp/bftsmart-library-1.2.0.jar \
       -DgroupId=bftsmart \
       -DartifactId=library \
       -Dversion=1.2.0 \
       -Dpackaging=jar

# Build the BFT-Smart integration
RUN mvn clean package

# Create directories for configuration and logs
RUN mkdir -p /app/config /app/logs/consensus /app/keys

# Copy the built JAR
RUN cp target/bft-smart-integration-1.0.0.jar /app/bftsmart-orderer.jar

# Expose BFT-Smart consensus ports
EXPOSE 11000-11006

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD java -cp /app/bftsmart-orderer.jar org.blockchain.consensus.bftsmart.HealthCheck || exit 1

# Entry point script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]