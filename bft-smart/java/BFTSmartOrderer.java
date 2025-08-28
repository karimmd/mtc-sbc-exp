package org.blockchain.consensus.bftsmart;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import bftsmart.reconfiguration.ViewManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.logging.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BFT-Smart Integration for Hyperledger Fabric Orderer
 * Implements Byzantine fault-tolerant consensus with dynamic reconfiguration
 * for multi-tier computing environments
 */
public class BFTSmartOrderer extends DefaultSingleRecoverable {
    
    private static final Logger logger = Logger.getLogger(BFTSmartOrderer.class.getName());
    
    private final ServiceReplica replica;
    private final int replicaId;
    private final String tierLocation;
    
    // Transaction ordering state
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final ConcurrentHashMap<Long, byte[]> orderedTransactions = new ConcurrentHashMap<>();
    
    // Multi-tier specific metrics
    private long consensusTimeSum = 0;
    private long transactionCount = 0;
    private final ConcurrentHashMap<String, Double> tierPerformance = new ConcurrentHashMap<>();
    
    public BFTSmartOrderer(int replicaId, String tierLocation) {
        this.replicaId = replicaId;
        this.tierLocation = tierLocation;
        
        // Initialize BFT-Smart replica with multi-tier aware configuration
        this.replica = new ServiceReplica(replicaId, this, this);
        
        logger.info(String.format("BFT-Smart Orderer initialized - Replica %d on %s tier", 
                                 replicaId, tierLocation));
        
        // Initialize tier performance tracking
        initializeTierMetrics();
    }
    
    private void initializeTierMetrics() {
        tierPerformance.put("cloud", 0.0);
        tierPerformance.put("fog", 0.0); 
        tierPerformance.put("edge", 0.0);
    }
    
    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        long startTime = System.nanoTime();
        
        try {
            // Process transaction ordering request from Fabric
            TransactionBatch batch = deserializeTransactionBatch(command);
            
            // Assign sequence numbers and order transactions
            long sequenceNumber = sequenceCounter.incrementAndGet();
            batch.setSequenceNumber(sequenceNumber);
            batch.setConsensusTimestamp(System.currentTimeMillis());
            
            // Store ordered transactions for Fabric delivery
            byte[] orderedBatch = serializeTransactionBatch(batch);
            orderedTransactions.put(sequenceNumber, orderedBatch);
            
            // Update performance metrics
            long consensusTime = System.nanoTime() - startTime;
            updateConsensusMetrics(consensusTime, msgCtx);
            
            logger.info(String.format("Ordered transaction batch %d with %d transactions (consensus time: %d ms)",
                                     sequenceNumber, batch.getTransactionCount(), consensusTime / 1_000_000));
            
            return orderedBatch;
            
        } catch (Exception e) {
            logger.severe("Error in transaction ordering: " + e.getMessage());
            return new byte[0];
        }
    }
    
    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        try {
            // Handle queries and status requests from Fabric
            String query = new String(command);
            
            if (query.equals("GET_STATUS")) {
                return getOrdererStatus().getBytes();
            } else if (query.equals("GET_METRICS")) {
                return getPerformanceMetrics().getBytes();
            } else if (query.startsWith("GET_SEQUENCE_")) {
                long seqNum = Long.parseLong(query.substring(13));
                byte[] transaction = orderedTransactions.get(seqNum);
                return transaction != null ? transaction : new byte[0];
            }
            
            return new byte[0];
            
        } catch (Exception e) {
            logger.warning("Error handling unordered request: " + e.getMessage());
            return new byte[0];
        }
    }
    
    private void updateConsensusMetrics(long consensusTime, MessageContext msgCtx) {
        consensusTimeSum += consensusTime;
        transactionCount++;
        
        // Update tier-specific performance based on message context
        String sourceTier = determineTierFromContext(msgCtx);
        if (sourceTier != null) {
            double avgTime = tierPerformance.get(sourceTier);
            double newAvg = (avgTime + (consensusTime / 1_000_000.0)) / 2.0;
            tierPerformance.put(sourceTier, newAvg);
        }
    }
    
    private String determineTierFromContext(MessageContext msgCtx) {
        // Determine source tier based on sender ID
        int senderId = msgCtx.getSender();
        if (senderId <= 1) return "cloud";
        else if (senderId <= 4) return "fog";
        else return "edge";
    }
    
    private String getOrdererStatus() {
        return String.format("BFT-Smart Orderer Status - Replica %d (%s tier)\n" +
                           "Processed transactions: %d\n" +
                           "Average consensus time: %.2f ms\n" +
                           "Current sequence: %d",
                           replicaId, tierLocation, transactionCount,
                           (transactionCount > 0 ? (consensusTimeSum / 1_000_000.0) / transactionCount : 0),
                           sequenceCounter.get());
    }
    
    private String getPerformanceMetrics() {
        StringBuilder metrics = new StringBuilder();
        metrics.append("Multi-Tier Consensus Performance:\n");
        for (String tier : tierPerformance.keySet()) {
            metrics.append(String.format("%s tier avg time: %.2f ms\n", 
                                       tier, tierPerformance.get(tier)));
        }
        return metrics.toString();
    }
    
    private TransactionBatch deserializeTransactionBatch(byte[] data) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = new ObjectInputStream(bis);
        return (TransactionBatch) in.readObject();
    }
    
    private byte[] serializeTransactionBatch(TransactionBatch batch) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(batch);
        out.flush();
        return bos.toByteArray();
    }
    
    @Override
    public void installSnapshot(byte[] state) {
        try {
            // Restore orderer state from snapshot for crash recovery
            logger.info("Installing consensus state snapshot");
            // Implementation would restore orderedTransactions and sequenceCounter
        } catch (Exception e) {
            logger.severe("Error installing snapshot: " + e.getMessage());
        }
    }
    
    @Override
    public byte[] getSnapshot() {
        try {
            // Create snapshot of current orderer state
            logger.info("Creating consensus state snapshot");
            // Implementation would serialize orderedTransactions and sequenceCounter
            return new byte[0];
        } catch (Exception e) {
            logger.severe("Error creating snapshot: " + e.getMessage());
            return new byte[0];
        }
    }
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java BFTSmartOrderer <replica_id> <tier_location>");
            System.exit(1);
        }
        
        int replicaId = Integer.parseInt(args[0]);
        String tierLocation = args[1];
        
        new BFTSmartOrderer(replicaId, tierLocation);
        
        // Keep the orderer running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("BFT-Smart orderer shutting down");
        }
    }
}