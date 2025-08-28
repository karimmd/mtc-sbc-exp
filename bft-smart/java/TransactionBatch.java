package org.blockchain.consensus.bftsmart;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

/**
 * Transaction batch for BFT-Smart consensus ordering
 * Represents a group of transactions to be ordered atomically
 */
public class TransactionBatch implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private List<byte[]> transactions;
    private long sequenceNumber;
    private long consensusTimestamp;
    private String batchId;
    private int transactionCount;
    
    public TransactionBatch() {
        this.transactions = new ArrayList<>();
        this.batchId = generateBatchId();
    }
    
    public TransactionBatch(List<byte[]> transactions) {
        this.transactions = new ArrayList<>(transactions);
        this.transactionCount = transactions.size();
        this.batchId = generateBatchId();
    }
    
    private String generateBatchId() {
        return "batch_" + System.currentTimeMillis() + "_" + System.nanoTime();
    }
    
    public void addTransaction(byte[] transaction) {
        this.transactions.add(transaction);
        this.transactionCount = transactions.size();
    }
    
    public List<byte[]> getTransactions() {
        return new ArrayList<>(transactions);
    }
    
    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    
    public void setConsensusTimestamp(long timestamp) {
        this.consensusTimestamp = timestamp;
    }
    
    public long getConsensusTimestamp() {
        return consensusTimestamp;
    }
    
    public String getBatchId() {
        return batchId;
    }
    
    public int getTransactionCount() {
        return transactionCount;
    }
    
    @Override
    public String toString() {
        return String.format("TransactionBatch{id=%s, seq=%d, count=%d, timestamp=%d}",
                           batchId, sequenceNumber, transactionCount, consensusTimestamp);
    }
}