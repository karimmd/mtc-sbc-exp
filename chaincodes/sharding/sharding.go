package main

import (
	"encoding/json"
	"fmt"
	"log" 
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

// ShardingContract provides functions for managing blockchain shards with reputation-based allocation
type ShardingContract struct {
	contractapi.Contract
}

// Shard represents a blockchain shard
type Shard struct {
	ShardID         string   `json:"shardID"`
	ValidatorNodes  []string `json:"validatorNodes"`
	MinReputation   float64  `json:"minReputation"`
	MaxCapacity     int      `json:"maxCapacity"`
	CurrentLoad     int      `json:"currentLoad"`
	BlockHeight     int64    `json:"blockHeight"`
	Status          string   `json:"status"` // active, inactive, maintenance
	CreatedAt       int64    `json:"createdAt"`
	LastUpdated     int64    `json:"lastUpdated"`
	TransactionPool []string `json:"transactionPool"`
}

// Cross-shard transaction for handling transactions across multiple shards
type CrossShardTransaction struct {
	TxID            string            `json:"txID"`
	SourceShard     string            `json:"sourceShard"`
	TargetShards    []string          `json:"targetShards"`
	Status          string            `json:"status"` // pending, committed, aborted
	CommitProofs    map[string]string `json:"commitProofs"`
	CreatedAt       int64             `json:"createdAt"`
	CompletedAt     int64             `json:"completedAt,omitempty"`
}

// ShardMetrics contains performance metrics for a shard
type ShardMetrics struct {
	ShardID           string  `json:"shardID"`
	ThroughputTPS     float64 `json:"throughputTPS"`
	AverageLatencyMs  float64 `json:"averageLatencyMs"`
	ValidationSuccess float64 `json:"validationSuccessRate"`
	NodeUtilization   float64 `json:"nodeUtilization"`
	LastCalculated    int64   `json:"lastCalculated"`
}

// BFTConsensusState represents BFT-Smart consensus state for a shard
type BFTConsensusState struct {
	ShardID        string            `json:"shardID"`
	View           int64             `json:"view"`
	Sequence       int64             `json:"sequence"`
	Phase          string            `json:"phase"` // prepare, commit, reply
	Participants   []string          `json:"participants"`
	Votes          map[string]string `json:"votes"`
	CommittedBlock string            `json:"committedBlock,omitempty"`
	Timestamp      int64             `json:"timestamp"`
}

// InitLedger initializes the sharding system with default shards
func (sc *ShardingContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
	// Initialize default shards for different tiers
	defaultShards := []Shard{
		{
			ShardID:         "shard_cloud_001",
			ValidatorNodes:  []string{"cloud-node-1", "cloud-node-2"},
			MinReputation:   0.7,
			MaxCapacity:     1000,
			CurrentLoad:     0,
			BlockHeight:     0,
			Status:          "active",
			CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
			LastUpdated:     ctx.GetStub().GetTxTimestamp().GetSeconds(),
			TransactionPool: []string{},
		},
		{
			ShardID:         "shard_fog_001", 
			ValidatorNodes:  []string{"fog-node-1", "fog-node-2", "fog-node-3"},
			MinReputation:   0.5,
			MaxCapacity:     500,
			CurrentLoad:     0,
			BlockHeight:     0,
			Status:          "active",
			CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
			LastUpdated:     ctx.GetStub().GetTxTimestamp().GetSeconds(),
			TransactionPool: []string{},
		},
		{
			ShardID:         "shard_edge_001",
			ValidatorNodes:  []string{"edge-node-1", "edge-node-2", "edge-node-3", "edge-node-4"},
			MinReputation:   0.3,
			MaxCapacity:     200,
			CurrentLoad:     0,
			BlockHeight:     0,
			Status:          "active", 
			CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
			LastUpdated:     ctx.GetStub().GetTxTimestamp().GetSeconds(),
			TransactionPool: []string{},
		},
	}

	for _, shard := range defaultShards {
		shardJSON, err := json.Marshal(shard)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState("shard_"+shard.ShardID, shardJSON)
		if err != nil {
			return fmt.Errorf("failed to put shard to world state: %v", err)
		}

		// Initialize BFT consensus state for each shard
		consensusState := BFTConsensusState{
			ShardID:      shard.ShardID,
			View:         0,
			Sequence:     0,
			Phase:        "prepare",
			Participants: shard.ValidatorNodes,
			Votes:        make(map[string]string),
			Timestamp:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
		}

		consensusJSON, err := json.Marshal(consensusState)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState("bft_consensus_"+shard.ShardID, consensusJSON)
		if err != nil {
			return fmt.Errorf("failed to put BFT consensus state: %v", err)
		}
	}

	return nil
}

// CreateShard creates a new shard with specified parameters
func (sc *ShardingContract) CreateShard(ctx contractapi.TransactionContextInterface, shardID string, validatorNodesStr string, minReputationStr string, maxCapacityStr string) error {
	exists, err := sc.ShardExists(ctx, shardID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("shard %s already exists", shardID)
	}

	minReputation, err := strconv.ParseFloat(minReputationStr, 64)
	if err != nil {
		return fmt.Errorf("invalid minimum reputation: %v", err)
	}

	maxCapacity, err := strconv.Atoi(maxCapacityStr)
	if err != nil {
		return fmt.Errorf("invalid max capacity: %v", err)
	}

	validatorNodes := strings.Split(validatorNodesStr, ",")
	
	shard := Shard{
		ShardID:         shardID,
		ValidatorNodes:  validatorNodes,
		MinReputation:   minReputation,
		MaxCapacity:     maxCapacity,
		CurrentLoad:     0,
		BlockHeight:     0,
		Status:          "active",
		CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
		LastUpdated:     ctx.GetStub().GetTxTimestamp().GetSeconds(),
		TransactionPool: []string{},
	}

	shardJSON, err := json.Marshal(shard)
	if err != nil {
		return err
	}

	err = ctx.GetStub().PutState("shard_"+shardID, shardJSON)
	if err != nil {
		return err
	}

	// Initialize BFT consensus for new shard
	consensusState := BFTConsensusState{
		ShardID:      shardID,
		View:         0,
		Sequence:     0,
		Phase:        "prepare",
		Participants: validatorNodes,
		Votes:        make(map[string]string),
		Timestamp:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	consensusJSON, err := json.Marshal(consensusState)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("bft_consensus_"+shardID, consensusJSON)
}

// AssignTransactionToShard assigns a transaction to the most suitable shard
func (sc *ShardingContract) AssignTransactionToShard(ctx contractapi.TransactionContextInterface, txID string, txSize int) (string, error) {
	// Get all active shards
	shards, err := sc.GetActiveShards(ctx)
	if err != nil {
		return "", err
	}

	if len(shards) == 0 {
		return "", fmt.Errorf("no active shards available")
	}

	// Find shard with lowest load that can accommodate the transaction
	var selectedShard *Shard
	var minLoadRatio float64 = 1.0

	for _, shard := range shards {
		if shard.CurrentLoad+txSize <= shard.MaxCapacity {
			loadRatio := float64(shard.CurrentLoad) / float64(shard.MaxCapacity)
			if loadRatio < minLoadRatio {
				minLoadRatio = loadRatio
				selectedShard = shard
			}
		}
	}

	if selectedShard == nil {
		return "", fmt.Errorf("no shard available with sufficient capacity")
	}

	// Add transaction to shard's pool
	selectedShard.TransactionPool = append(selectedShard.TransactionPool, txID)
	selectedShard.CurrentLoad += txSize
	selectedShard.LastUpdated = ctx.GetStub().GetTxTimestamp().GetSeconds()

	// Update shard state
	shardJSON, err := json.Marshal(selectedShard)
	if err != nil {
		return "", err
	}

	err = ctx.GetStub().PutState("shard_"+selectedShard.ShardID, shardJSON)
	if err != nil {
		return "", err
	}

	return selectedShard.ShardID, nil
}

// ProcessBFTConsensus processes BFT consensus for a shard
func (sc *ShardingContract) ProcessBFTConsensus(ctx contractapi.TransactionContextInterface, shardID string, nodeID string, vote string, blockData string) error {
	// Get current consensus state
	consensusStateJSON, err := ctx.GetStub().GetState("bft_consensus_" + shardID)
	if err != nil {
		return fmt.Errorf("failed to read consensus state: %v", err)
	}
	if consensusStateJSON == nil {
		return fmt.Errorf("consensus state not found for shard %s", shardID)
	}

	var consensusState BFTConsensusState
	err = json.Unmarshal(consensusStateJSON, &consensusState)
	if err != nil {
		return err
	}

	// Validate that node is a participant
	isParticipant := false
	for _, participant := range consensusState.Participants {
		if participant == nodeID {
			isParticipant = true
			break
		}
	}

	if !isParticipant {
		return fmt.Errorf("node %s is not a participant in shard %s consensus", nodeID, shardID)
	}

	// Record vote
	consensusState.Votes[nodeID] = vote
	consensusState.Timestamp = ctx.GetStub().GetTxTimestamp().GetSeconds()

	// Check if consensus reached (need 2f+1 votes where f is number of Byzantine nodes)
	requiredVotes := (len(consensusState.Participants)*2)/3 + 1
	agreeVotes := 0
	
	for _, v := range consensusState.Votes {
		if v == "commit" {
			agreeVotes++
		}
	}

	// If consensus reached, commit block
	if agreeVotes >= requiredVotes {
		consensusState.Phase = "commit"
		consensusState.CommittedBlock = blockData
		consensusState.Sequence++
		
		// Update shard block height
		shard, err := sc.GetShard(ctx, shardID)
		if err != nil {
			return err
		}

		shard.BlockHeight++
		shard.LastUpdated = ctx.GetStub().GetTxTimestamp().GetSeconds()
		// Clear transaction pool after successful commit
		shard.TransactionPool = []string{}
		shard.CurrentLoad = 0

		shardJSON, err := json.Marshal(shard)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState("shard_"+shardID, shardJSON)
		if err != nil {
			return err
		}

		// Reset votes for next consensus round
		consensusState.Votes = make(map[string]string)
		consensusState.Phase = "prepare"
	}

	// Update consensus state
	consensusJSON, err := json.Marshal(consensusState)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("bft_consensus_"+shardID, consensusJSON)
}

// CreateCrossShardTransaction creates a transaction that spans multiple shards
func (sc *ShardingContract) CreateCrossShardTransaction(ctx contractapi.TransactionContextInterface, txID string, sourceShard string, targetShardsStr string) error {
	targetShards := strings.Split(targetShardsStr, ",")

	// Validate all shards exist
	if !sc.shardExists(ctx, sourceShard) {
		return fmt.Errorf("source shard %s does not exist", sourceShard)
	}

	for _, targetShard := range targetShards {
		if !sc.shardExists(ctx, targetShard) {
			return fmt.Errorf("target shard %s does not exist", targetShard)
		}
	}

	crossTx := CrossShardTransaction{
		TxID:         txID,
		SourceShard:  sourceShard,
		TargetShards: targetShards,
		Status:       "pending",
		CommitProofs: make(map[string]string),
		CreatedAt:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	crossTxJSON, err := json.Marshal(crossTx)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("cross_shard_tx_"+txID, crossTxJSON)
}

// UpdateShardMetrics updates performance metrics for a shard
func (sc *ShardingContract) UpdateShardMetrics(ctx contractapi.TransactionContextInterface, shardID string, throughputTPS string, avgLatencyMs string, validationSuccess string, nodeUtilization string) error {
	tps, err := strconv.ParseFloat(throughputTPS, 64)
	if err != nil {
		return fmt.Errorf("invalid throughput value: %v", err)
	}

	latency, err := strconv.ParseFloat(avgLatencyMs, 64)
	if err != nil {
		return fmt.Errorf("invalid latency value: %v", err)
	}

	successRate, err := strconv.ParseFloat(validationSuccess, 64)
	if err != nil {
		return fmt.Errorf("invalid success rate: %v", err)
	}

	utilization, err := strconv.ParseFloat(nodeUtilization, 64)
	if err != nil {
		return fmt.Errorf("invalid utilization value: %v", err)
	}

	metrics := ShardMetrics{
		ShardID:           shardID,
		ThroughputTPS:     tps,
		AverageLatencyMs:  latency,
		ValidationSuccess: successRate,
		NodeUtilization:   utilization,
		LastCalculated:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("shard_metrics_"+shardID, metricsJSON)
}

// GetShard returns information about a specific shard
func (sc *ShardingContract) GetShard(ctx contractapi.TransactionContextInterface, shardID string) (*Shard, error) {
	shardJSON, err := ctx.GetStub().GetState("shard_" + shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to read from world state: %v", err)
	}
	if shardJSON == nil {
		return nil, fmt.Errorf("shard %s does not exist", shardID)
	}

	var shard Shard
	err = json.Unmarshal(shardJSON, &shard)
	if err != nil {
		return nil, err
	}

	return &shard, nil
}

// GetActiveShards returns all active shards
func (sc *ShardingContract) GetActiveShards(ctx contractapi.TransactionContextInterface) ([]*Shard, error) {
	resultsIterator, err := ctx.GetStub().GetStateByRange("shard_", "shard_~")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var shards []*Shard
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var shard Shard
		err = json.Unmarshal(queryResponse.Value, &shard)
		if err != nil {
			return nil, err
		}

		if shard.Status == "active" {
			shards = append(shards, &shard)
		}
	}

	return shards, nil
}

// ShardExists checks if a shard exists
func (sc *ShardingContract) ShardExists(ctx contractapi.TransactionContextInterface, shardID string) (bool, error) {
	shardJSON, err := ctx.GetStub().GetState("shard_" + shardID)
	if err != nil {
		return false, fmt.Errorf("failed to read from world state: %v", err)
	}

	return shardJSON != nil, nil
}

// Helper function for internal use
func (sc *ShardingContract) shardExists(ctx contractapi.TransactionContextInterface, shardID string) bool {
	exists, err := sc.ShardExists(ctx, shardID)
	if err != nil {
		return false
	}
	return exists
}

// GetShardMetrics returns performance metrics for a shard
func (sc *ShardingContract) GetShardMetrics(ctx contractapi.TransactionContextInterface, shardID string) (*ShardMetrics, error) {
	metricsJSON, err := ctx.GetStub().GetState("shard_metrics_" + shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to read metrics: %v", err)
	}
	if metricsJSON == nil {
		return nil, fmt.Errorf("metrics not found for shard %s", shardID)
	}

	var metrics ShardMetrics
	err = json.Unmarshal(metricsJSON, &metrics)
	if err != nil {
		return nil, err
	}

	return &metrics, nil
}

func main() {
	shardingChaincode, err := contractapi.NewChaincode(&ShardingContract{})
	if err != nil {
		log.Panicf("Error creating sharding chaincode: %v", err)
	}

	if err := shardingChaincode.Start(); err != nil {
		log.Panicf("Error starting sharding chaincode: %v", err)
	}
}