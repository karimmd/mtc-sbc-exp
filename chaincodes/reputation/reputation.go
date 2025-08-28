package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

// ReputationContract provides functions for managing node reputation using subjective logic
type ReputationContract struct {
	contractapi.Contract
}

// SubjectiveLogicValue represents a subjective logic triplet (belief, disbelief, uncertainty)
type SubjectiveLogicValue struct {
	Belief      float64 `json:"belief"`
	Disbelief   float64 `json:"disbelief"`  
	Uncertainty float64 `json:"uncertainty"`
	BaseRate    float64 `json:"baseRate"`
}

// NodeReputation represents the reputation state of a node
type NodeReputation struct {
	NodeID        string               `json:"nodeID"`
	Tier          string               `json:"tier"`
	ReputationSL  SubjectiveLogicValue `json:"reputationSL"`
	Interactions  int                  `json:"interactions"`
	SuccessCount  int                  `json:"successCount"`
	FailureCount  int                  `json:"failureCount"`
	LastUpdated   int64                `json:"lastUpdated"`
	ShardID       string               `json:"shardID,omitempty"`
}

// TransactionRecord represents a transaction validation record
type TransactionRecord struct {
	TxID         string `json:"txID"`
	ValidatorID  string `json:"validatorID"`
	IsValid      bool   `json:"isValid"`
	Timestamp    int64  `json:"timestamp"`
	BlockHeight  int64  `json:"blockHeight"`
	ShardID      string `json:"shardID"`
}

// ShardAllocation represents shard assignment based on reputation
type ShardAllocation struct {
	ShardID           string   `json:"shardID"`
	AssignedNodes     []string `json:"assignedNodes"`
	MinReputationReq  float64  `json:"minReputationReq"`
	MaxNodes          int      `json:"maxNodes"`
	CurrentNodes      int      `json:"currentNodes"`
	CreatedAt         int64    `json:"createdAt"`
}

// InitLedger initializes the ledger with default reputation values for system bootstrap
func (rc *ReputationContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
	// Initialize default reputation for system nodes
	defaultNodes := []NodeReputation{
		{
			NodeID: "cloud-node-1",
			Tier:   "cloud",
			ReputationSL: SubjectiveLogicValue{
				Belief:      0.5,
				Disbelief:   0.0,
				Uncertainty: 0.5,
				BaseRate:    0.6,
			},
			Interactions: 0,
			SuccessCount: 0,
			FailureCount: 0,
			LastUpdated:  ctx.GetStub().GetTxTimestamp().GetSeconds(),
		},
		{
			NodeID: "fog-node-1", 
			Tier:   "fog",
			ReputationSL: SubjectiveLogicValue{
				Belief:      0.4,
				Disbelief:   0.0,
				Uncertainty: 0.6,
				BaseRate:    0.5,
			},
			Interactions: 0,
			SuccessCount: 0,
			FailureCount: 0,
			LastUpdated:  ctx.GetStub().GetTxTimestamp().GetSeconds(),
		},
	}

	for _, node := range defaultNodes {
		nodeJSON, err := json.Marshal(node)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState("reputation_"+node.NodeID, nodeJSON)
		if err != nil {
			return fmt.Errorf("failed to put node reputation to world state: %v", err)
		}
	}

	return nil
}

// RegisterNode registers a new node with initial reputation
func (rc *ReputationContract) RegisterNode(ctx contractapi.TransactionContextInterface, nodeID string, tier string) error {
	exists, err := rc.NodeExists(ctx, nodeID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("node %s already exists", nodeID)
	}

	// Set initial reputation based on tier
	var initialBelief float64
	var baseRate float64
	
	switch tier {
	case "cloud":
		initialBelief = 0.6
		baseRate = 0.7
	case "fog":
		initialBelief = 0.4
		baseRate = 0.5
	case "edge":
		initialBelief = 0.3
		baseRate = 0.4
	default:
		return fmt.Errorf("invalid tier: %s", tier)
	}

	nodeReputation := NodeReputation{
		NodeID: nodeID,
		Tier:   tier,
		ReputationSL: SubjectiveLogicValue{
			Belief:      initialBelief,
			Disbelief:   0.0,
			Uncertainty: 1.0 - initialBelief,
			BaseRate:    baseRate,
		},
		Interactions: 0,
		SuccessCount: 0,
		FailureCount: 0,
		LastUpdated:  ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	nodeJSON, err := json.Marshal(nodeReputation)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("reputation_"+nodeID, nodeJSON)
}

// UpdateReputation updates a node's reputation based on transaction validation result
func (rc *ReputationContract) UpdateReputation(ctx contractapi.TransactionContextInterface, nodeID string, success bool, txID string, blockHeight int64, shardID string) error {
	nodeReputation, err := rc.GetNodeReputation(ctx, nodeID)
	if err != nil {
		return err
	}

	// Update interaction counts
	nodeReputation.Interactions++
	if success {
		nodeReputation.SuccessCount++
	} else {
		nodeReputation.FailureCount++
	}

	// Update subjective logic values using Beta distribution
	nodeReputation.ReputationSL = rc.updateSubjectiveLogic(nodeReputation.ReputationSL, success)
	nodeReputation.LastUpdated = ctx.GetStub().GetTxTimestamp().GetSeconds()

	// Record transaction validation
	txRecord := TransactionRecord{
		TxID:        txID,
		ValidatorID: nodeID,
		IsValid:     success,
		Timestamp:   ctx.GetStub().GetTxTimestamp().GetSeconds(),
		BlockHeight: blockHeight,
		ShardID:     shardID,
	}

	// Store updated reputation
	nodeJSON, err := json.Marshal(nodeReputation)
	if err != nil {
		return err
	}

	err = ctx.GetStub().PutState("reputation_"+nodeID, nodeJSON)
	if err != nil {
		return err
	}

	// Store transaction record
	txRecordJSON, err := json.Marshal(txRecord)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("tx_record_"+txID+"_"+nodeID, txRecordJSON)
}

// updateSubjectiveLogic updates subjective logic values using Beta distribution
func (rc *ReputationContract) updateSubjectiveLogic(current SubjectiveLogicValue, success bool) SubjectiveLogicValue {
	// Beta distribution parameters
	alpha := float64(1) // Prior positive evidence
	beta := float64(1)  // Prior negative evidence

	if success {
		alpha += 1
	} else {
		beta += 1
	}

	// Calculate new subjective logic values
	total := alpha + beta
	newBelief := (alpha - 1) / (total - 2 + 1/current.Uncertainty)
	newDisbelief := (beta - 1) / (total - 2 + 1/current.Uncertainty)
	newUncertainty := 2.0 / (total - 2 + 1/current.Uncertainty)

	// Ensure values are within valid range [0,1] and sum to 1
	if newBelief < 0 {
		newBelief = 0
	}
	if newDisbelief < 0 {
		newDisbelief = 0
	}
	if newUncertainty < 0 {
		newUncertainty = 0.1 // Minimum uncertainty
	}

	// Normalize to sum to 1
	total_vals := newBelief + newDisbelief + newUncertainty
	if total_vals > 0 {
		newBelief /= total_vals
		newDisbelief /= total_vals
		newUncertainty /= total_vals
	}

	return SubjectiveLogicValue{
		Belief:      newBelief,
		Disbelief:   newDisbelief,
		Uncertainty: newUncertainty,
		BaseRate:    current.BaseRate,
	}
}

// GetNodeReputation returns the reputation of a specific node
func (rc *ReputationContract) GetNodeReputation(ctx contractapi.TransactionContextInterface, nodeID string) (*NodeReputation, error) {
	nodeJSON, err := ctx.GetStub().GetState("reputation_" + nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to read from world state: %v", err)
	}
	if nodeJSON == nil {
		return nil, fmt.Errorf("node %s does not exist", nodeID)
	}

	var nodeReputation NodeReputation
	err = json.Unmarshal(nodeJSON, &nodeReputation)
	if err != nil {
		return nil, err
	}

	return &nodeReputation, nil
}

// CalculateExpectedValue calculates the expected reputation value using subjective logic
func (rc *ReputationContract) CalculateExpectedValue(ctx contractapi.TransactionContextInterface, nodeID string) (float64, error) {
	nodeReputation, err := rc.GetNodeReputation(ctx, nodeID)
	if err != nil {
		return 0, err
	}

	sl := nodeReputation.ReputationSL
	expectedValue := sl.Belief + sl.Uncertainty*sl.BaseRate
	
	return expectedValue, nil
}

// GetTopReputationNodes returns nodes with reputation above threshold, sorted by reputation
func (rc *ReputationContract) GetTopReputationNodes(ctx contractapi.TransactionContextInterface, threshold string, tier string) ([]*NodeReputation, error) {
	thresholdFloat, err := strconv.ParseFloat(threshold, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid threshold value: %v", err)
	}

	resultsIterator, err := ctx.GetStub().GetStateByRange("reputation_", "reputation_~")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var topNodes []*NodeReputation
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var nodeReputation NodeReputation
		err = json.Unmarshal(queryResponse.Value, &nodeReputation)
		if err != nil {
			return nil, err
		}

		// Filter by tier if specified
		if tier != "" && nodeReputation.Tier != tier {
			continue
		}

		// Calculate expected reputation value
		expectedValue := nodeReputation.ReputationSL.Belief + 
			nodeReputation.ReputationSL.Uncertainty*nodeReputation.ReputationSL.BaseRate

		if expectedValue >= thresholdFloat {
			topNodes = append(topNodes, &nodeReputation)
		}
	}

	return topNodes, nil
}

// AllocateShardByReputation allocates nodes to shard based on reputation
func (rc *ReputationContract) AllocateShardByReputation(ctx contractapi.TransactionContextInterface, shardID string, minReputation string, maxNodes int) error {
	minReputationFloat, err := strconv.ParseFloat(minReputation, 64)
	if err != nil {
		return fmt.Errorf("invalid minimum reputation: %v", err)
	}

	// Get eligible nodes
	eligibleNodes, err := rc.GetTopReputationNodes(ctx, minReputation, "")
	if err != nil {
		return err
	}

	if len(eligibleNodes) == 0 {
		return fmt.Errorf("no nodes meet minimum reputation requirement")
	}

	// Select top nodes up to maxNodes limit
	var selectedNodeIDs []string
	nodeCount := int(math.Min(float64(len(eligibleNodes)), float64(maxNodes)))
	
	for i := 0; i < nodeCount; i++ {
		selectedNodeIDs = append(selectedNodeIDs, eligibleNodes[i].NodeID)
		
		// Update node's shard assignment
		eligibleNodes[i].ShardID = shardID
		nodeJSON, err := json.Marshal(eligibleNodes[i])
		if err != nil {
			return err
		}
		
		err = ctx.GetStub().PutState("reputation_"+eligibleNodes[i].NodeID, nodeJSON)
		if err != nil {
			return err
		}
	}

	// Create shard allocation record
	allocation := ShardAllocation{
		ShardID:          shardID,
		AssignedNodes:    selectedNodeIDs,
		MinReputationReq: minReputationFloat,
		MaxNodes:         maxNodes,
		CurrentNodes:     nodeCount,
		CreatedAt:        ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	allocationJSON, err := json.Marshal(allocation)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("shard_allocation_"+shardID, allocationJSON)
}

// NodeExists checks if a node already exists
func (rc *ReputationContract) NodeExists(ctx contractapi.TransactionContextInterface, nodeID string) (bool, error) {
	nodeJSON, err := ctx.GetStub().GetState("reputation_" + nodeID)
	if err != nil {
		return false, fmt.Errorf("failed to read from world state: %v", err)
	}

	return nodeJSON != nil, nil
}

// GetAllNodes returns all registered nodes
func (rc *ReputationContract) GetAllNodes(ctx contractapi.TransactionContextInterface) ([]*NodeReputation, error) {
	resultsIterator, err := ctx.GetStub().GetStateByRange("reputation_", "reputation_~")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var nodes []*NodeReputation
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var nodeReputation NodeReputation
		err = json.Unmarshal(queryResponse.Value, &nodeReputation)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &nodeReputation)
	}

	return nodes, nil
}

func main() {
	reputationChaincode, err := contractapi.NewChaincode(&ReputationContract{})
	if err != nil {
		log.Panicf("Error creating reputation chaincode: %v", err)
	}

	if err := reputationChaincode.Start(); err != nil {
		log.Panicf("Error starting reputation chaincode: %v", err)
	}
}