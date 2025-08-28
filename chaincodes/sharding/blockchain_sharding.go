package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"math"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

// BlockchainContract implements Service-aware Dynamic Sharding in Hyperledger Fabric
type BlockchainContract struct {
	contractapi.Contract
}

// ServiceType represents different types of services in Blockchain
type ServiceType struct {
	ServiceID      string  `json:"serviceID"`
	ServiceName    string  `json:"serviceName"`
	LatencyReq     int     `json:"latencyReq"`     // milliseconds
	ThroughputReq  int     `json:"throughputReq"`  // TPS
	ConsistencyReq string  `json:"consistencyReq"` // strong, eventual, weak
	Priority       int     `json:"priority"`       // 1-5 scale
	ResourceReq    int     `json:"resourceReq"`    // CPU units
}

// BlockchainShard represents a shard in the Blockchain system
type BlockchainShard struct {
	ShardID         string        `json:"shardID"`
	ServiceTypes    []string      `json:"serviceTypes"`
	AssignedNodes   []string      `json:"assignedNodes"`
	CurrentServices int           `json:"currentServices"`
	MaxServices     int           `json:"maxServices"`
	AvgLatency      float64       `json:"avgLatency"`
	TotalThroughput int           `json:"totalThroughput"`
	ResourceUsage   float64       `json:"resourceUsage"`
	Status          string        `json:"status"` // active, overloaded, maintenance
	CreatedAt       int64         `json:"createdAt"`
	LastBalanced    int64         `json:"lastBalanced"`
}

// ServiceMapping tracks which service is handled by which shard
type ServiceMapping struct {
	ServiceID     string `json:"serviceID"`
	PrimaryShards string `json:"primaryShard"`
	BackupShards  string `json:"backupShards"`
	LastMigrated  int64  `json:"lastMigrated"`
	MigrationCost int    `json:"migrationCost"`
}

// DynamicLoadMetrics tracks real-time load for dynamic sharding decisions
type DynamicLoadMetrics struct {
	ShardID              string  `json:"shardID"`
	CurrentTPS           float64 `json:"currentTPS"`
	AverageResponseTime  float64 `json:"averageResponseTime"`
	ResourceUtilization  float64 `json:"resourceUtilization"`
	ServiceDistribution  map[string]int `json:"serviceDistribution"`
	LoadTrend            string  `json:"loadTrend"` // increasing, decreasing, stable
	PredictedLoad        float64 `json:"predictedLoad"`
	LastUpdated          int64   `json:"lastUpdated"`
}

// InitLedger initializes Blockchain with default service types and shards
func (blockchain *BlockchainContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
	// Initialize default service types
	defaultServices := []ServiceType{
		{
			ServiceID:      "payment_service",
			ServiceName:    "Payment Processing",
			LatencyReq:     100,  // 100ms
			ThroughputReq:  1000, // 1000 TPS
			ConsistencyReq: "strong",
			Priority:       5, // Highest priority
			ResourceReq:    80,
		},
		{
			ServiceID:      "data_analytics",
			ServiceName:    "Data Analytics",
			LatencyReq:     5000, // 5 seconds
			ThroughputReq:  500,  // 500 TPS
			ConsistencyReq: "eventual",
			Priority:       3,
			ResourceReq:    120,
		},
		{
			ServiceID:      "identity_mgmt",
			ServiceName:    "Identity Management",
			LatencyReq:     200,  // 200ms
			ThroughputReq:  800,  // 800 TPS
			ConsistencyReq: "strong",
			Priority:       4,
			ResourceReq:    60,
		},
		{
			ServiceID:      "iot_data",
			ServiceName:    "IoT Data Processing",
			LatencyReq:     50,   // 50ms
			ThroughputReq:  2000, // 2000 TPS
			ConsistencyReq: "weak",
			Priority:       2,
			ResourceReq:    40,
		},
	}

	for _, service := range defaultServices {
		serviceJSON, err := json.Marshal(service)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState("service_type_"+service.ServiceID, serviceJSON)
		if err != nil {
			return fmt.Errorf("failed to put service type to world state: %v", err)
		}
	}

	// Initialize default shards with service-aware allocation
	defaultShards := []BlockchainShard{
		{
			ShardID:         "blockchain_shard_payment",
			ServiceTypes:    []string{"payment_service", "identity_mgmt"},
			AssignedNodes:   []string{"node_p1", "node_p2", "node_p3"},
			CurrentServices: 0,
			MaxServices:     50,
			AvgLatency:      0,
			TotalThroughput: 0,
			ResourceUsage:   0,
			Status:          "active",
			CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
			LastBalanced:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
		},
		{
			ShardID:         "blockchain_shard_analytics",
			ServiceTypes:    []string{"data_analytics"},
			AssignedNodes:   []string{"node_a1", "node_a2", "node_a3", "node_a4"},
			CurrentServices: 0,
			MaxServices:     30,
			AvgLatency:      0,
			TotalThroughput: 0,
			ResourceUsage:   0,
			Status:          "active",
			CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
			LastBalanced:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
		},
		{
			ShardID:         "blockchain_shard_iot",
			ServiceTypes:    []string{"iot_data"},
			AssignedNodes:   []string{"node_i1", "node_i2", "node_i3", "node_i4", "node_i5"},
			CurrentServices: 0,
			MaxServices:     80,
			AvgLatency:      0,
			TotalThroughput: 0,
			ResourceUsage:   0,
			Status:          "active",
			CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
			LastBalanced:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
		},
	}

	for _, shard := range defaultShards {
		shardJSON, err := json.Marshal(shard)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState("blockchain_shard_"+shard.ShardID, shardJSON)
		if err != nil {
			return fmt.Errorf("failed to put shard to world state: %v", err)
		}
	}

	return nil
}

// RegisterServiceType registers a new service type with specific requirements
func (blockchain *BlockchainContract) RegisterServiceType(ctx contractapi.TransactionContextInterface, serviceID string, serviceName string, latencyReqStr string, throughputReqStr string, consistencyReq string, priorityStr string, resourceReqStr string) error {
	exists, err := blockchain.ServiceTypeExists(ctx, serviceID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("service type %s already exists", serviceID)
	}

	latencyReq, err := strconv.Atoi(latencyReqStr)
	if err != nil {
		return fmt.Errorf("invalid latency requirement: %v", err)
	}

	throughputReq, err := strconv.Atoi(throughputReqStr)
	if err != nil {
		return fmt.Errorf("invalid throughput requirement: %v", err)
	}

	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		return fmt.Errorf("invalid priority: %v", err)
	}

	resourceReq, err := strconv.Atoi(resourceReqStr)
	if err != nil {
		return fmt.Errorf("invalid resource requirement: %v", err)
	}

	serviceType := ServiceType{
		ServiceID:      serviceID,
		ServiceName:    serviceName,
		LatencyReq:     latencyReq,
		ThroughputReq:  throughputReq,
		ConsistencyReq: consistencyReq,
		Priority:       priority,
		ResourceReq:    resourceReq,
	}

	serviceJSON, err := json.Marshal(serviceType)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("service_type_"+serviceID, serviceJSON)
}

// DynamicShardAllocation performs service-aware dynamic shard allocation
func (blockchain *BlockchainContract) DynamicShardAllocation(ctx contractapi.TransactionContextInterface, serviceID string) (string, error) {
	// Get service requirements
	service, err := blockchain.GetServiceType(ctx, serviceID)
	if err != nil {
		return "", err
	}

	// Get all active shards
	shards, err := blockchain.GetAllActiveShards(ctx)
	if err != nil {
		return "", err
	}

	if len(shards) == 0 {
		return "", fmt.Errorf("no active shards available")
	}

	// Find best shard using service-aware algorithm
	bestShard := blockchain.findBestShardForService(service, shards)

	if bestShard == nil {
		// Need to create new shard or rebalance existing ones
		return blockchain.triggerShardRebalancing(ctx, service)
	}

	// Update shard allocation
	bestShard.CurrentServices++
	// Update service types if not already included
	if !blockchain.containsService(bestShard.ServiceTypes, serviceID) {
		bestShard.ServiceTypes = append(bestShard.ServiceTypes, serviceID)
	}

	// Update shard state
	shardJSON, err := json.Marshal(bestShard)
	if err != nil {
		return "", err
	}

	err = ctx.GetStub().PutState("blockchain_shard_"+bestShard.ShardID, shardJSON)
	if err != nil {
		return "", err
	}

	// Create service mapping
	mapping := ServiceMapping{
		ServiceID:     serviceID,
		PrimaryShards: bestShard.ShardID,
		BackupShards:  "", // Could be set for fault tolerance
		LastMigrated:  ctx.GetStub().GetTxTimestamp().GetSeconds(),
		MigrationCost: blockchain.calculateMigrationCost(service, bestShard),
	}

	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return "", err
	}

	err = ctx.GetStub().PutState("service_mapping_"+serviceID, mappingJSON)
	if err != nil {
		return "", err
	}

	return bestShard.ShardID, nil
}

// findBestShardForService implements Blockchain service-aware selection algorithm
func (blockchain *BlockchainContract) findBestShardForService(service *ServiceType, shards []*BlockchainShard) *BlockchainShard {
	var bestShard *BlockchainShard
	var bestScore float64 = -1

	for _, shard := range shards {
		if shard.Status != "active" {
			continue
		}

		// Check if shard can handle the service
		if shard.CurrentServices >= shard.MaxServices {
			continue
		}

		// Calculate service affinity score
		score := blockchain.calculateServiceAffinityScore(service, shard)
		
		if score > bestScore {
			bestScore = score
			bestShard = shard
		}
	}

	return bestShard
}

// calculateServiceAffinityScore computes how well a service fits into a shard
func (blockchain *BlockchainContract) calculateServiceAffinityScore(service *ServiceType, shard *BlockchainShard) float64 {
	score := 0.0

	// 1. Latency compatibility (40% weight)
	latencyScore := 1.0 - (shard.AvgLatency / float64(service.LatencyReq))
	if latencyScore < 0 {
		latencyScore = 0
	}
	score += latencyScore * 0.4

	// 2. Resource utilization (30% weight)
	resourceScore := 1.0 - shard.ResourceUsage/100.0
	score += resourceScore * 0.3

	// 3. Service type similarity (20% weight)
	similarityScore := 0.0
	for _, existingService := range shard.ServiceTypes {
		// Check if services have similar requirements
		// (In real implementation, would compare service characteristics)
		if existingService == service.ServiceID {
			similarityScore += 0.5
		}
	}
	if len(shard.ServiceTypes) > 0 {
		similarityScore = similarityScore / float64(len(shard.ServiceTypes))
	}
	score += similarityScore * 0.2

	// 4. Load balancing (10% weight)
	loadScore := 1.0 - (float64(shard.CurrentServices) / float64(shard.MaxServices))
	score += loadScore * 0.1

	return score
}

// UpdateShardMetrics updates real-time metrics for dynamic load balancing
func (blockchain *BlockchainContract) UpdateShardMetrics(ctx contractapi.TransactionContextInterface, shardID string, currentTPSStr string, avgResponseTimeStr string, resourceUtilStr string) error {
	currentTPS, err := strconv.ParseFloat(currentTPSStr, 64)
	if err != nil {
		return fmt.Errorf("invalid TPS value: %v", err)
	}

	avgResponseTime, err := strconv.ParseFloat(avgResponseTimeStr, 64)
	if err != nil {
		return fmt.Errorf("invalid response time: %v", err)
	}

	resourceUtil, err := strconv.ParseFloat(resourceUtilStr, 64)
	if err != nil {
		return fmt.Errorf("invalid resource utilization: %v", err)
	}

	// Get existing metrics or create new
	existingMetrics, _ := blockchain.GetShardMetrics(ctx, shardID)
	
	// Calculate load trend
	loadTrend := "stable"
	predictedLoad := currentTPS
	if existingMetrics != nil {
		if currentTPS > existingMetrics.CurrentTPS*1.1 {
			loadTrend = "increasing"
			predictedLoad = currentTPS * 1.2
		} else if currentTPS < existingMetrics.CurrentTPS*0.9 {
			loadTrend = "decreasing"
			predictedLoad = currentTPS * 0.8
		}
	}

	metrics := DynamicLoadMetrics{
		ShardID:             shardID,
		CurrentTPS:          currentTPS,
		AverageResponseTime: avgResponseTime,
		ResourceUtilization: resourceUtil,
		LoadTrend:          loadTrend,
		PredictedLoad:      predictedLoad,
		LastUpdated:        ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	// Update shard with new metrics
	shard, err := blockchain.GetBlockchainShard(ctx, shardID)
	if err != nil {
		return err
	}

	shard.AvgLatency = avgResponseTime
	shard.ResourceUsage = resourceUtil
	shard.TotalThroughput = int(currentTPS)

	// Trigger rebalancing if needed
	if resourceUtil > 85.0 || avgResponseTime > 1000 { // Overload conditions
		shard.Status = "overloaded"
	} else {
		shard.Status = "active"
	}

	shardJSON, err := json.Marshal(shard)
	if err != nil {
		return err
	}

	err = ctx.GetStub().PutState("blockchain_shard_"+shardID, shardJSON)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState("shard_metrics_"+shardID, metricsJSON)
}

// triggerShardRebalancing creates new shard or rebalances existing ones
func (blockchain *BlockchainContract) triggerShardRebalancing(ctx contractapi.TransactionContextInterface, service *ServiceType) (string, error) {
	// Simple implementation: create new shard
	newShardID := fmt.Sprintf("blockchain_shard_%s_%d", service.ServiceID, ctx.GetStub().GetTxTimestamp().GetSeconds())
	
	newShard := BlockchainShard{
		ShardID:         newShardID,
		ServiceTypes:    []string{service.ServiceID},
		AssignedNodes:   []string{fmt.Sprintf("node_%s_1", service.ServiceID), fmt.Sprintf("node_%s_2", service.ServiceID)},
		CurrentServices: 1,
		MaxServices:     50,
		AvgLatency:      0,
		TotalThroughput: 0,
		ResourceUsage:   0,
		Status:          "active",
		CreatedAt:       ctx.GetStub().GetTxTimestamp().GetSeconds(),
		LastBalanced:    ctx.GetStub().GetTxTimestamp().GetSeconds(),
	}

	shardJSON, err := json.Marshal(newShard)
	if err != nil {
		return "", err
	}

	err = ctx.GetStub().PutState("blockchain_shard_"+newShardID, shardJSON)
	if err != nil {
		return "", err
	}

	return newShardID, nil
}

// Helper functions
func (blockchain *BlockchainContract) ServiceTypeExists(ctx contractapi.TransactionContextInterface, serviceID string) (bool, error) {
	serviceJSON, err := ctx.GetStub().GetState("service_type_" + serviceID)
	if err != nil {
		return false, fmt.Errorf("failed to read from world state: %v", err)
	}

	return serviceJSON != nil, nil
}

func (blockchain *BlockchainContract) GetServiceType(ctx contractapi.TransactionContextInterface, serviceID string) (*ServiceType, error) {
	serviceJSON, err := ctx.GetStub().GetState("service_type_" + serviceID)
	if err != nil {
		return nil, fmt.Errorf("failed to read from world state: %v", err)
	}
	if serviceJSON == nil {
		return nil, fmt.Errorf("service type %s does not exist", serviceID)
	}

	var service ServiceType
	err = json.Unmarshal(serviceJSON, &service)
	if err != nil {
		return nil, err
	}

	return &service, nil
}

func (blockchain *BlockchainContract) GetBlockchainShard(ctx contractapi.TransactionContextInterface, shardID string) (*BlockchainShard, error) {
	shardJSON, err := ctx.GetStub().GetState("blockchain_shard_" + shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to read from world state: %v", err)
	}
	if shardJSON == nil {
		return nil, fmt.Errorf("shard %s does not exist", shardID)
	}

	var shard BlockchainShard
	err = json.Unmarshal(shardJSON, &shard)
	if err != nil {
		return nil, err
	}

	return &shard, nil
}

func (blockchain *BlockchainContract) GetAllActiveShards(ctx contractapi.TransactionContextInterface) ([]*BlockchainShard, error) {
	resultsIterator, err := ctx.GetStub().GetStateByRange("blockchain_shard_", "blockchain_shard_~")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var shards []*BlockchainShard
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var shard BlockchainShard
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

func (blockchain *BlockchainContract) GetShardMetrics(ctx contractapi.TransactionContextInterface, shardID string) (*DynamicLoadMetrics, error) {
	metricsJSON, err := ctx.GetStub().GetState("shard_metrics_" + shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to read metrics: %v", err)
	}
	if metricsJSON == nil {
		return nil, nil // No metrics yet
	}

	var metrics DynamicLoadMetrics
	err = json.Unmarshal(metricsJSON, &metrics)
	if err != nil {
		return nil, err
	}

	return &metrics, nil
}

func (blockchain *BlockchainContract) containsService(services []string, serviceID string) bool {
	for _, s := range services {
		if s == serviceID {
			return true
		}
	}
	return false
}

func (blockchain *BlockchainContract) calculateMigrationCost(service *ServiceType, shard *BlockchainShard) int {
	// Simple migration cost calculation
	baseCost := 100
	resourceFactor := int(math.Ceil(float64(service.ResourceReq) / 10.0))
	priorityFactor := 6 - service.Priority // Higher priority = lower migration cost
	
	return baseCost + resourceFactor*10 + priorityFactor*5
}

// GetServiceMapping returns the mapping of service to shards
func (blockchain *BlockchainContract) GetServiceMapping(ctx contractapi.TransactionContextInterface, serviceID string) (*ServiceMapping, error) {
	mappingJSON, err := ctx.GetStub().GetState("service_mapping_" + serviceID)
	if err != nil {
		return nil, fmt.Errorf("failed to read mapping: %v", err)
	}
	if mappingJSON == nil {
		return nil, fmt.Errorf("service mapping for %s not found", serviceID)
	}

	var mapping ServiceMapping
	err = json.Unmarshal(mappingJSON, &mapping)
	if err != nil {
		return nil, err
	}

	return &mapping, nil
}

func main() {
	blockchainChaincode, err := contractapi.NewChaincode(&BlockchainContract{})
	if err != nil {
		log.Panicf("Error creating Blockchain chaincode: %v", err)
	}

	if err := blockchainChaincode.Start(); err != nil {
		log.Panicf("Error starting Blockchain chaincode: %v", err)
	}
}