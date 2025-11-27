package loadbalancer

import (
	"math/rand"
	"sync"
	"time"
	
	"distributed-task-scheduler/internal/common"
)

// Strategy 负载均衡策略
type Strategy string

const (
	StrategyRoundRobin Strategy = "round_robin"
	StrategyRandom     Strategy = "random"
	StrategyLeastLoad  Strategy = "least_load"
	StrategyWeighted   Strategy = "weighted"
)

// LoadBalancer 负载均衡器接口
type LoadBalancer interface {
	SelectWorker(workers []*common.NodeInfo) (*common.NodeInfo, error)
	UpdateStats(workerID string, stats *common.WorkerStats)
}

// RoundRobinLoadBalancer 轮询负载均衡器
type RoundRobinLoadBalancer struct {
	current int
	mu      sync.Mutex
}

// NewRoundRobinLoadBalancer 创建轮询负载均衡器
func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	return &RoundRobinLoadBalancer{
		current: 0,
	}
}

func (lb *RoundRobinLoadBalancer) SelectWorker(workers []*common.NodeInfo) (*common.NodeInfo, error) {
	if len(workers) == 0 {
		return nil, ErrNoWorkers
	}
	
	// 过滤掉离线节点
	onlineWorkers := make([]*common.NodeInfo, 0)
	for _, w := range workers {
		if w.Status == "online" {
			onlineWorkers = append(onlineWorkers, w)
		}
	}
	
	if len(onlineWorkers) == 0 {
		return nil, ErrNoWorkers
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	worker := onlineWorkers[lb.current%len(onlineWorkers)]
	lb.current++
	
	return worker, nil
}

func (lb *RoundRobinLoadBalancer) UpdateStats(workerID string, stats *common.WorkerStats) {
	// 轮询策略不需要更新统计
}

// RandomLoadBalancer 随机负载均衡器
type RandomLoadBalancer struct {
	rand *rand.Rand
}

// NewRandomLoadBalancer 创建随机负载均衡器
func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (lb *RandomLoadBalancer) SelectWorker(workers []*common.NodeInfo) (*common.NodeInfo, error) {
	if len(workers) == 0 {
		return nil, ErrNoWorkers
	}
	
	onlineWorkers := make([]*common.NodeInfo, 0)
	for _, w := range workers {
		if w.Status == "online" {
			onlineWorkers = append(onlineWorkers, w)
		}
	}
	
	if len(onlineWorkers) == 0 {
		return nil, ErrNoWorkers
	}
	
	idx := lb.rand.Intn(len(onlineWorkers))
	return onlineWorkers[idx], nil
}

func (lb *RandomLoadBalancer) UpdateStats(workerID string, stats *common.WorkerStats) {
	// 随机策略不需要更新统计
}

// LeastLoadLoadBalancer 最少负载负载均衡器
type LeastLoadLoadBalancer struct {
	stats map[string]*common.WorkerStats
	mu    sync.RWMutex
}

// NewLeastLoadLoadBalancer 创建最少负载负载均衡器
func NewLeastLoadLoadBalancer() *LeastLoadLoadBalancer {
	return &LeastLoadLoadBalancer{
		stats: make(map[string]*common.WorkerStats),
	}
}

func (lb *LeastLoadLoadBalancer) SelectWorker(workers []*common.NodeInfo) (*common.NodeInfo, error) {
	if len(workers) == 0 {
		return nil, ErrNoWorkers
	}
	
	onlineWorkers := make([]*common.NodeInfo, 0)
	for _, w := range workers {
		if w.Status == "online" {
			onlineWorkers = append(onlineWorkers, w)
		}
	}
	
	if len(onlineWorkers) == 0 {
		return nil, ErrNoWorkers
	}
	
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	
	var selected *common.NodeInfo
	minLoad := 1.0
	
	for _, worker := range onlineWorkers {
		load := worker.Load
		
		// 如果节点有统计信息，使用统计信息计算负载
		if stats, ok := lb.stats[worker.ID]; ok {
			// 综合考虑CPU、内存和运行中的任务数
			load = (stats.CPUUsage + stats.MemoryUsage) / 2.0
			if stats.RunningTasks > 0 {
				load += float64(stats.RunningTasks) * 0.1
			}
			if load > 1.0 {
				load = 1.0
			}
		}
		
		if load < minLoad {
			minLoad = load
			selected = worker
		}
	}
	
	if selected == nil {
		selected = onlineWorkers[0]
	}
	
	return selected, nil
}

func (lb *LeastLoadLoadBalancer) UpdateStats(workerID string, stats *common.WorkerStats) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	lb.stats[workerID] = stats
}

// WeightedLoadBalancer 加权负载均衡器
type WeightedLoadBalancer struct {
	weights map[string]int
	mu      sync.RWMutex
}

// NewWeightedLoadBalancer 创建加权负载均衡器
func NewWeightedLoadBalancer() *WeightedLoadBalancer {
	return &WeightedLoadBalancer{
		weights: make(map[string]int),
	}
}

func (lb *WeightedLoadBalancer) SelectWorker(workers []*common.NodeInfo) (*common.NodeInfo, error) {
	if len(workers) == 0 {
		return nil, ErrNoWorkers
	}
	
	onlineWorkers := make([]*common.NodeInfo, 0)
	for _, w := range workers {
		if w.Status == "online" {
			onlineWorkers = append(onlineWorkers, w)
		}
	}
	
	if len(onlineWorkers) == 0 {
		return nil, ErrNoWorkers
	}
	
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	
	// 计算总权重
	totalWeight := 0
	for _, worker := range onlineWorkers {
		weight := lb.weights[worker.ID]
		if weight <= 0 {
			weight = 1 // 默认权重为1
		}
		totalWeight += weight
	}
	
	if totalWeight == 0 {
		// 如果所有权重为0，使用随机选择
		return onlineWorkers[rand.Intn(len(onlineWorkers))], nil
	}
	
	// 根据权重随机选择
	r := rand.Intn(totalWeight)
	current := 0
	for _, worker := range onlineWorkers {
		weight := lb.weights[worker.ID]
		if weight <= 0 {
			weight = 1
		}
		current += weight
		if r < current {
			return worker, nil
		}
	}
	
	return onlineWorkers[0], nil
}

func (lb *WeightedLoadBalancer) UpdateStats(workerID string, stats *common.WorkerStats) {
	// 可以根据统计信息动态调整权重
}

// SetWeight 设置节点权重
func (lb *WeightedLoadBalancer) SetWeight(workerID string, weight int) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.weights[workerID] = weight
}

// Errors
var (
	ErrNoWorkers = &LoadBalancerError{Msg: "no workers available"}
)

type LoadBalancerError struct {
	Msg string
}

func (e *LoadBalancerError) Error() string {
	return e.Msg
}

// NewLoadBalancer 根据策略创建负载均衡器
func NewLoadBalancer(strategy Strategy) LoadBalancer {
	switch strategy {
	case StrategyRoundRobin:
		return NewRoundRobinLoadBalancer()
	case StrategyRandom:
		return NewRandomLoadBalancer()
	case StrategyLeastLoad:
		return NewLeastLoadLoadBalancer()
	case StrategyWeighted:
		return NewWeightedLoadBalancer()
	default:
		return NewRoundRobinLoadBalancer()
	}
}

