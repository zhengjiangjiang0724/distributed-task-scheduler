package common

import (
	"time"
)

// NodeInfo 节点信息
type NodeInfo struct {
	ID           string            `json:"id"`
	Role         string            `json:"role"` // "scheduler" or "worker"
	Address      string            `json:"address"`
	Port         int               `json:"port"`
	Capabilities map[string]string `json:"capabilities"`
	Load         float64           `json:"load"` // 当前负载 0.0-1.0
	Status       string            `json:"status"` // "online" or "offline"
	RegisteredAt time.Time         `json:"registered_at"`
	LastHeartbeat time.Time        `json:"last_heartbeat"`
}

// WorkerStats 工作节点统计信息
type WorkerStats struct {
	WorkerID      string        `json:"worker_id"`
	TotalTasks    int64         `json:"total_tasks"`
	RunningTasks  int           `json:"running_tasks"`
	CompletedTasks int64        `json:"completed_tasks"`
	FailedTasks   int64         `json:"failed_tasks"`
	CPUUsage      float64       `json:"cpu_usage"`
	MemoryUsage   float64       `json:"memory_usage"`
	Uptime        time.Duration `json:"uptime"`
}

// SchedulerStats 调度器统计信息
type SchedulerStats struct {
	TotalTasks     int64         `json:"total_tasks"`
	QueuedTasks    int           `json:"queued_tasks"`
	RunningTasks   int           `json:"running_tasks"`
	CompletedTasks int64         `json:"completed_tasks"`
	FailedTasks    int64         `json:"failed_tasks"`
	Workers        int           `json:"workers"`
	Uptime         time.Duration `json:"uptime"`
}

