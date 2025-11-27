package task

import (
	"encoding/json"
	"time"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusQueued    TaskStatus = "queued"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusCancelled TaskStatus = "cancelled"
)

// Priority 任务优先级
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityUrgent
)

// Task 任务定义
type Task struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Status      TaskStatus        `json:"status"`
	Priority    Priority          `json:"priority"`
	Payload     map[string]interface{} `json:"payload"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	WorkerID    string            `json:"worker_id,omitempty"`
	RetryCount  int               `json:"retry_count"`
	MaxRetries  int               `json:"max_retries"`
	Timeout     time.Duration     `json:"timeout"`
	Result      interface{}       `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
}

// TaskResult 任务执行结果
type TaskResult struct {
	TaskID    string      `json:"task_id"`
	Status    TaskStatus  `json:"status"`
	Result    interface{} `json:"result,omitempty"`
	Error     string      `json:"error,omitempty"`
	Duration  time.Duration `json:"duration"`
}

// Serialize 序列化任务
func (t *Task) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

// Deserialize 反序列化任务
func Deserialize(data []byte) (*Task, error) {
	var task Task
	err := json.Unmarshal(data, &task)
	return &task, err
}

// IsTerminal 判断任务是否已结束
func (t *Task) IsTerminal() bool {
	return t.Status == StatusCompleted || t.Status == StatusFailed || t.Status == StatusCancelled
}

// CanRetry 判断任务是否可以重试
func (t *Task) CanRetry() bool {
	return t.Status == StatusFailed && t.RetryCount < t.MaxRetries
}

