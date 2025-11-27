package task

import (
	"context"
	"time"
)

// Executor 任务执行器接口
type Executor interface {
	Execute(ctx context.Context, task *Task) (*TaskResult, error)
	Type() string
}

// ExecutorFunc 函数式执行器
type ExecutorFunc func(ctx context.Context, task *Task) (*TaskResult, error)

func (f ExecutorFunc) Execute(ctx context.Context, task *Task) (*TaskResult, error) {
	return f(ctx, task)
}

func (f ExecutorFunc) Type() string {
	return "function"
}

// BaseExecutor 基础执行器
type BaseExecutor struct {
	taskType string
	handler  func(ctx context.Context, task *Task) (*TaskResult, error)
}

// NewBaseExecutor 创建基础执行器
func NewBaseExecutor(taskType string, handler func(ctx context.Context, task *Task) (*TaskResult, error)) *BaseExecutor {
	return &BaseExecutor{
		taskType: taskType,
		handler:  handler,
	}
}

func (e *BaseExecutor) Execute(ctx context.Context, task *Task) (*TaskResult, error) {
	start := time.Now()
	
	// 创建带超时的上下文
	var cancel context.CancelFunc
	if task.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}
	
	result, err := e.handler(ctx, task)
	if err != nil {
		return &TaskResult{
			TaskID:   task.ID,
			Status:   StatusFailed,
			Error:    err.Error(),
			Duration: time.Since(start),
		}, err
	}
	
	if result == nil {
		result = &TaskResult{
			TaskID:   task.ID,
			Status:   StatusCompleted,
			Duration: time.Since(start),
		}
	}
	
	result.Duration = time.Since(start)
	return result, nil
}

func (e *BaseExecutor) Type() string {
	return e.taskType
}

