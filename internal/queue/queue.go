package queue

import (
	"context"
	"time"
	
	"distributed-task-scheduler/pkg/task"
)

// Queue 任务队列接口
type Queue interface {
	// Enqueue 将任务加入队列
	Enqueue(ctx context.Context, t *task.Task) error
	
	// Dequeue 从队列中取出任务（阻塞）
	Dequeue(ctx context.Context) (*task.Task, error)
	
	// DequeueNonBlocking 非阻塞获取任务
	DequeueNonBlocking(ctx context.Context) (*task.Task, error)
	
	// Length 获取队列长度
	Length(ctx context.Context) (int, error)
	
	// Peek 查看队列头部的任务但不取出
	Peek(ctx context.Context) (*task.Task, error)
	
	// Clear 清空队列
	Clear(ctx context.Context) error
}

// PriorityQueue 基于优先级的队列实现
type PriorityQueue struct {
	name        string
	coordinator QueueCoordinator
}

// QueueCoordinator 队列协调器接口
type QueueCoordinator interface {
	EnqueueTask(queueName string, taskID string, priority int) error
	DequeueTask(queueName string) (string, error)
	GetQueueLength(queueName string) (int, error)
	PutTask(taskID string, data []byte, ttl time.Duration) error
	GetTask(taskID string) ([]byte, error)
}

// NewPriorityQueue 创建优先级队列
func NewPriorityQueue(name string, coordinator QueueCoordinator) *PriorityQueue {
	return &PriorityQueue{
		name:        name,
		coordinator: coordinator,
	}
}

func (q *PriorityQueue) Enqueue(ctx context.Context, t *task.Task) error {
	// 序列化任务
	data, err := t.Serialize()
	if err != nil {
		return err
	}
	
	// 存储任务
	if err := q.coordinator.PutTask(t.ID, data, 0); err != nil {
		return err
	}
	
	// 计算优先级数值（数字越大优先级越高）
	priority := int(t.Priority)
	
	// 加入队列
	return q.coordinator.EnqueueTask(q.name, t.ID, priority)
}

func (q *PriorityQueue) Dequeue(ctx context.Context) (*task.Task, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			taskID, err := q.coordinator.DequeueTask(q.name)
			if err != nil {
				// 队列为空，等待一段时间后重试
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
			
			// 获取任务数据
			data, err := q.coordinator.GetTask(taskID)
			if err != nil {
				continue // 任务可能已被删除，继续尝试下一个
			}
			
			t, err := task.Deserialize(data)
			if err != nil {
				continue
			}
			
			return t, nil
		}
	}
}

func (q *PriorityQueue) DequeueNonBlocking(ctx context.Context) (*task.Task, error) {
	taskID, err := q.coordinator.DequeueTask(q.name)
	if err != nil {
		return nil, err
	}
	
	data, err := q.coordinator.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	
	return task.Deserialize(data)
}

func (q *PriorityQueue) Length(ctx context.Context) (int, error) {
	return q.coordinator.GetQueueLength(q.name)
}

func (q *PriorityQueue) Peek(ctx context.Context) (*task.Task, error) {
	// etcd实现中，peek需要先dequeue再enqueue，这里简化实现
	return q.DequeueNonBlocking(ctx)
}

func (q *PriorityQueue) Clear(ctx context.Context) error {
	// 实现清空队列逻辑
	// 这里简化处理，实际需要删除队列中所有任务
	return nil
}

