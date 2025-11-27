package tests

import (
	"context"
	"testing"
	"time"

	"distributed-task-scheduler/internal/coordinator"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/pkg/task"
	"go.uber.org/zap"
)

// BenchmarkEnqueueDequeue 测试队列的入队和出队性能
func BenchmarkEnqueueDequeue(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	
	coord, err := coordinator.NewEtcdCoordinator(
		[]string{"localhost:2379"},
		"bench-test",
		logger,
	)
	if err != nil {
		b.Skipf("etcd not available: %v", err)
		return
	}
	defer coord.Close()
	
	taskQueue := queue.NewPriorityQueue("benchmark", coord)
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := &task.Task{
				ID:       "bench-task",
				Name:     "bench",
				Type:     "echo",
				Status:   task.StatusQueued,
				Priority: task.PriorityNormal,
				Payload:  make(map[string]interface{}),
			}
			
			if err := taskQueue.Enqueue(ctx, t); err != nil {
				b.Error(err)
				return
			}
			
			if _, err := taskQueue.DequeueNonBlocking(ctx); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkTaskSerialization 测试任务序列化性能
func BenchmarkTaskSerialization(b *testing.B) {
	t := &task.Task{
		ID:       "test-task",
		Name:     "Test Task",
		Type:     "echo",
		Status:   task.StatusPending,
		Priority: task.PriorityNormal,
		Payload: map[string]interface{}{
			"key1": "value1",
			"key2": 123,
			"key3": true,
		},
		Metadata: map[string]string{
			"meta1": "value1",
		},
		CreatedAt:  time.Now(),
		MaxRetries: 3,
		Timeout:    5 * time.Minute,
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := t.Serialize()
		if err != nil {
			b.Error(err)
		}
	}
}

// BenchmarkTaskDeserialization 测试任务反序列化性能
func BenchmarkTaskDeserialization(b *testing.B) {
	t := &task.Task{
		ID:       "test-task",
		Name:     "Test Task",
		Type:     "echo",
		Status:   task.StatusPending,
		Priority: task.PriorityNormal,
		Payload: map[string]interface{}{
			"key1": "value1",
			"key2": 123,
			"key3": true,
		},
		CreatedAt:  time.Now(),
		MaxRetries: 3,
		Timeout:    5 * time.Minute,
	}
	
	data, _ := t.Serialize()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := task.Deserialize(data)
		if err != nil {
			b.Error(err)
		}
	}
}

