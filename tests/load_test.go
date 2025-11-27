package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-task-scheduler/internal/coordinator"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/pkg/task"
	"go.uber.org/zap"
)

// TestConcurrentEnqueue 测试并发入队
func TestConcurrentEnqueue(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	
	coord, err := coordinator.NewEtcdCoordinator(
		[]string{"localhost:2379"},
		"load-test",
		logger,
	)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
		return
	}
	defer coord.Close()
	
	taskQueue := queue.NewPriorityQueue("load-test", coord)
	ctx := context.Background()
	
	workers := 100
	tasksPerWorker := 10
	var successCount int64
	
	var wg sync.WaitGroup
	wg.Add(workers)
	
	start := time.Now()
	
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			
		for j := 0; j < tasksPerWorker; j++ {
			tsk := &task.Task{
				ID:       fmt.Sprintf("task-%d-%d", workerID, j),
				Name:     fmt.Sprintf("Task %d-%d", workerID, j),
				Type:     "echo",
				Status:   task.StatusQueued,
				Priority: task.PriorityNormal,
				Payload:  make(map[string]interface{}),
			}
			
			if err := taskQueue.Enqueue(ctx, tsk); err != nil {
				t.Errorf("failed to enqueue task: %v", err)
				return
			}
				
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}
	
	wg.Wait()
	duration := time.Since(start)
	
	t.Logf("Enqueued %d tasks in %v", successCount, duration)
	t.Logf("Throughput: %.2f tasks/second", float64(successCount)/duration.Seconds())
}

// TestConcurrentDequeue 测试并发出队
func TestConcurrentDequeue(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	
	coord, err := coordinator.NewEtcdCoordinator(
		[]string{"localhost:2379"},
		"load-test-dequeue",
		logger,
	)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
		return
	}
	defer coord.Close()
	
	taskQueue := queue.NewPriorityQueue("load-test-dequeue", coord)
	ctx := context.Background()
	
	// 先入队一些任务
	taskCount := 100
	for i := 0; i < taskCount; i++ {
		tsk := &task.Task{
			ID:       fmt.Sprintf("task-%d", i),
			Name:     fmt.Sprintf("Task %d", i),
			Type:     "echo",
			Status:   task.StatusQueued,
			Priority: task.PriorityNormal,
			Payload:  make(map[string]interface{}),
		}
		taskQueue.Enqueue(ctx, tsk)
	}
	
	workers := 10
	var dequeuedCount int64
	
	var wg sync.WaitGroup
	wg.Add(workers)
	
	start := time.Now()
	
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			
		for {
			tsk, err := taskQueue.DequeueNonBlocking(ctx)
			if err != nil {
				break
			}
			if tsk != nil {
				atomic.AddInt64(&dequeuedCount, 1)
			}
		}
		}()
	}
	
	wg.Wait()
	duration := time.Since(start)
	
	t.Logf("Dequeued %d tasks in %v", dequeuedCount, duration)
	t.Logf("Throughput: %.2f tasks/second", float64(dequeuedCount)/duration.Seconds())
	
	if dequeuedCount != int64(taskCount) {
		t.Errorf("expected %d tasks, got %d", taskCount, dequeuedCount)
	}
}

// TestPriorityQueue 测试优先级队列
func TestPriorityQueue(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	
	coord, err := coordinator.NewEtcdCoordinator(
		[]string{"localhost:2379"},
		"priority-test",
		logger,
	)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
		return
	}
	defer coord.Close()
	
	taskQueue := queue.NewPriorityQueue("priority-test", coord)
	ctx := context.Background()
	
	// 按不同优先级入队
	tasks := []*task.Task{
		{ID: "low", Priority: task.PriorityLow},
		{ID: "normal", Priority: task.PriorityNormal},
		{ID: "high", Priority: task.PriorityHigh},
		{ID: "urgent", Priority: task.PriorityUrgent},
	}
	
	for _, tsk := range tasks {
		tsk.Name = tsk.ID
		tsk.Type = "echo"
		tsk.Status = task.StatusQueued
		tsk.Payload = make(map[string]interface{})
		taskQueue.Enqueue(ctx, tsk)
	}
	
	// 出队，应该按优先级顺序
	expectedOrder := []string{"urgent", "high", "normal", "low"}
	for _, expectedID := range expectedOrder {
		dequeued, err := taskQueue.DequeueNonBlocking(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}
		
		if dequeued.ID != expectedID {
			t.Errorf("expected %s, got %s", expectedID, dequeued.ID)
		}
	}
}

