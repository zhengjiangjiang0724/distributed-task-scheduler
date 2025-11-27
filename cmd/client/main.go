package main

import (
	"flag"
	"fmt"
	"time"

	"distributed-task-scheduler/internal/coordinator"
	"distributed-task-scheduler/pkg/task"
	"go.uber.org/zap"
)

func main() {
	var (
		etcdEndpoints = flag.String("etcd-endpoints", "localhost:2379", "Etcd endpoints")
		queueName     = flag.String("queue", "default", "Task queue name")
		taskType      = flag.String("type", "echo", "Task type (echo, compute, unreliable)")
		taskName      = flag.String("name", "test-task", "Task name")
		priority      = flag.Int("priority", 1, "Task priority (0=low, 1=normal, 2=high, 3=urgent)")
		count         = flag.Int("count", 1, "Number of tasks to submit")
	)
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 创建协调器（仅用于提交任务）
	endpoints := parseEndpoints(*etcdEndpoints)
	coord, err := coordinator.NewEtcdCoordinator(endpoints, "client-"+fmt.Sprintf("%d", time.Now().Unix()), logger)
	if err != nil {
		logger.Fatal("failed to create coordinator", zap.Error(err))
	}
	defer coord.Close()

	for i := 0; i < *count; i++ {
		t := &task.Task{
			ID:       fmt.Sprintf("%s-%d-%d", *taskName, time.Now().Unix(), i),
			Name:     fmt.Sprintf("%s-%d", *taskName, i),
			Type:     *taskType,
			Status:   task.StatusPending,
			Priority: task.Priority(*priority),
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("Hello from task %d", i),
			},
			MaxRetries: 3,
			Timeout:    5 * time.Minute,
		}

		// 序列化任务
		data, err := t.Serialize()
		if err != nil {
			logger.Error("failed to serialize task", zap.Error(err))
			continue
		}

		// 存储任务
		if err := coord.PutTask(t.ID, data, 0); err != nil {
			logger.Error("failed to store task", zap.Error(err))
			continue
		}

		// 加入队列
		if err := coord.EnqueueTask(*queueName, t.ID, *priority); err != nil {
			logger.Error("failed to enqueue task", zap.Error(err))
			continue
		}

		fmt.Printf("Task submitted: %s (type: %s, priority: %d)\n", t.ID, t.Type, *priority)
	}

	fmt.Printf("Submitted %d tasks\n", *count)
}

func parseEndpoints(endpointsStr string) []string {
	if endpointsStr == "" {
		return []string{"localhost:2379"}
	}
	
	endpoints := make([]string, 0)
	current := ""
	for _, char := range endpointsStr {
		if char == ',' {
			if current != "" {
				endpoints = append(endpoints, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		endpoints = append(endpoints, current)
	}
	
	if len(endpoints) == 0 {
		return []string{"localhost:2379"}
	}
	
	return endpoints
}

