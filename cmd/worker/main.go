package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-task-scheduler/internal/worker"
	"distributed-task-scheduler/pkg/task"
	"go.uber.org/zap"
)

func main() {
	var (
		nodeID             = flag.String("node-id", fmt.Sprintf("worker-%d", time.Now().Unix()), "Worker node ID")
		etcdEndpoints      = flag.String("etcd-endpoints", "localhost:2379", "Etcd endpoints (comma-separated)")
		queueName          = flag.String("queue", "default", "Task queue name")
		maxConcurrentTasks = flag.Int("max-concurrent", 10, "Maximum concurrent tasks")
		logLevel           = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	)
	flag.Parse()

	// 初始化日志
	logger := initLogger(*logLevel)
	defer logger.Sync()

	logger.Info("starting worker",
		zap.String("node_id", *nodeID),
		zap.String("etcd_endpoints", *etcdEndpoints),
		zap.Int("max_concurrent", *maxConcurrentTasks),
	)

	// 解析etcd endpoints
	endpoints := parseEndpoints(*etcdEndpoints)

	// 创建工作节点
	w, err := worker.NewWorker(*nodeID, endpoints, *queueName, *maxConcurrentTasks, logger)
	if err != nil {
		logger.Fatal("failed to create worker", zap.Error(err))
	}

	// 注册示例执行器
	registerExecutors(w, logger)

	// 启动工作节点
	if err := w.Start(); err != nil {
		logger.Fatal("failed to start worker", zap.Error(err))
	}

	// 启动统计信息输出
	go printStats(w, logger)

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutting down worker...")

	// 停止工作节点
	if err := w.Stop(); err != nil {
		logger.Error("error stopping worker", zap.Error(err))
	}

	logger.Info("worker stopped")
}

func initLogger(level string) *zap.Logger {
	var config zap.Config
	switch level {
	case "debug":
		config = zap.NewDevelopmentConfig()
	default:
		config = zap.NewProductionConfig()
	}

	logger, _ := config.Build()
	return logger
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

func registerExecutors(w *worker.Worker, logger *zap.Logger) {
	// 注册示例任务执行器
	w.RegisterExecutor(task.NewBaseExecutor("echo", func(ctx context.Context, t *task.Task) (*task.TaskResult, error) {
		logger.Info("executing echo task", zap.String("task_id", t.ID))
		
		// 模拟任务执行
		message := "Hello, World!"
		if msg, ok := t.Payload["message"].(string); ok {
			message = msg
		}
		
		time.Sleep(1 * time.Second) // 模拟处理时间
		
		return &task.TaskResult{
			TaskID: t.ID,
			Status: task.StatusCompleted,
			Result: map[string]interface{}{
				"message": message,
				"timestamp": time.Now().Unix(),
			},
		}, nil
	}))

	// 注册计算任务执行器
	w.RegisterExecutor(task.NewBaseExecutor("compute", func(ctx context.Context, t *task.Task) (*task.TaskResult, error) {
		logger.Info("executing compute task", zap.String("task_id", t.ID))
		
		// 模拟计算任务
		duration := 2 * time.Second
		if d, ok := t.Payload["duration"].(float64); ok {
			duration = time.Duration(d) * time.Second
		}
		
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(duration):
		}
		
		result := map[string]interface{}{
			"computed": true,
			"duration": duration.String(),
		}
		
		return &task.TaskResult{
			TaskID: t.ID,
			Status: task.StatusCompleted,
			Result: result,
		}, nil
	}))

	// 注册可能失败的任务执行器（用于测试重试）
	w.RegisterExecutor(task.NewBaseExecutor("unreliable", func(ctx context.Context, t *task.Task) (*task.TaskResult, error) {
		logger.Info("executing unreliable task", zap.String("task_id", t.ID), zap.Int("retry_count", t.RetryCount))
		
		// 前两次失败，第三次成功
		if t.RetryCount < 2 {
			return nil, fmt.Errorf("simulated failure (attempt %d)", t.RetryCount+1)
		}
		
		time.Sleep(500 * time.Millisecond)
		
		return &task.TaskResult{
			TaskID: t.ID,
			Status: task.StatusCompleted,
			Result: map[string]interface{}{
				"success": true,
				"attempts": t.RetryCount + 1,
			},
		}, nil
	}))
}

func printStats(w *worker.Worker, logger *zap.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := w.GetStats()
		logger.Info("worker stats",
			zap.String("worker_id", stats.WorkerID),
			zap.Int64("total_tasks", stats.TotalTasks),
			zap.Int("running_tasks", stats.RunningTasks),
			zap.Int64("completed_tasks", stats.CompletedTasks),
			zap.Int64("failed_tasks", stats.FailedTasks),
			zap.Float64("cpu_usage", stats.CPUUsage),
			zap.Float64("memory_usage", stats.MemoryUsage),
			zap.Duration("uptime", stats.Uptime),
		)
	}
}

