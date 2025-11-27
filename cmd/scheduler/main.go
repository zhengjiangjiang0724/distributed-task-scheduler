package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-task-scheduler/internal/scheduler"
	"distributed-task-scheduler/pkg/loadbalancer"
	"go.uber.org/zap"
)

func main() {
	var (
		nodeID        = flag.String("node-id", fmt.Sprintf("scheduler-%d", time.Now().Unix()), "Scheduler node ID")
		etcdEndpoints = flag.String("etcd-endpoints", "localhost:2379", "Etcd endpoints (comma-separated)")
		queueName     = flag.String("queue", "default", "Task queue name")
		strategy      = flag.String("strategy", "round_robin", "Load balancing strategy")
		logLevel      = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	)
	flag.Parse()

	// 初始化日志
	logger := initLogger(*logLevel)
	defer logger.Sync()

	logger.Info("starting scheduler",
		zap.String("node_id", *nodeID),
		zap.String("etcd_endpoints", *etcdEndpoints),
		zap.String("queue", *queueName),
	)

	// 解析etcd endpoints
	endpoints := parseEndpoints(*etcdEndpoints)
	
	// 解析负载均衡策略
	lbStrategy := loadbalancer.Strategy(*strategy)

	// 创建调度器
	sched, err := scheduler.NewScheduler(*nodeID, endpoints, *queueName, lbStrategy, logger)
	if err != nil {
		logger.Fatal("failed to create scheduler", zap.Error(err))
	}

	// 启动调度器
	if err := sched.Start(); err != nil {
		logger.Fatal("failed to start scheduler", zap.Error(err))
	}

	// 启动统计信息输出
	go printStats(sched, logger)

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutting down scheduler...")

	// 停止调度器
	if err := sched.Stop(); err != nil {
		logger.Error("error stopping scheduler", zap.Error(err))
	}

	logger.Info("scheduler stopped")
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

func printStats(s *scheduler.Scheduler, logger *zap.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := s.GetStats()
		logger.Info("scheduler stats",
			zap.Int64("total_tasks", stats.TotalTasks),
			zap.Int("queued_tasks", stats.QueuedTasks),
			zap.Int("running_tasks", stats.RunningTasks),
			zap.Int64("completed_tasks", stats.CompletedTasks),
			zap.Int64("failed_tasks", stats.FailedTasks),
			zap.Int("workers", stats.Workers),
			zap.Duration("uptime", stats.Uptime),
		)
	}
}

