package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distributed-task-scheduler/internal/common"
	"distributed-task-scheduler/internal/coordinator"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/pkg/loadbalancer"
	"distributed-task-scheduler/pkg/task"
	"go.uber.org/zap"
)

// Scheduler 任务调度器
type Scheduler struct {
	nodeID       string
	coordinator  *coordinator.EtcdCoordinator
	taskQueue    queue.Queue
	loadBalancer loadbalancer.LoadBalancer
	logger       *zap.Logger
	
	// 状态管理
	running      bool
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	
	// 统计信息
	stats        *common.SchedulerStats
	statsMu      sync.RWMutex
	startTime    time.Time
	
	// 任务分配映射
	assignedTasks map[string]string // taskID -> workerID
	assignedMu    sync.RWMutex
}

// NewScheduler 创建调度器
func NewScheduler(nodeID string, etcdEndpoints []string, queueName string, strategy loadbalancer.Strategy, logger *zap.Logger) (*Scheduler, error) {
	// 创建协调器
	coord, err := coordinator.NewEtcdCoordinator(etcdEndpoints, nodeID, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator: %w", err)
	}
	
	// 创建任务队列
	taskQueue := queue.NewPriorityQueue(queueName, coord)
	
	// 创建负载均衡器
	lb := loadbalancer.NewLoadBalancer(strategy)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	s := &Scheduler{
		nodeID:        nodeID,
		coordinator:   coord,
		taskQueue:     taskQueue,
		loadBalancer:  lb,
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		stats:         &common.SchedulerStats{},
		startTime:     time.Now(),
		assignedTasks: make(map[string]string),
	}
	
	return s, nil
}

// Start 启动调度器
func (s *Scheduler) Start() error {
	if s.running {
		return fmt.Errorf("scheduler is already running")
	}
	
	// 注册调度器节点
	node := &common.NodeInfo{
		ID:           s.nodeID,
		Role:         "scheduler",
		Address:      "localhost",
		Port:         8080,
		Capabilities: make(map[string]string),
		Load:         0.0,
		Status:       "online",
		RegisteredAt: time.Now(),
		LastHeartbeat: time.Now(),
	}
	
	if err := s.coordinator.RegisterNode(node); err != nil {
		return fmt.Errorf("failed to register scheduler node: %w", err)
	}
	
	s.running = true
	s.startTime = time.Now()
	
	// 启动监听工作节点
	s.wg.Add(1)
	go s.watchWorkers()
	
	// 启动调度循环
	s.wg.Add(1)
	go s.scheduleLoop()
	
	// 启动任务状态检查
	s.wg.Add(1)
	go s.taskStatusCheckLoop()
	
	s.logger.Info("scheduler started", zap.String("node_id", s.nodeID))
	return nil
}

// watchWorkers 监听工作节点变化
func (s *Scheduler) watchWorkers() {
	defer s.wg.Done()
	
	// 获取初始工作节点列表
	workers, err := s.coordinator.ListNodes("worker")
	if err != nil {
		s.logger.Error("failed to list workers", zap.Error(err))
	} else {
		s.logger.Info("initial workers loaded", zap.Int("count", len(workers)))
	}
	
	// 监听工作节点变化
	s.coordinator.WatchNodes("worker", func(workers []*common.NodeInfo) {
		s.logger.Info("workers updated", zap.Int("count", len(workers)))
		
		// 检查是否有任务需要重新分配
		s.reassignFailedTasks(workers)
	})
}

// scheduleLoop 调度循环
func (s *Scheduler) scheduleLoop() {
	defer s.wg.Done()
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.scheduleTasks(); err != nil {
				s.logger.Error("failed to schedule tasks", zap.Error(err))
			}
		}
	}
}

// scheduleTasks 调度任务
func (s *Scheduler) scheduleTasks() error {
	// 获取工作节点
	workers, err := s.coordinator.ListNodes("worker")
	if err != nil {
		return fmt.Errorf("failed to list workers: %w", err)
	}
	
	if len(workers) == 0 {
		return nil // 没有工作节点，跳过调度
	}
	
	// 获取队列长度
	queueLength, err := s.taskQueue.Length(s.ctx)
	if err != nil {
		return err
	}
	
	// 批量调度任务
	batchSize := 10
	for i := 0; i < batchSize && i < queueLength; i++ {
		// 从队列中取出任务
		t, err := s.taskQueue.DequeueNonBlocking(s.ctx)
		if err != nil {
			break // 队列为空
		}
		
		// 选择工作节点
		worker, err := s.loadBalancer.SelectWorker(workers)
		if err != nil {
			// 没有可用工作节点，将任务重新放回队列
			s.taskQueue.Enqueue(s.ctx, t)
			s.logger.Warn("no available workers, requeue task", zap.String("task_id", t.ID))
			break
		}
		
		// 分配任务到工作节点
		if err := s.assignTask(t, worker); err != nil {
			s.logger.Error("failed to assign task", zap.String("task_id", t.ID), zap.Error(err))
			// 分配失败，将任务重新放回队列
			t.RetryCount++
			if t.CanRetry() {
				s.taskQueue.Enqueue(s.ctx, t)
			} else {
				s.updateTaskStatus(t, task.StatusFailed, fmt.Sprintf("failed to assign: %v", err))
			}
			continue
		}
		
		s.updateStats(func(stats *common.SchedulerStats) {
			stats.RunningTasks++
		})
		
		s.logger.Info("task assigned", zap.String("task_id", t.ID), zap.String("worker_id", worker.ID))
	}
	
	return nil
}

// assignTask 分配任务到工作节点
func (s *Scheduler) assignTask(t *task.Task, worker *common.NodeInfo) error {
	// 更新任务状态
	t.Status = task.StatusRunning
	now := time.Now()
	t.StartedAt = &now
	t.WorkerID = worker.ID
	
	// 存储任务状态
	data, err := t.Serialize()
	if err != nil {
		return err
	}
	
	if err := s.coordinator.PutTask(t.ID, data, 0); err != nil {
		return err
	}
	
	// 记录任务分配
	s.assignedMu.Lock()
	s.assignedTasks[t.ID] = worker.ID
	s.assignedMu.Unlock()
	
	// 这里可以通过RPC或其他方式通知工作节点
	// 为了简化，我们使用etcd来传递任务分配信息
	// 工作节点会监听任务队列或直接查询任务状态
	
	return nil
}

// updateTaskStatus 更新任务状态
func (s *Scheduler) updateTaskStatus(t *task.Task, status task.TaskStatus, errorMsg string) {
	t.Status = status
	if errorMsg != "" {
		t.Error = errorMsg
	}
	
	if status == task.StatusCompleted || status == task.StatusFailed {
		now := time.Now()
		t.CompletedAt = &now
		
		s.updateStats(func(stats *common.SchedulerStats) {
			stats.RunningTasks--
			if status == task.StatusCompleted {
				stats.CompletedTasks++
			} else {
				stats.FailedTasks++
			}
		})
	}
	
	data, err := t.Serialize()
	if err != nil {
		s.logger.Error("failed to serialize task", zap.Error(err))
		return
	}
	
	if err := s.coordinator.PutTask(t.ID, data, 0); err != nil {
		s.logger.Error("failed to update task", zap.Error(err))
	}
}

// SubmitTask 提交任务
func (s *Scheduler) SubmitTask(t *task.Task) error {
	if !s.running {
		return fmt.Errorf("scheduler is not running")
	}
	
	t.Status = task.StatusQueued
	t.CreatedAt = time.Now()
	
	// 设置默认值
	if t.MaxRetries == 0 {
		t.MaxRetries = 3
	}
	if t.Timeout == 0 {
		t.Timeout = 5 * time.Minute
	}
	
	// 加入队列
	if err := s.taskQueue.Enqueue(s.ctx, t); err != nil {
		return err
	}
	
	s.updateStats(func(stats *common.SchedulerStats) {
		stats.TotalTasks++
		stats.QueuedTasks++
	})
	
	s.logger.Info("task submitted", zap.String("task_id", t.ID), zap.String("name", t.Name))
	return nil
}

// GetTaskStatus 获取任务状态
func (s *Scheduler) GetTaskStatus(taskID string) (*task.Task, error) {
	data, err := s.coordinator.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	
	return task.Deserialize(data)
}

// GetStats 获取统计信息
func (s *Scheduler) GetStats() *common.SchedulerStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	
	stats := *s.stats
	stats.Uptime = time.Since(s.startTime)
	
	// 获取当前队列长度
	if queueLength, err := s.taskQueue.Length(s.ctx); err == nil {
		stats.QueuedTasks = queueLength
	}
	
	// 获取工作节点数量
	if workers, err := s.coordinator.ListNodes("worker"); err == nil {
		stats.Workers = len(workers)
	}
	
	return &stats
}

// updateStats 更新统计信息
func (s *Scheduler) updateStats(updater func(*common.SchedulerStats)) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	updater(s.stats)
}

// reassignFailedTasks 重新分配失败的任务
func (s *Scheduler) reassignFailedTasks(workers []*common.NodeInfo) {
	if len(workers) == 0 {
		return
	}
	
	// 检查已分配但工作节点已下线的任务
	s.assignedMu.RLock()
	toReassign := make([]string, 0)
	for taskID, workerID := range s.assignedTasks {
		workerExists := false
		for _, w := range workers {
			if w.ID == workerID && w.Status == "online" {
				workerExists = true
				break
			}
		}
		if !workerExists {
			toReassign = append(toReassign, taskID)
		}
	}
	s.assignedMu.RUnlock()
	
	// 重新分配任务
	for _, taskID := range toReassign {
		t, err := s.GetTaskStatus(taskID)
		if err != nil {
			continue
		}
		
		if !t.IsTerminal() {
			// 将任务重新加入队列
			t.Status = task.StatusQueued
			t.WorkerID = ""
			s.taskQueue.Enqueue(s.ctx, t)
			
			s.assignedMu.Lock()
			delete(s.assignedTasks, taskID)
			s.assignedMu.Unlock()
			
			s.logger.Info("task requeued due to worker offline", zap.String("task_id", taskID))
		}
	}
}

// taskStatusCheckLoop 任务状态检查循环
func (s *Scheduler) taskStatusCheckLoop() {
	defer s.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkTimeoutTasks()
		}
	}
}

// checkTimeoutTasks 检查超时任务
func (s *Scheduler) checkTimeoutTasks() {
	// 这里可以实现检查超时任务的逻辑
	// 简化实现，实际中需要遍历所有运行中的任务
}

// Stop 停止调度器
func (s *Scheduler) Stop() error {
	if !s.running {
		return nil
	}
	
	s.running = false
	s.cancel()
	
	// 等待所有goroutine结束
	s.wg.Wait()
	
	// 注销节点
	if err := s.coordinator.UnregisterNode("scheduler", s.nodeID); err != nil {
		s.logger.Error("failed to unregister scheduler", zap.Error(err))
	}
	
	// 关闭协调器
	if err := s.coordinator.Close(); err != nil {
		s.logger.Error("failed to close coordinator", zap.Error(err))
	}
	
	s.logger.Info("scheduler stopped")
	return nil
}

