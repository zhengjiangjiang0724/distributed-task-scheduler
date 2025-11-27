package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distributed-task-scheduler/internal/common"
	"distributed-task-scheduler/internal/coordinator"
	"distributed-task-scheduler/pkg/task"
	"go.uber.org/zap"
)

// Worker 工作节点
type Worker struct {
	nodeID      string
	coordinator *coordinator.EtcdCoordinator
	logger      *zap.Logger
	
	// 执行器注册表
	executors   map[string]task.Executor
	executorsMu sync.RWMutex
	
	// 状态管理
	running     bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	
	// 当前执行的任务
	runningTasks map[string]*runningTask
	runningMu    sync.RWMutex
	
	// 统计信息
	stats       *common.WorkerStats
	statsMu     sync.RWMutex
	startTime   time.Time
	
	// 配置
	maxConcurrentTasks int
	queueName          string
}

type runningTask struct {
	task      *task.Task
	ctx       context.Context
	cancel    context.CancelFunc
	startTime time.Time
}

// NewWorker 创建工作节点
func NewWorker(nodeID string, etcdEndpoints []string, queueName string, maxConcurrentTasks int, logger *zap.Logger) (*Worker, error) {
	coord, err := coordinator.NewEtcdCoordinator(etcdEndpoints, nodeID, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator: %w", err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	w := &Worker{
		nodeID:            nodeID,
		coordinator:       coord,
		logger:            logger,
		executors:         make(map[string]task.Executor),
		ctx:               ctx,
		cancel:            cancel,
		runningTasks:      make(map[string]*runningTask),
		stats:             &common.WorkerStats{WorkerID: nodeID},
		startTime:         time.Now(),
		maxConcurrentTasks: maxConcurrentTasks,
		queueName:         queueName,
	}
	
	return w, nil
}

// RegisterExecutor 注册任务执行器
func (w *Worker) RegisterExecutor(executor task.Executor) {
	w.executorsMu.Lock()
	defer w.executorsMu.Unlock()
	w.executors[executor.Type()] = executor
	w.logger.Info("executor registered", zap.String("type", executor.Type()))
}

// Start 启动工作节点
func (w *Worker) Start() error {
	if w.running {
		return fmt.Errorf("worker is already running")
	}
	
	// 注册工作节点
	node := &common.NodeInfo{
		ID:            w.nodeID,
		Role:          "worker",
		Address:       "localhost",
		Port:          8081,
		Capabilities:  make(map[string]string),
		Load:          0.0,
		Status:        "online",
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}
	
	if err := w.coordinator.RegisterNode(node); err != nil {
		return fmt.Errorf("failed to register worker node: %w", err)
	}
	
	w.running = true
	w.startTime = time.Now()
	
	// 启动任务监听循环
	w.wg.Add(1)
	go w.taskLoop()
	
	// 启动统计信息更新
	w.wg.Add(1)
	go w.statsUpdateLoop()
	
	w.logger.Info("worker started", zap.String("node_id", w.nodeID), zap.Int("max_concurrent", w.maxConcurrentTasks))
	return nil
}

// taskLoop 任务监听循环
func (w *Worker) taskLoop() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			// 检查并发限制
			w.runningMu.RLock()
			currentRunning := len(w.runningTasks)
			w.runningMu.RUnlock()
			
			if currentRunning >= w.maxConcurrentTasks {
				continue
			}
			
			// 从队列中拉取任务
			t, err := w.PollTask()
			if err != nil {
				// 队列为空或其他错误，继续等待
				continue
			}
			
			// 检查任务是否已经在运行
			w.runningMu.RLock()
			if _, exists := w.runningTasks[t.ID]; exists {
				w.runningMu.RUnlock()
				continue
			}
			w.runningMu.RUnlock()
			
			// 检查是否有对应的执行器
			w.executorsMu.RLock()
			executor, exists := w.executors[t.Type]
			w.executorsMu.RUnlock()
			
			if !exists {
				w.logger.Warn("no executor found for task type", zap.String("task_id", t.ID), zap.String("type", t.Type))
				w.markTaskFailed(t, fmt.Sprintf("no executor for type: %s", t.Type))
				continue
			}
			
			// 启动任务执行
			w.wg.Add(1)
			go w.executeTask(t, executor)
		}
	}
}

// PollTask 轮询获取任务（工作节点主动拉取）
func (w *Worker) PollTask() (*task.Task, error) {
	taskID, err := w.coordinator.DequeueTask(w.queueName)
	if err != nil {
		return nil, err
	}
	
	data, err := w.coordinator.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	
	t, err := task.Deserialize(data)
	if err != nil {
		return nil, err
	}
	
	// 检查任务是否分配给当前worker
	if t.WorkerID != "" && t.WorkerID != w.nodeID {
		// 任务分配给了其他worker，重新放回队列
		w.coordinator.EnqueueTask(w.queueName, taskID, int(t.Priority))
		return nil, fmt.Errorf("task assigned to other worker")
	}
	
	// 如果任务没有指定worker，分配给当前worker
	if t.WorkerID == "" {
		t.WorkerID = w.nodeID
		t.Status = task.StatusRunning
		now := time.Now()
		t.StartedAt = &now
		
		data, _ := t.Serialize()
		w.coordinator.PutTask(taskID, data, 0)
	}
	
	return t, nil
}

// executeTask 执行任务
func (w *Worker) executeTask(t *task.Task, executor task.Executor) {
	defer w.wg.Done()
	
	// 创建任务上下文
	taskCtx, cancel := context.WithCancel(w.ctx)
	if t.Timeout > 0 {
		taskCtx, cancel = context.WithTimeout(taskCtx, t.Timeout)
	}
	
	rt := &runningTask{
		task:      t,
		ctx:       taskCtx,
		cancel:    cancel,
		startTime: time.Now(),
	}
	
	// 记录正在运行的任务
	w.runningMu.Lock()
	w.runningTasks[t.ID] = rt
	w.runningMu.Unlock()
	
	// 更新统计
	w.updateStats(func(stats *common.WorkerStats) {
		stats.RunningTasks++
		stats.TotalTasks++
	})
	
	w.logger.Info("task execution started", zap.String("task_id", t.ID), zap.String("type", t.Type))
	
	// 执行任务
	result, err := executor.Execute(taskCtx, t)
	
	// 从运行列表中移除
	w.runningMu.Lock()
	delete(w.runningTasks, t.ID)
	w.runningMu.Unlock()
	
	// 更新任务状态
	if err != nil {
		w.logger.Error("task execution failed", zap.String("task_id", t.ID), zap.Error(err))
		
		if t.CanRetry() {
			t.RetryCount++
			t.Status = task.StatusQueued
			t.WorkerID = ""
			// 重新加入队列
			data, _ := t.Serialize()
			w.coordinator.PutTask(t.ID, data, 0)
			w.coordinator.EnqueueTask(w.queueName, t.ID, int(t.Priority))
		} else {
			w.markTaskFailed(t, err.Error())
		}
		
		w.updateStats(func(stats *common.WorkerStats) {
			stats.RunningTasks--
			stats.FailedTasks++
		})
	} else {
		w.markTaskCompleted(t, result)
		
		w.updateStats(func(stats *common.WorkerStats) {
			stats.RunningTasks--
			stats.CompletedTasks++
		})
		
		w.logger.Info("task execution completed", zap.String("task_id", t.ID), zap.Duration("duration", result.Duration))
	}
	
	cancel()
}

// markTaskCompleted 标记任务完成
func (w *Worker) markTaskCompleted(t *task.Task, result *task.TaskResult) {
	now := time.Now()
	t.Status = task.StatusCompleted
	t.CompletedAt = &now
	if result != nil {
		t.Result = result.Result
	}
	
	data, err := t.Serialize()
	if err != nil {
		w.logger.Error("failed to serialize completed task", zap.Error(err))
		return
	}
	
	if err := w.coordinator.PutTask(t.ID, data, 0); err != nil {
		w.logger.Error("failed to update completed task", zap.Error(err))
	}
}

// markTaskFailed 标记任务失败
func (w *Worker) markTaskFailed(t *task.Task, errorMsg string) {
	now := time.Now()
	t.Status = task.StatusFailed
	t.CompletedAt = &now
	t.Error = errorMsg
	
	data, err := t.Serialize()
	if err != nil {
		w.logger.Error("failed to serialize failed task", zap.Error(err))
		return
	}
	
	if err := w.coordinator.PutTask(t.ID, data, 0); err != nil {
		w.logger.Error("failed to update failed task", zap.Error(err))
	}
}

// updateStats 更新统计信息
func (w *Worker) updateStats(updater func(*common.WorkerStats)) {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	updater(w.stats)
}

// statsUpdateLoop 统计信息更新循环
func (w *Worker) statsUpdateLoop() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.updateLoadStats()
		}
	}
}

// updateLoadStats 更新负载统计
func (w *Worker) updateLoadStats() {
	w.runningMu.RLock()
	runningCount := len(w.runningTasks)
	w.runningMu.RUnlock()
	
	// 更新负载（简化实现）
	load := float64(runningCount) / float64(w.maxConcurrentTasks)
	if load > 1.0 {
		load = 1.0
	}
	
	// 这里可以添加实际的CPU、内存监控
	w.updateStats(func(stats *common.WorkerStats) {
		stats.CPUUsage = load * 0.7 // 模拟CPU使用率
		stats.MemoryUsage = load * 0.6 // 模拟内存使用率
		stats.RunningTasks = runningCount
	})
}

// GetStats 获取统计信息
func (w *Worker) GetStats() *common.WorkerStats {
	w.statsMu.RLock()
	defer w.statsMu.RUnlock()
	
	stats := *w.stats
	stats.Uptime = time.Since(w.startTime)
	
	w.runningMu.RLock()
	stats.RunningTasks = len(w.runningTasks)
	w.runningMu.RUnlock()
	
	return &stats
}

// Stop 停止工作节点
func (w *Worker) Stop() error {
	if !w.running {
		return nil
	}
	
	w.running = false
	w.cancel()
	
	// 等待所有任务完成或超时
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		w.logger.Warn("some tasks didn't complete within timeout")
		// 取消所有运行中的任务
		w.runningMu.Lock()
		for _, rt := range w.runningTasks {
			rt.cancel()
		}
		w.runningMu.Unlock()
	}
	
	// 注销节点
	if err := w.coordinator.UnregisterNode("worker", w.nodeID); err != nil {
		w.logger.Error("failed to unregister worker", zap.Error(err))
	}
	
	// 关闭协调器
	if err := w.coordinator.Close(); err != nil {
		w.logger.Error("failed to close coordinator", zap.Error(err))
	}
	
	w.logger.Info("worker stopped")
	return nil
}

