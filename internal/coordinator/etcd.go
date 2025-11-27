package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"distributed-task-scheduler/internal/common"
	"go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// etcd key prefixes
	prefixNodes      = "/nodes"
	prefixTasks      = "/tasks"
	prefixQueues     = "/queues"
	prefixLocks      = "/locks"
	
	// lease time
	defaultLeaseTime = 10 * time.Second
	heartbeatInterval = 3 * time.Second
)

// EtcdCoordinator etcd协调器
type EtcdCoordinator struct {
	client     *clientv3.Client
	logger     *zap.Logger
	leaseID    clientv3.LeaseID
	ctx        context.Context
	cancel     context.CancelFunc
	nodeID     string
	leaseKeepAlive <-chan *clientv3.LeaseKeepAliveResponse
}

// NewEtcdCoordinator 创建etcd协调器
func NewEtcdCoordinator(endpoints []string, nodeID string, logger *zap.Logger) (*EtcdCoordinator, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	coord := &EtcdCoordinator{
		client:  client,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		nodeID:  nodeID,
	}
	
	// 创建lease并保持心跳
	if err := coord.startLease(); err != nil {
		coord.Close()
		return nil, err
	}
	
	return coord, nil
}

// startLease 启动lease并保持心跳
func (c *EtcdCoordinator) startLease() error {
	lease, err := c.client.Grant(c.ctx, int64(defaultLeaseTime.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}
	
	c.leaseID = lease.ID
	
	// 保持lease活跃
	keepAlive, err := c.client.KeepAlive(c.ctx, c.leaseID)
	if err != nil {
		return fmt.Errorf("failed to keep alive lease: %w", err)
	}
	
	c.leaseKeepAlive = keepAlive
	
	// 监听lease保持状态
	go func() {
		for {
			select {
			case resp := <-c.leaseKeepAlive:
				if resp == nil {
					c.logger.Warn("lease keep alive channel closed")
					return
				}
				c.logger.Debug("lease keep alive", zap.Int64("lease_id", int64(c.leaseID)))
			case <-c.ctx.Done():
				return
			}
		}
	}()
	
	return nil
}

// RegisterNode 注册节点
func (c *EtcdCoordinator) RegisterNode(node *common.NodeInfo) error {
	node.LastHeartbeat = time.Now()
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %w", err)
	}
	
	key := path.Join(prefixNodes, node.Role, node.ID)
	_, err = c.client.Put(c.ctx, key, string(data), clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}
	
	c.logger.Info("node registered", zap.String("node_id", node.ID), zap.String("role", node.Role))
	
	// 定期更新心跳
	go c.heartbeat(node)
	
	return nil
}

// heartbeat 定期更新节点心跳
func (c *EtcdCoordinator) heartbeat(node *common.NodeInfo) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			node.LastHeartbeat = time.Now()
			node.Load = getCurrentLoad() // 这里可以扩展获取实际负载
			data, err := json.Marshal(node)
			if err != nil {
				c.logger.Error("failed to marshal node info for heartbeat", zap.Error(err))
				continue
			}
			
			key := path.Join(prefixNodes, node.Role, node.ID)
			_, err = c.client.Put(c.ctx, key, string(data), clientv3.WithLease(c.leaseID))
			if err != nil {
				c.logger.Error("failed to update heartbeat", zap.Error(err))
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// getCurrentLoad 获取当前负载（简化实现）
func getCurrentLoad() float64 {
	// 实际实现中可以从系统获取CPU、内存等负载信息
	return 0.5
}

// UnregisterNode 注销节点
func (c *EtcdCoordinator) UnregisterNode(role, nodeID string) error {
	key := path.Join(prefixNodes, role, nodeID)
	_, err := c.client.Delete(c.ctx, key)
	if err != nil {
		return fmt.Errorf("failed to unregister node: %w", err)
	}
	
	c.logger.Info("node unregistered", zap.String("node_id", nodeID), zap.String("role", role))
	return nil
}

// ListNodes 列出所有节点
func (c *EtcdCoordinator) ListNodes(role string) ([]*common.NodeInfo, error) {
	key := path.Join(prefixNodes, role)
	resp, err := c.client.Get(c.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	
	nodes := make([]*common.NodeInfo, 0)
	for _, kv := range resp.Kvs {
		var node common.NodeInfo
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			c.logger.Warn("failed to unmarshal node info", zap.Error(err))
			continue
		}
		nodes = append(nodes, &node)
	}
	
	return nodes, nil
}

// WatchNodes 监听节点变化
func (c *EtcdCoordinator) WatchNodes(role string, handler func([]*common.NodeInfo)) {
	key := path.Join(prefixNodes, role)
	watchChan := c.client.Watch(c.ctx, key, clientv3.WithPrefix())
	
	go func() {
		for {
			select {
			case watchResp := <-watchChan:
				if watchResp.Canceled {
					c.logger.Warn("watch canceled")
					return
				}
				
				// 重新获取所有节点
				nodes, err := c.ListNodes(role)
				if err != nil {
					c.logger.Error("failed to list nodes after watch event", zap.Error(err))
					continue
				}
				
				handler(nodes)
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// PutTask 存储任务
func (c *EtcdCoordinator) PutTask(taskID string, data []byte, ttl time.Duration) error {
	key := path.Join(prefixTasks, taskID)
	
	var opts []clientv3.OpOption
	if ttl > 0 {
		lease, err := c.client.Grant(c.ctx, int64(ttl.Seconds()))
		if err != nil {
			return fmt.Errorf("failed to create lease for task: %w", err)
		}
		opts = append(opts, clientv3.WithLease(lease.ID))
	}
	
	_, err := c.client.Put(c.ctx, key, string(data), opts...)
	return err
}

// GetTask 获取任务
func (c *EtcdCoordinator) GetTask(taskID string) ([]byte, error) {
	key := path.Join(prefixTasks, taskID)
	resp, err := c.client.Get(c.ctx, key)
	if err != nil {
		return nil, err
	}
	
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}
	
	return resp.Kvs[0].Value, nil
}

// DeleteTask 删除任务
func (c *EtcdCoordinator) DeleteTask(taskID string) error {
	key := path.Join(prefixTasks, taskID)
	_, err := c.client.Delete(c.ctx, key)
	return err
}

// EnqueueTask 将任务加入队列
func (c *EtcdCoordinator) EnqueueTask(queueName string, taskID string, priority int) error {
	// 使用etcd的有序key来实现优先级队列
	// 格式: /queues/{queue}/priority_{priority}_{timestamp}_{taskID}
	timestamp := time.Now().UnixNano()
	key := path.Join(prefixQueues, queueName, fmt.Sprintf("priority_%d_%d_%s", priority, timestamp, taskID))
	
	_, err := c.client.Put(c.ctx, key, taskID)
	return err
}

// DequeueTask 从队列中取出任务
func (c *EtcdCoordinator) DequeueTask(queueName string) (string, error) {
	key := path.Join(prefixQueues, queueName)
	
	// 使用事务获取并删除第一个任务
	for {
		resp, err := c.client.Get(c.ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(1))
		if err != nil {
			return "", err
		}
		
		if len(resp.Kvs) == 0 {
			return "", fmt.Errorf("queue is empty")
		}
		
		kv := resp.Kvs[0]
		taskID := string(kv.Value)
		
		// 尝试删除
		delResp, err := c.client.Delete(c.ctx, string(kv.Key))
		if err != nil {
			return "", err
		}
		
		// 如果删除成功（说明没有并发竞争）
		if delResp.Deleted > 0 {
			return taskID, nil
		}
		
		// 如果删除失败，说明被其他节点取走了，继续尝试下一个
		time.Sleep(10 * time.Millisecond)
	}
}

// TryLock 尝试获取分布式锁
func (c *EtcdCoordinator) TryLock(lockKey string, ttl time.Duration) (bool, error) {
	key := path.Join(prefixLocks, lockKey)
	
	lease, err := c.client.Grant(c.ctx, int64(ttl.Seconds()))
	if err != nil {
		return false, err
	}
	
	// 尝试创建key，如果key已存在则获取锁失败
	txn := c.client.Txn(c.ctx)
	txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, c.nodeID, clientv3.WithLease(lease.ID))).
		Else(clientv3.OpGet(key))
	
	txnResp, err := txn.Commit()
	if err != nil {
		return false, err
	}
	
	return txnResp.Succeeded, nil
}

// Unlock 释放锁
func (c *EtcdCoordinator) Unlock(lockKey string) error {
	key := path.Join(prefixLocks, lockKey)
	_, err := c.client.Delete(c.ctx, key)
	return err
}

// Close 关闭协调器
func (c *EtcdCoordinator) Close() error {
	c.cancel()
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// GetQueueLength 获取队列长度
func (c *EtcdCoordinator) GetQueueLength(queueName string) (int, error) {
	key := path.Join(prefixQueues, queueName)
	resp, err := c.client.Get(c.ctx, key, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	
	return int(resp.Count), nil
}

// WatchQueue 监听队列变化
func (c *EtcdCoordinator) WatchQueue(queueName string, handler func(string)) {
	key := path.Join(prefixQueues, queueName)
	watchChan := c.client.Watch(c.ctx, key, clientv3.WithPrefix())
	
	go func() {
		for {
			select {
			case watchResp := <-watchChan:
				if watchResp.Canceled {
					return
				}
				
				for _, event := range watchResp.Events {
					if event.Type == clientv3.EventTypePut {
						taskID := string(event.Kv.Value)
						handler(taskID)
					}
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// ParsePriorityFromQueueKey 从队列key中解析优先级
func ParsePriorityFromQueueKey(key string) int {
	parts := strings.Split(key, "_")
	if len(parts) >= 2 && parts[0] == "priority" {
		var priority int
		fmt.Sscanf(parts[1], "%d", &priority)
		return priority
	}
	return 0
}

