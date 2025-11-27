# 快速开始指南

本指南将帮助你快速启动和测试分布式任务调度系统。

## 前置条件

1. **安装Go**: 版本 1.21 或更高（如果使用源码编译）
2. **安装Docker和Docker Compose**: 版本 20.10+（如果使用Docker部署）
3. **安装etcd**: 版本 3.5 或更高（如果本地运行）

## 部署方式选择

本系统支持两种部署方式：

- **Docker Compose 部署（推荐）**：最简单快速，适合快速体验和开发环境
- **本地编译部署**：适合生产环境或需要自定义配置的场景

---

## 方式一：Docker Compose 部署（推荐）

### 步骤 1: 启动所有服务

使用 Docker Compose 一键启动整个系统（包括 etcd、调度器和多个工作节点）：

```bash
cd distributed-task-scheduler
docker-compose up -d
```

这将启动以下服务：
- etcd 服务（端口 2379）
- 1 个调度器实例
- 3 个工作节点实例

### 步骤 2: 查看服务状态

```bash
docker-compose ps
```

### 步骤 3: 查看日志

```bash
# 查看所有服务日志
docker-compose logs -f

# 查看特定服务日志
docker-compose logs -f scheduler
docker-compose logs -f worker-1
docker-compose logs -f etcd
```

### 步骤 4: 提交任务

```bash
# 提交单个任务
docker-compose run --rm client \
  --etcd-endpoints=etcd:2379 \
  --queue=default \
  --type=echo \
  --name=test-task \
  --priority=1 \
  --count=1

# 提交多个任务
docker-compose run --rm client \
  --etcd-endpoints=etcd:2379 \
  --queue=default \
  --type=echo \
  --name=batch-task \
  --priority=2 \
  --count=100
```

### 步骤 5: 停止服务

```bash
# 停止所有服务
docker-compose down

# 停止并清理数据
docker-compose down -v
```

### 扩展工作节点

如果需要更多工作节点，可以修改 `docker-compose.yml` 添加更多 worker 服务，或使用：

```bash
docker-compose up -d --scale worker-1=3
```

### 自定义配置

可以通过修改 `docker-compose.yml` 中的 `command` 参数来调整各服务的配置，例如：
- 修改 `--max-concurrent` 调整工作节点并发数
- 修改 `--strategy` 调整负载均衡策略
- 修改 `--log-level` 调整日志级别

---

## 方式二：本地编译部署

### 步骤 1: 启动etcd

**macOS:**
```bash
# 使用Homebrew安装
brew install etcd

# 启动etcd
etcd
```

**Linux:**
```bash
# 下载etcd (以v3.5.10为例)
ETCD_VER=v3.5.10
curl -L https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o etcd-${ETCD_VER}-linux-amd64.tar.gz
tar xzvf etcd-${ETCD_VER}-linux-amd64.tar.gz
cd etcd-${ETCD_VER}-linux-amd64

# 启动etcd
./etcd
```

etcd默认监听在 `localhost:2379`。

### 步骤 2: 编译项目

```bash
cd distributed-task-scheduler

# 下载依赖
go mod download

# 编译（或使用Makefile）
make build
# 或
go build -o bin/scheduler ./cmd/scheduler
go build -o bin/worker ./cmd/worker
go build -o bin/client ./cmd/client
```

### 步骤 3: 启动调度器

在一个终端窗口中：

```bash
./bin/scheduler \
  --node-id=scheduler-1 \
  --etcd-endpoints=localhost:2379 \
  --queue=default \
  --strategy=round_robin \
  --log-level=info
```

或使用Makefile：

```bash
make run-scheduler
```

你应该看到类似以下的输出：

```
{"level":"info","msg":"starting scheduler","node_id":"scheduler-1",...}
{"level":"info","msg":"scheduler started","node_id":"scheduler-1"}
```

### 步骤 4: 启动工作节点

在新的终端窗口中：

```bash
./bin/worker \
  --node-id=worker-1 \
  --etcd-endpoints=localhost:2379 \
  --queue=default \
  --max-concurrent=10 \
  --log-level=info
```

或使用Makefile：

```bash
make run-worker
```

可以启动多个工作节点。在另一个终端窗口启动第二个工作节点：

```bash
./bin/worker --node-id=worker-2 --max-concurrent=10
```

### 步骤 5: 提交任务

在第三个终端窗口中：

```bash
./bin/client \
  --etcd-endpoints=localhost:2379 \
  --queue=default \
  --type=echo \
  --name=test-task \
  --priority=1 \
  --count=10
```

或使用Makefile：

```bash
make run-client
```

### 步骤 6: 观察结果

### 在调度器终端

你应该看到任务分配日志：

```
{"level":"info","msg":"task assigned","task_id":"...","worker_id":"worker-1"}
{"level":"info","msg":"scheduler stats","total_tasks":10,"queued_tasks":0,...}
```

### 在工作节点终端

你应该看到任务执行日志：

```
{"level":"info","msg":"task execution started","task_id":"...","type":"echo"}
{"level":"info","msg":"task execution completed","task_id":"...","duration":"1.01s"}
{"level":"info","msg":"worker stats","total_tasks":10,"completed_tasks":10,...}
```

## 测试不同场景

### 1. 测试优先级

提交不同优先级的任务：

```bash
# 低优先级
./bin/client --type=echo --name=low --priority=0 --count=5

# 高优先级
./bin/client --type=echo --name=high --priority=2 --count=5
```

观察任务执行顺序，高优先级任务应该先执行。

### 2. 测试重试机制

提交unreliable类型的任务（前两次失败，第三次成功）：

```bash
./bin/client --type=unreliable --name=retry-test --count=1
```

观察工作节点日志，应该看到任务重试。

### 3. 测试故障恢复

1. 启动一个工作节点
2. 提交一些任务
3. 停止工作节点（Ctrl+C）
4. 观察调度器日志，应该检测到节点下线并重新分配任务

### 4. 测试负载均衡

启动多个工作节点：

```bash
# 终端1
./bin/worker --node-id=worker-1 --max-concurrent=5

# 终端2
./bin/worker --node-id=worker-2 --max-concurrent=5

# 终端3
./bin/worker --node-id=worker-3 --max-concurrent=5
```

提交大量任务：

```bash
./bin/client --type=echo --count=100
```

观察任务被分配到不同的工作节点。

## 运行测试

### 单元测试

```bash
go test ./...
```

### 基准测试

```bash
cd tests
go test -bench=. -benchmem
```

### 负载测试

```bash
cd tests
go test -run TestConcurrentEnqueue -v
go test -run TestConcurrentDequeue -v
```

## 使用不同的负载均衡策略

调度器支持多种负载均衡策略：

```bash
# 轮询（默认）
./bin/scheduler --strategy=round_robin

# 随机
./bin/scheduler --strategy=random

# 最少负载
./bin/scheduler --strategy=least_load

# 加权
./bin/scheduler --strategy=weighted
```

## 常见问题

### etcd连接失败

确保etcd正在运行：

```bash
# 检查etcd是否运行
curl http://localhost:2379/version
```

### 任务没有执行

1. 检查工作节点是否已启动
2. 检查工作节点是否注册了对应的任务执行器
3. 查看日志中的错误信息

### 任务重复执行

这是正常的，因为多个调度器可以同时工作。系统通过etcd的事务机制避免任务重复分配。

## 下一步

- 阅读 [架构设计文档](ARCHITECTURE.md) 了解系统设计
- 阅读 [技术总结文档](TECHNICAL_SUMMARY.md) 了解实现细节
- 查看 [性能测试报告](PERFORMANCE.md) 了解性能指标
- 自定义任务执行器以满足你的需求

## 获取帮助

如果遇到问题，请：

1. 查看日志输出
2. 检查etcd状态
3. 查看文档
4. 提交Issue

