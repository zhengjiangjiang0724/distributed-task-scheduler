.PHONY: build clean test bench run-scheduler run-worker run-client help

# 构建目录
BUILD_DIR := bin

# Go参数
GO := go
GO_BUILD := $(GO) build
GO_TEST := $(GO) test
GO_MOD := $(GO) mod

# 编译所有二进制文件
build:
	@echo "Building binaries..."
	@mkdir -p $(BUILD_DIR)
	$(GO_BUILD) -o $(BUILD_DIR)/scheduler ./cmd/scheduler
	$(GO_BUILD) -o $(BUILD_DIR)/worker ./cmd/worker
	$(GO_BUILD) -o $(BUILD_DIR)/client ./cmd/client
	@echo "Build complete!"

# 清理构建文件
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete!"

# 下载依赖
deps:
	@echo "Downloading dependencies..."
	$(GO_MOD) download
	$(GO_MOD) tidy
	@echo "Dependencies downloaded!"

# 运行测试
test:
	@echo "Running tests..."
	$(GO_TEST) ./...

# 运行基准测试
bench:
	@echo "Running benchmarks..."
	cd tests && $(GO_TEST) -bench=. -benchmem

# 运行负载测试
test-load:
	@echo "Running load tests..."
	cd tests && $(GO_TEST) -run TestConcurrent -v

# 运行调度器
run-scheduler: build
	@echo "Starting scheduler..."
	./$(BUILD_DIR)/scheduler \
		--node-id=scheduler-1 \
		--etcd-endpoints=localhost:2379 \
		--queue=default \
		--strategy=round_robin \
		--log-level=info

# 运行工作节点
run-worker: build
	@echo "Starting worker..."
	./$(BUILD_DIR)/worker \
		--node-id=worker-1 \
		--etcd-endpoints=localhost:2379 \
		--queue=default \
		--max-concurrent=10 \
		--log-level=info

# 运行客户端
run-client: build
	@echo "Submitting tasks..."
	./$(BUILD_DIR)/client \
		--etcd-endpoints=localhost:2379 \
		--queue=default \
		--type=echo \
		--name=test-task \
		--priority=1 \
		--count=10

# 格式化代码
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

# 代码检查
lint:
	@echo "Linting code..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, skipping..."; \
	fi

# 安装所有工具
install-tools:
	@echo "Installing tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# 显示帮助信息
help:
	@echo "Available targets:"
	@echo "  build          - Build all binaries"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download dependencies"
	@echo "  test           - Run tests"
	@echo "  bench          - Run benchmark tests"
	@echo "  test-load      - Run load tests"
	@echo "  run-scheduler  - Run scheduler"
	@echo "  run-worker     - Run worker"
	@echo "  run-client     - Run client to submit tasks"
	@echo "  fmt            - Format code"
	@echo "  lint           - Lint code"
	@echo "  install-tools  - Install development tools"
	@echo "  help           - Show this help message"

