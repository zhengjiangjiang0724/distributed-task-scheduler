# 多阶段构建 Dockerfile
# 第一阶段：构建阶段
FROM golang:1.21-alpine AS builder

# 安装必要的构建工具
RUN apk add --no-cache git make

# 设置工作目录
WORKDIR /build

# 复制 go mod 文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建所有组件
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o worker ./cmd/worker && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o client ./cmd/client

# 第二阶段：运行阶段
FROM alpine:latest

# 安装 ca-certificates 用于 HTTPS 连接
RUN apk --no-cache add ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /build/scheduler /app/scheduler
COPY --from=builder /build/worker /app/worker
COPY --from=builder /build/client /app/client

# 设置可执行权限
RUN chmod +x /app/scheduler /app/worker /app/client

# 默认运行 scheduler（可以通过 docker-compose 覆盖）
CMD ["/app/scheduler"]

