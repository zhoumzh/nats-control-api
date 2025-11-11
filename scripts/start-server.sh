#!/bin/bash

# 启动后端API服务器脚本

set -e

echo "启动leb-control-api服务器..."

# 构建后端应用
echo "构建后端应用..."
go build -o bin/leb-control-api cmd/main.go

# 启动服务器
echo "启动服务器（使用test配置）..."
./bin/leb-control-api --config config-test.yaml

echo "服务器已启动"