#!/bin/bash

# 数据预置脚本 - 预置系统账户和用户到数据库

set -e

echo "开始预置系统账户和用户数据..."

# 构建预置工具
echo "构建预置工具..."
go build -o bin/seed cmd/seed/main.go

# 运行预置脚本（使用测试配置文件）
echo "运行数据预置..."
./bin/seed --config config-test.yaml

echo ""
echo "数据预置完成！"
echo "现在可以在集群管理界面中使用以下系统账户创建集群："
echo "1. AliSYS - 阿里云集群系统账户"
echo "2. BaiduSYS - 百度云集群系统账户"
echo "3. LocalSYS - 本地集群系统账户"
echo ""
echo "每个账户都包含对应的system用户，可在创建集群时选择使用。"