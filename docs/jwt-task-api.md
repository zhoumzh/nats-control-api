# JWT 任务管理 API

本文档介绍 JWT 任务管理相关的 API 端点，用于监控和管理 NATS JWT 同步任务。

## API 端点

### 1. 获取任务统计信息

```http
GET /api/v1/jwt-tasks/stats
```

获取所有 JWT 任务的统计信息。

**响应示例:**
```json
{
  "total": 150,
  "pending": 2,
  "processing": 1,
  "completed": 140,
  "failed": 5,
  "retrying": 2
}
```

### 2. 列出所有任务

```http
GET /api/v1/jwt-tasks?status={status}&limit={limit}&offset={offset}
```

**查询参数:**
- `status` (可选): 任务状态过滤 (`pending`, `processing`, `completed`, `failed`, `retrying`)
- `limit` (可选): 返回任务数量 (默认 20, 最大 100)
- `offset` (可选): 跳过任务数量 (默认 0)

**示例请求:**
```bash
# 获取所有失败的任务
curl "http://localhost:8080/api/v1/jwt-tasks?status=failed&limit=10"

# 获取最新的 20 个任务
curl "http://localhost:8080/api/v1/jwt-tasks?limit=20"

# 分页获取任务
curl "http://localhost:8080/api/v1/jwt-tasks?limit=10&offset=20"
```

### 3. 列出失败的任务

```http
GET /api/v1/jwt-tasks/failed?limit={limit}
```

专门获取失败状态的任务列表。

**示例请求:**
```bash
curl "http://localhost:8080/api/v1/jwt-tasks/failed?limit=5"
```

### 4. 获取特定任务详情

```http
GET /api/v1/jwt-tasks/{id}
```

**示例请求:**
```bash
curl "http://localhost:8080/api/v1/jwt-tasks/5ff063f1-bc47-4e6f-9c72-80be2f359179"
```

**响应示例:**
```json
{
  "id": "5ff063f1-bc47-4e6f-9c72-80be2f359179",
  "type": "jwt_delete",
  "status": "failed",
  "entity_type": "account",
  "entity_id": "f5fc31a5-2f08-4c21-8c46-84c22ababa22",
  "operation": "disable",
  "error": "failed to delete account JWT: nats: no responders available for request",
  "retries": 3,
  "max_retries": 3,
  "created_at": "2025-08-15T14:22:27.390584+08:00",
  "updated_at": "2025-08-15T14:22:38.088448+08:00"
}
```

### 5. 重试失败的任务

```http
POST /api/v1/jwt-tasks/{id}/retry
```

将失败的任务重置为待处理状态，系统会自动重新处理。

**示例请求:**
```bash
curl -X POST "http://localhost:8080/api/v1/jwt-tasks/5ff063f1-bc47-4e6f-9c72-80be2f359179/retry"
```

**成功响应:**
```json
{
  "message": "Task has been reset to pending status and will be retried automatically",
  "task_id": "5ff063f1-bc47-4e6f-9c72-80be2f359179"
}
```

**错误响应:**
```json
{
  "error": "task 5ff063f1-bc47-4e6f-9c72-80be2f359179 is not in failed status (current: completed)"
}
```

## 任务状态说明

- **pending**: 等待处理
- **processing**: 正在处理中
- **completed**: 已完成
- **failed**: 处理失败
- **retrying**: 重试中

## 使用场景

### 1. 监控任务健康状态
```bash
# 定期检查任务统计
curl "http://localhost:8080/api/v1/jwt-tasks/stats"
```

### 2. 处理失败任务
```bash
# 1. 查看失败任务
curl "http://localhost:8080/api/v1/jwt-tasks/failed"

# 2. 查看具体错误
curl "http://localhost:8080/api/v1/jwt-tasks/{task_id}"

# 3. 修复问题后重试
curl -X POST "http://localhost:8080/api/v1/jwt-tasks/{task_id}/retry"
```

### 3. 调试任务问题
```bash
# 查看特定账户的相关任务
curl "http://localhost:8080/api/v1/jwt-tasks?limit=50" | jq '.[] | select(.entity_id=="account_id")'

# 查看最近的处理中任务
curl "http://localhost:8080/api/v1/jwt-tasks?status=processing"
```

## 注意事项

1. **重试限制**: 只有状态为 `failed` 的任务才能重试
2. **自动处理**: 重试后的任务会在 5 秒内被自动处理
3. **权限控制**: 当前所有端点都是公开的，生产环境建议添加认证
4. **任务清理**: 建议定期清理过期的已完成任务以保持数据库性能