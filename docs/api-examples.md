# NATS RBAC API 使用示例

本文档展示了如何使用扩展后的 NATS RBAC API 来配置账户和用户的高级权限。

## Account 管理示例

### 创建带 JetStream 的账户

```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "prod-account",
    "description": "生产环境账户",
    "limits": {
      "max_connections": 1000,
      "max_payload": 1048576,
      "jetstream_limits": {
        "enabled": true,
        "memory_storage": 1073741824,
        "disk_storage": 10737418240,
        "streams": 100,
        "consumers": 500,
        "max_ack_pending": 1000
      },
      "default_permissions": {
        "publish": {
          "allow": ["prod.>"],
          "deny": ["prod.admin.>"]
        },
        "subscribe": {
          "allow": ["prod.>"],
          "deny": ["prod.secret.>"]
        }
      }
    }
  }'
```

### 创建禁用 JetStream 的账户

```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "basic-account",
    "description": "基础消息账户（无JetStream）",
    "limits": {
      "max_connections": 100,
      "max_payload": 65536,
      "jetstream_limits": {
        "enabled": false
      }
    }
  }'
```

### 创建带 Import/Export 的账户

```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "name": "service-account",
    "description": "微服务账户",
    "limits": {
      "exports": [
        {
          "name": "user-service",
          "subject": "user.>",
          "type": "service",
          "token_req": true,
          "info": {
            "description": "用户管理服务",
            "info_url": "https://docs.example.com/user-service"
          }
        }
      ],
      "imports": [
        {
          "name": "auth-service",
          "subject": "auth.>",
          "account": "AXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
          "type": "service",
          "to": "internal.auth.>"
        }
      ]
    }
  }'
```

## User 管理示例

### 创建带 JetStream 权限的用户

```bash
curl -X POST http://localhost:8080/api/v1/accounts/{account_id}/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "stream-user",
    "description": "JetStream 流处理用户",
    "permissions": {
      "publish": {
        "allow": ["events.>"],
        "deny": ["events.internal.>"]
      },
      "subscribe": {
        "allow": ["events.>", "results.>"]
      },
      "jetstream": {
        "publish": {
          "allow": ["EVENTS.>"],
          "deny": ["EVENTS.SYSTEM.>"]
        },
        "subscribe": {
          "allow": ["EVENTS.>", "RESULTS.>"]
        }
      }
    },
    "limits": {
      "max_payload": 524288,
      "jetstream_limits": {
        "memory_storage": 104857600,
        "disk_storage": 1073741824,
        "streams": 10,
        "consumers": 50,
        "max_ack_pending": 100
      }
    }
  }'
```

### 创建带访问控制的用户

```bash
curl -X POST http://localhost:8080/api/v1/accounts/{account_id}/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "restricted-user",
    "description": "受限访问用户",
    "permissions": {
      "publish": {
        "allow": ["data.>"]
      },
      "subscribe": {
        "allow": ["notifications.>"]
      }
    },
    "limits": {
      "access_controls": {
        "source_ips": ["192.168.1.0/24", "10.0.0.0/8"],
        "time_restrictions": {
          "start": "2024-01-01T00:00:00Z",
          "end": "2024-12-31T23:59:59Z",
          "timezone": "Asia/Shanghai",
          "days_of_week": [1, 2, 3, 4, 5],
          "hours_of_day": [9, 10, 11, 12, 13, 14, 15, 16, 17]
        }
      },
      "connection_types": ["websocket", "standard"]
    }
  }'
```

### 创建临时用户（无访问限制）

```bash
curl -X POST http://localhost:8080/api/v1/accounts/{account_id}/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "temp-user",
    "description": "临时用户（默认无访问控制）",
    "permissions": {
      "publish": {
        "allow": ["temp.>"]
      },
      "subscribe": {
        "allow": ["temp.>"]
      }
    }
  }'
```

## 权限配置说明

### Account 级配置

1. **JetStream 配置** (`jetstream_limits`):
   - `enabled`: **必需字段** - 是否为账户启用 JetStream（true/false）
   - `memory_storage`: 内存存储限制（字节）
   - `disk_storage`: 磁盘存储限制（字节）
   - `streams`: 最大流数量
   - `consumers`: 最大消费者数量
   - `max_ack_pending`: 最大待确认消息数

   **注意**: 只有当 `enabled: true` 时，其他 JetStream 限制才会生效。如果 `enabled: false` 或未设置，账户将无法使用 JetStream 功能。

2. **Import/Export 配置**:
   - 支持服务和流的导入导出
   - 可配置访问令牌要求
   - 支持主题映射

3. **默认权限** (`default_permissions`):
   - 为账户内所有用户设置默认权限
   - 可以被用户级权限覆盖

### User 级配置

1. **访问控制** (`access_controls`):
   - **IP 限制**: CIDR 格式的 IP 地址白名单，空数组表示不限制
   - **时间限制**: 可选的时间窗口控制，所有字段为空表示不限制
     - `start`/`end`: RFC3339 格式的时间范围
     - `timezone`: 时区设置，默认 UTC
     - `days_of_week`: 允许的星期几（0=周日，6=周六）
     - `hours_of_day`: 允许的小时（0-23）

2. **JetStream 权限**:
   - 独立于常规消息的流权限控制
   - 支持发布和订阅权限分离

3. **连接控制**:
   - `connection_types`: 允许的连接类型

### 默认行为

- **IP 限制**: 空数组或未设置时，允许所有 IP 访问
- **时间限制**: 未设置或字段为空时，允许全时段访问
- **连接类型**: 未设置时，允许所有连接类型

## 查询和更新

使用相同的 JSON 结构可以更新现有的账户和用户配置：

```bash
# 更新账户
curl -X PUT http://localhost:8080/api/v1/accounts/{id} \
  -H "Content-Type: application/json" \
  -d '{...}'

# 更新用户
curl -X PUT http://localhost:8080/api/v1/users/{id} \
  -H "Content-Type: application/json" \
  -d '{...}'
```

所有配置更改会自动触发 JWT 更新并推送到 NATS 服务器。