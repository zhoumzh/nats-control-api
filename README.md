# LEB Control API

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-blue.svg)](https://golang.org/)
[![NATS](https://img.shields.io/badge/NATS-2.x-brightgreen.svg)](https://nats.io/)

A comprehensive NATS control plane management system with JWT authentication and multi-cluster support. This is the backend API service for managing NATS clusters, accounts, users, and JetStream configurations in a distributed cloud environment.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [API Documentation](#api-endpoints)
- [Configuration](#configuration-reference)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Architecture
Cluster 集群

├─ System Account（系统账号）

│  ├─ User 1（普通用户）

│  ├─ User 2（普通用户）

│  └─ Admin User（管理员用户）

├─ Account A

│  ├─ JetStream 1

│  │  ├─ Consumer 1

│  │  └─ Consumer 2

│  └─ JetStream 2

│     ├─ Consumer 3

│     └─ Consumer 4

└─ Account B

└─ JetStream 3

├─ Consumer 5

└─ Consumer 6


Cluster 集群 2

├─ System Account（系统账号）

│  ├─ User 3（普通用户）

│  └─ Admin User（管理员用户）

└─ Account C

└─ JetStream 4

├─ Consumer 7

└─ Consumer 8

## Features

- ✅ **Account Management**: Create, update, enable/disable NATS accounts with comprehensive permission controls
- ✅ **User Management**: Create, update, enable/disable users within accounts with detailed permission declarations
- ✅ **Cluster Management**: Multi-cluster management with real-time monitoring and health checks
- ✅ **JetStream Support**: Stream and consumer management across clusters with advanced retry capabilities
- ✅ **JWT Integration**: Automatic JWT generation, validation, and multi-cluster synchronization
- ✅ **Async Processing**: Background JWT task processing with retry mechanism and error handling
- ✅ **RESTful API**: Complete REST API with comprehensive Swagger documentation
- ✅ **Real-time Monitoring**: Cluster status monitoring and performance metrics
- ✅ **Configuration**: Flexible configuration supporting YAML files and environment variables
- 🆕 **JetStream Retry**: Advanced retry mechanism for failed JetStream creations with persistent configuration storage

## Architecture

The system follows NATS JWT authentication hierarchy:
```
Operator (Root) -> Account -> User
```

### Key Components

1. **Models**: Account, User, Cluster, JetStream, and JWTTask entities with comprehensive field definitions
2. **Configuration**: Support for YAML files and environment variables with security considerations
3. **Database**: Repository pattern with SQLite implementation for persistence
4. **JWT Manager**: NATS JWT creation, validation, and nats-resolver integration
5. **Services**: Business logic for account/user management, cluster monitoring, and JWT task processing
6. **API Handlers**: RESTful endpoints with comprehensive Swagger documentation
7. **Async Processing**: Background worker for JWT operations with retry mechanism
8. **Cluster Monitoring**: Real-time cluster health monitoring and performance tracking

## Quick Start

### 1. Configuration

#### Option A: Configuration File
```bash
cp config.yaml config.local.yaml
# Edit config.local.yaml with your NATS settings
```

#### Option B: Environment Variables
```bash
export NATS_OPERATOR_NKEY="your-operator-nkey"
export DATABASE_DSN="./control.db"
export SERVER_PORT="8080"
export LOG_LEVEL="info"
```

### 2. Build and Run

```bash
# Install dependencies
go mod tidy

# Generate Swagger docs and build
./build.sh

# Run the application
./bin/leb-control-api -config config.yaml
# or
go run cmd/main.go -config config.yaml
```

### 3. Access API

- **API Base URL**: `http://localhost:8080/api/v1`
- **Swagger UI**: `http://localhost:8080/swagger/index.html`
- **Health Check**: `http://localhost:8080/api/v1/health`

## API Endpoints

### Cluster Management
- `POST /api/v1/clusters` - Create cluster
- `GET /api/v1/clusters` - List clusters
- `GET /api/v1/clusters/{id}` - Get cluster details
- `PUT /api/v1/clusters/{id}` - Update cluster
- `DELETE /api/v1/clusters/{id}` - Delete cluster
- `GET /api/v1/clusters/{id}/status` - Get cluster status

### Account Management
- `POST /api/v1/accounts` - Create account
- `GET /api/v1/accounts` - List accounts
- `GET /api/v1/accounts/{id}` - Get account
- `PUT /api/v1/accounts/{id}` - Update account
- `POST /api/v1/accounts/{id}/disable` - Disable account
- `POST /api/v1/accounts/{id}/enable` - Enable account

### User Management
- `POST /api/v1/accounts/{accountId}/users` - Create user
- `GET /api/v1/accounts/{accountId}/users` - List users
- `GET /api/v1/users/{id}` - Get user
- `PUT /api/v1/users/{id}` - Update user
- `POST /api/v1/users/{id}/disable` - Disable user
- `POST /api/v1/users/{id}/enable` - Enable user

### JetStream Management
- `POST /api/v1/jetstreams` - Create JetStream
- `GET /api/v1/jetstreams` - List JetStreams
- `GET /api/v1/jetstreams/{id}` - Get JetStream details
- `PUT /api/v1/jetstreams/{id}` - Update JetStream
- `DELETE /api/v1/jetstreams/{id}` - Delete JetStream
- `POST /api/v1/jetstreams/{id}/sync` - Sync JetStream status
- 🆕 `POST /api/v1/jetstreams/{id}/retry` - Retry failed JetStream creation

### JWT Task Management
- `GET /api/v1/jwt-tasks` - List JWT tasks
- `GET /api/v1/jwt-tasks/{id}` - Get task details
- `POST /api/v1/jwt-tasks/{id}/retry` - Retry failed task

## Configuration Reference

### Server Configuration
```yaml
server:
  port: 8080
  host: "0.0.0.0"
```

### NATS Configuration
```yaml
nats:
  operator_nkey: ""      # NATS_OPERATOR_NKEY - NKey for the NATS operator
```

### Database Configuration
```yaml
database:
  driver: "sqlite3"
  dsn: "./control.db"    # SQLite database file path
```

### Logging Configuration
```yaml
log:
  level: "info"    # debug, info, warn, error
  format: "json"   # json, text
```

## Security Considerations

- **Production**: Use environment variables for sensitive configuration like operator NKeys
- **Testing**: Use separate configuration files with test values
- **NKeys**: All private keys stored as NATS NKey format for enhanced security
- **JWT Validation**: Automatic JWT validation and comprehensive error handling
- **CORS**: Cross-origin resource sharing configured for frontend integration

## Development

### Project Structure
```
├── cmd/                    # Application entry point and CLI tools
│   ├── main.go            # Main application entry
│   ├── seed/              # Database seeding utilities
│   └── test-*/            # Testing utilities
├── internal/
│   ├── api/               # HTTP handlers and routes
│   ├── config/            # Configuration management
│   ├── db/                # Database repository layer
│   ├── jwt/               # NATS JWT management
│   └── service/           # Business logic layer
├── pkg/
│   ├── models/            # Data models and entities
│   └── utils/             # Utility functions
├── docs/                  # Generated Swagger documentation
├── scripts/               # Deployment and utility scripts
└── config.yaml           # Configuration file
```

### Database Operations

The system uses SQLite for data persistence with GORM as the ORM layer:

1. **Models**: Defined in `pkg/models/` with comprehensive entity relationships
2. **Repository Pattern**: Database operations abstracted through repository interfaces
3. **Migrations**: Automatic database schema management on startup
4. **Transactions**: Proper transaction handling for complex operations

### Cluster Monitoring

Real-time cluster monitoring system:

1. **Health Checks**: Periodic connectivity and performance monitoring
2. **Status Tracking**: Real-time cluster status updates and alerts
3. **Performance Metrics**: Collection and storage of cluster performance data
4. **Auto-Discovery**: Automatic detection of cluster topology changes

### JWT Task Processing

The system uses async JWT task processing:

1. **Create/Update Operations**: Generate and push JWT to NATS
2. **Disable Operations**: Delete JWT from NATS server
3. **Retry Mechanism**: Automatic retry with configurable limits
4. **Error Handling**: Comprehensive error logging and status tracking

### 🆕 JetStream Retry Mechanism

Advanced retry functionality for JetStream creation failures:

**Features:**
- **Persistent Configuration**: Failed JetStream configurations are preserved in database for future retry
- **Intelligent Status Management**: Differentiate between database records and actual NATS streams
- **User-Friendly Interface**: Retry buttons available in JetStream management UI for error-status streams
- **Detailed Error Reporting**: Comprehensive Chinese error messages for different failure scenarios
- **Data Consistency**: Ensures database and NATS cluster state synchronization

**Workflow:**
1. **Creation Attempt**: JetStream creation attempted on NATS cluster
2. **Failure Handling**: If NATS creation fails, database record preserved with 'error' status
3. **Retry Capability**: Users can retry failed creations after fixing configuration issues
4. **Success Recovery**: Successful retry updates status to 'active' and syncs with cluster

**Backend Implementation:**
- `RetryJetStreamCreation` service method with comprehensive validation
- Status verification (only 'error' status streams can be retried)
- User and cluster validation before retry attempt
- Detailed logging and error categorization

**Frontend Implementation:**
- Retry buttons displayed for error-status JetStreams in management interface
- Loading states and error handling for retry operations
- Automatic data refresh after successful retry
- Integration with existing JetStream store and API layer

## Tech Stack

- **Language**: Go 1.23+
- **Framework**: Gin (HTTP router and middleware)
- **Database**: SQLite with GORM ORM
- **NATS**: Official Go client with JWT/NKeys support
- **Documentation**: Swagger/OpenAPI 3.0 with auto-generated docs
- **Configuration**: YAML files + Environment variables
- **Logging**: Structured logging support
- **Build Tools**: Native Go build system with shell scripts

## Contributing

We welcome contributions from the community! Here's how you can help:

### Getting Started

1. **Fork the repository** and clone it locally
2. **Create a new branch** for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** and ensure code quality:
   ```bash
   go fmt ./...
   go vet ./...
   go test ./...
   ```
4. **Commit your changes** with clear commit messages
5. **Push to your fork** and submit a pull request

### Contribution Guidelines

- Follow Go best practices and idiomatic patterns
- Write clear commit messages describing what and why
- Add tests for new features and bug fixes
- Update documentation for API changes
- Ensure all tests pass before submitting PR
- Keep PRs focused on a single feature or fix

### Code Style

- Use `gofmt` for code formatting
- Follow [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Write meaningful comments for exported functions
- Keep functions small and focused

### Reporting Issues

Found a bug or have a feature request? Please [open an issue](../../issues) with:
- Clear description of the problem or suggestion
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Your environment details (Go version, OS, etc.)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2025 Zhou Mingzhu

## Acknowledgments

- Built with [NATS](https://nats.io/) - The Cloud Native Messaging System
- Powered by [Gin](https://gin-gonic.com/) - HTTP web framework
- Documentation generated with [Swag](https://github.com/swaggo/swag)

## Support

For questions, issues, or contributions, please visit the [GitHub repository](../../).
