package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Port int    `yaml:"port" envconfig:"SERVER_PORT"`
		Host string `yaml:"host" envconfig:"SERVER_HOST"`
	} `yaml:"server"`

	NATS struct {
		URL              string `yaml:"url" envconfig:"NATS_URL"`
		OperatorNKey     string `yaml:"operator_nkey" envconfig:"NATS_OPERATOR_NKEY"`
		SystemAccountJWT string `yaml:"system_account_jwt" envconfig:"NATS_SYSTEM_ACCOUNT_JWT"`
		SystemUserJWT    string `yaml:"system_user_jwt" envconfig:"NATS_SYSTEM_USER_JWT"`
		SystemUserKey    string `yaml:"system_user_key" envconfig:"NATS_SYSTEM_USER_KEY"`
	} `yaml:"nats"`

	Database struct {
		Driver string `yaml:"driver" envconfig:"DB_DRIVER"`
		DSN    string `yaml:"dsn" envconfig:"DB_DSN"`
	} `yaml:"database"`

	Log struct {
		Level  string `yaml:"level" envconfig:"LOG_LEVEL"`
		Format string `yaml:"format" envconfig:"LOG_FORMAT"`
	} `yaml:"log"`

	Monitor struct {
		HealthCheck struct {
			Enable   bool `yaml:"enable" envconfig:"MONITOR_HEALTH_CHECK_ENABLE"`
			Interval int  `yaml:"interval" envconfig:"MONITOR_HEALTH_CHECK_INTERVAL"` // 健康检查间隔（秒）
		} `yaml:"healthCheck"`
		ServerList struct {
			Timeout int `yaml:"timeout" envconfig:"MONITOR_SERVER_LIST_TIMEOUT"` // 获取服务器列表超时时间（秒）
		} `yaml:"serverList"`
	} `yaml:"monitor"`
}

func Load(configPath string) (*Config, error) {
	cfg := &Config{}

	// 先设置默认值
	setDefaults(cfg)

	// 然后从文件加载配置（会覆盖默认值）
	if configPath != "" {
		if err := loadFromFile(cfg, configPath); err != nil {
			return nil, fmt.Errorf("从文件加载配置失败: %w", err)
		}
	}

	// 最后处理环境变量（会覆盖文件配置）
	if err := envconfig.Process("", cfg); err != nil {
		return nil, fmt.Errorf("处理环境变量失败: %w", err)
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return cfg, nil
}

func setDefaults(cfg *Config) {
	cfg.Server.Port = 8080
	cfg.Server.Host = "0.0.0.0"
	cfg.NATS.URL = "nats://localhost:4222"
	cfg.Database.Driver = "sqlite3"
	cfg.Database.DSN = "./rbac.db"
	cfg.Log.Level = "info"
	cfg.Log.Format = "json"
	cfg.Monitor.HealthCheck.Enable = true // 默认开启集群健康检查
	cfg.Monitor.HealthCheck.Interval = 30 // 默认30秒检查一次
	cfg.Monitor.ServerList.Timeout = 5 // 默认5秒超时
}

func loadFromFile(cfg *Config, path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("获取绝对路径失败: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}

	return nil
}

func validate(cfg *Config) error {
	if cfg.NATS.OperatorNKey == "" {
		return fmt.Errorf("NATS操作员密钥是必需的")
	}

	return nil
}
