package main

import (
	"flag"
	"fmt"
	"nats-control-api/internal/nats"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	_ "nats-control-api/docs" // Import generated docs
	"nats-control-api/internal/api"
	"nats-control-api/internal/config"
	"nats-control-api/internal/db"
	"nats-control-api/internal/jwt"
	"nats-control-api/internal/service"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

//	@title			NATS Control API
//	@version		1.0
//	@description	API for managing NATS control plane with multi-cluster support
//	@termsOfService	http://swagger.io/terms/

//	@contact.name	API Support
//	@contact.email	support@example.com

//	@license.name	MIT
//	@license.url	https://opensource.org/licenses/MIT

//	@host		localhost:8080
//	@BasePath	/api/v1

//	@schemes	http https

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("配置文件加载失败: %v", err)
	}

	log.Info("配置信息:", cfg)

	setupLogging(cfg)

	repo, err := db.NewRepository(cfg.Database.Driver, cfg.Database.DSN)
	if err != nil {
		log.Fatalf("数据库初始化失败: %v", err)
	}
	defer repo.Close()

	natsManager, err := jwt.NewNATSManager(
		cfg.NATS.OperatorNKey,
	)
	if err != nil {
		log.Fatalf("NATS管理器初始化失败: %v", err)
	}
	defer natsManager.Close()

	natsServer := nats.NewService()

	accountService := service.NewAccountService(repo, natsManager)
	userService := service.NewUserService(repo, natsManager)
	jwtTaskService := service.NewJWTTaskService(repo)
	clusterService := service.NewClusterService(repo, cfg)

	// Initialize cluster monitoring service
	clusterMonitorService := service.NewClusterMonitorService(repo, clusterService, cfg)

	jetStreamManagerService := service.NewJetStreamManagerService(repo, cfg, natsServer, clusterService, clusterMonitorService, natsManager)
	consumerService := service.NewConsumerManageService(repo, cfg, jetStreamManagerService, clusterService, natsManager)

	jwtService := service.NewJWTService(repo, natsManager, clusterService)
	jwtProcessor := service.NewJWTTaskProcessor(repo, jwtService)
	jwtProcessor.Start()
	defer jwtProcessor.Stop()

	// Start cluster monitoring only if enabled in config
	if cfg.Monitor.HealthCheck.Enable {
		log.Infof("集群健康检查已启用，检查间隔: %d秒", cfg.Monitor.HealthCheck.Interval)
		if err := clusterMonitorService.StartMonitoring(); err != nil {
			log.Fatalf("Failed to start cluster monitoring: %v", err)
		}
		defer clusterMonitorService.StopMonitoring()
	} else {
		log.Info("集群健康检查已禁用")
	}

	accountHandler := api.NewAccountHandler(accountService, jwtService, clusterService)
	userHandler := api.NewUserHandler(userService, jwtService)
	healthHandler := api.NewHealthHandler()
	jwtTaskHandler := api.NewJWTTaskHandler(jwtTaskService)
	clusterHandler := api.NewClusterHandler(clusterService, clusterMonitorService, jetStreamManagerService, cfg)
	jetStreamHandler := api.NewJetStreamHandler(jetStreamManagerService)
	consumerHandler := api.NewConsumerHandler(consumerService)

	router := setupRouter(cfg)
	api.SetupRoutes(router, accountHandler, userHandler, healthHandler, jwtTaskHandler, clusterHandler, jetStreamHandler, consumerHandler)

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	serverAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	log.Infof("服务器启动中，监听地址: %s", serverAddr)
	log.Infof("Swagger文档地址: http://localhost:%d/swagger/index.html", cfg.Server.Port)

	go func() {
		if err := router.Run(serverAddr); err != nil {
			log.Fatalf("服务器启动失败: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("正在关闭服务器...")
}

func setupLogging(cfg *config.Config) {
	// Company logging component is already configured by default
	// No additional setup needed as it handles levels and formatting internally
}

func setupRouter(cfg *config.Config) *gin.Engine {
	if cfg.Log.Level == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	// Add context-aware logging middleware
	router.Use(func(c *gin.Context) {
		ctx := c.Request.Context()
		start := time.Now()

		// Process request
		c.Next()

		// Log request with context
		latency := time.Since(start)
		log.WithContext(ctx).Infof("[%s] %s %s %d %v",
			c.Request.Method,
			c.Request.URL.Path,
			c.ClientIP(),
			c.Writer.Status(),
			latency,
		)
	})

	router.Use(gin.Recovery())

	router.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	return router
}
