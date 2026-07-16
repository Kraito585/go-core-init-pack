package app

import (
	"context"
	"fmt"
	coreconfig "go-core/core/config"

	"go-core/core/pkg/coretelemetry"

	"go-core/core/pkg/health"

	"go-core/migrations"
	"net"

	pkgkafka "go-core/core/pkg/kafka"
	"go-core/core/pkg/logger"

	"go-core/core/pkg/migrate"
	"go-core/core/pkg/postgres"

	coreredis "go-core/core/pkg/redis"

	"go-core/core/pkg/storage"

	"go-core/core/pkg/security"
	"go-core/internal/middleware"

	"go-core/internal/telemetry"

	appconfig "go-core/pkg/config"

	"go-core/core/pkg/response"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Microservice struct {
	FiberApp *fiber.App

	GRPCServer *grpc.Server

	CoreCfg *coreconfig.CoreConfig
	AppCfg  *appconfig.AppConfig

	Logger *slog.Logger

	S3Storage *storage.S3Client

	DBPool *pgxpool.Pool

	RedisClient     *coreredis.Wrapper
	Encryptor       *security.Encryptor
	MigrationsReady *atomic.Bool

	OutboxRelay *pkgkafka.OutboxRelay

	StateReplicator *pkgkafka.StateReplicator

	TaskProcessor *pkgkafka.TaskProcessor

	//core:jwt
	JWTManager *security.JWTManager
	//core:jwt:end

	TracerShutdown func(context.Context) error

	HealthCheckers []health.Checker
}

type Builder struct {
	app *Microservice
	err error
}

func NewBuilder(configPath string) *Builder {
	coreCfg, err := coreconfig.LoadCoreConfig(configPath)
	if err != nil {
		return &Builder{err: err}
	}

	appCfg, err := appconfig.LoadAppConfig(configPath)
	if err != nil {
		return &Builder{err: err}
	}

	isPrometheusEnabled := coreCfg.Prometheus.Enabled
	promServiceName := coreCfg.Prometheus.ServiceName

	coretelemetry.InitMetrics(promServiceName, isPrometheusEnabled)
	telemetry.InitAppMetrics(promServiceName, isPrometheusEnabled)

	fiberApp := fiber.New(fiber.Config{
		ErrorHandler: response.GlobalErrorHandler,
	})

	return &Builder{
		app: &Microservice{
			FiberApp:        fiberApp,
			CoreCfg:         coreCfg,
			AppCfg:          appCfg,
			MigrationsReady: &atomic.Bool{},
			HealthCheckers:  make([]health.Checker, 0),
		},
	}
}

func (b *Builder) WithLogger() *Builder {
	if b.err != nil {
		return b
	}

	logger.Init(b.app.AppCfg.App.IsProd)

	b.app.Logger = slog.Default()

	slog.Info("Логгер инициализирован", slog.Bool("is_prod", b.app.AppCfg.App.IsProd))

	return b
}

func (b *Builder) WithGRPCServer(interceptors ...grpc.UnaryServerInterceptor) *Builder {
	if b.err != nil {
		return b
	}

	var opts []grpc.ServerOption

	if len(interceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(interceptors...))
	}

	b.app.GRPCServer = grpc.NewServer(opts...)

	slog.Info("gRPC сервер успешно инициализирован")
	return b
}

func (b *Builder) WithS3Storage() *Builder {
	if b.err != nil {
		return b
	}

	cfg := b.app.CoreCfg.S3
	if !cfg.Enabled {
		return b
	}

	s3Client, err := storage.NewS3Client(
		context.Background(),
		cfg.Endpoint,
		cfg.Region,
		cfg.AccessKey,
		cfg.SecretKey,
		cfg.Bucket,
	)
	if err != nil {
		if !b.app.AppCfg.App.IsProd {
			b.err = err
			return b
		}
		slog.Warn("Ошибка инициализации S3 (Prod)", slog.Any("error", err))
	}

	b.app.S3Storage = s3Client
	slog.Info("S3 Хранилище успешно подключено")

	if s3Client != nil {
		s3Checker := health.NewS3Checker(s3Client)
		b.app.HealthCheckers = append(b.app.HealthCheckers, s3Checker)
	}

	return b
}

//core:jwt
func (b *Builder) WithJWT() *Builder {
	if b.err != nil {
		return b
	}

	cfg := b.app.CoreCfg.JWT
	if !cfg.Enabled {
		return b
	}

	jwtManager, err := security.NewJWTManager(cfg.PrivateKeyPath, cfg.PublicKeyPath, cfg.AccessTTL)
	if err != nil {
		if !b.app.AppCfg.App.IsProd {
			b.err = err
			return b
		}
		slog.Warn("Ошибка инициализации JWT Manager (Prod)", slog.Any("error", err))
	}

	b.app.JWTManager = jwtManager
	if jwtManager != nil {
		slog.Info("JWT Manager успешно инициализирован")
	}

	return b
}

//core:jwt:end

func (b *Builder) WithMigrations() *Builder {
	if b.err != nil {
		return b
	}

	if err := migrate.Run(b.app.CoreCfg, migrations.FS); err != nil {
		if !b.app.AppCfg.App.IsProd {
			b.err = fmt.Errorf("критическая ошибка миграций (Dev): %w", err)
			return b
		}

		slog.Warn("Критическая ошибка миграций (Prod). Трафик заблокирован.", slog.Any("error", err))
		b.app.MigrationsReady.Store(false)
	} else {
		b.app.MigrationsReady.Store(true)
	}

	migChecker := health.NewMigrationChecker(b.app.MigrationsReady)
	b.app.HealthCheckers = append(b.app.HealthCheckers, migChecker)

	return b
}

func (b *Builder) WithDatabases() *Builder {
	if b.err != nil {
		return b
	}

	var mainDBName string
	if len(b.app.CoreCfg.Postgres.Names) > 0 {
		mainDBName = b.app.CoreCfg.Postgres.Names[0]
	} else {
		mainDBName = b.app.CoreCfg.Postgres.Name
	}

	if mainDBName == "" {
		b.err = fmt.Errorf("критическая ошибка: не указано имя базы данных (ни db_name, ни db_names)")
		return b
	}

	pool, err := postgres.NewPool(context.Background(), b.app.CoreCfg.Postgres, mainDBName)
	if err != nil {
		b.err = err
		return b
	}

	b.app.DBPool = pool

	pgChecker := health.NewPostgresChecker(b.app.DBPool)
	b.app.HealthCheckers = append(b.app.HealthCheckers, pgChecker)

	return b
}

func (b *Builder) WithRedis() *Builder {
	if b.err != nil {
		return b
	}

	ctx := context.Background()
	client, err := coreredis.NewRedisManager(ctx, b.app.CoreCfg.Redis)
	if err != nil {
		b.err = fmt.Errorf("ошибка инициализации пула Redis: %w", err)
		return b
	}

	b.app.RedisClient = client

	redisChecker := health.NewRedisChecker(b.app.RedisClient)
	b.app.HealthCheckers = append(b.app.HealthCheckers, redisChecker)

	return b
}

func (b *Builder) WithEncryptor() *Builder {
	if b.err != nil {
		return b
	}

	enc, err := security.NewEncryptor(b.app.CoreCfg.Security.MasterKey)
	if err != nil {
		b.err = fmt.Errorf("ошибка инициализации шифровальщика: %w", err)
		return b
	}

	b.app.Encryptor = enc
	return b
}

func (b *Builder) WithOutboxRelay() *Builder {
	if b.err != nil {
		return b
	}

	if !b.app.CoreCfg.Kafka.Enabled {
		return b
	}

	if !b.app.CoreCfg.Kafka.OutboxRelay.Enabled {
		return b
	}

	if b.app.DBPool == nil {
		b.err = fmt.Errorf("критическая ошибка: Outbox Relay требует пул БД (вызови WithDatabases раньше)")
		return b
	}

	isProd := b.app.AppCfg.App.IsProd

	b.app.OutboxRelay = pkgkafka.NewOutboxRelay(b.app.DBPool, b.app.CoreCfg.Kafka, isProd)

	relayChecker := health.NewOutboxRelayChecker(b.app.OutboxRelay)
	b.app.HealthCheckers = append(b.app.HealthCheckers, relayChecker)

	return b
}

func (b *Builder) WithStateReplicator() *Builder {
	if b.err != nil {
		return b
	}

	if !b.app.CoreCfg.Kafka.Enabled {
		return b
	}

	if !b.app.CoreCfg.Kafka.StateReplicator.Enabled {
		return b
	}

	if b.app.DBPool == nil {
		b.err = fmt.Errorf("критическая ошибка: StateReplicator требует пул БД (вызови WithDatabases раньше)")
		return b
	}

	b.app.StateReplicator = pkgkafka.NewStateReplicator(b.app.CoreCfg.Kafka, b.app.AppCfg.App.IsProd, b.app.DBPool, b.app.AppCfg.App.PodID)

	syncChecker := health.NewSyncHistoryChecker(b.app.DBPool, b.app.CoreCfg.Kafka.StateReplicator.BootstrapThreshold, b.app.CoreCfg.Kafka.StateReplicator.BootstrapBlock)
	b.app.HealthCheckers = append(b.app.HealthCheckers, syncChecker)

	return b
}

func (b *Builder) WithTaskProcessor() *Builder {
	if b.err != nil {
		return b
	}

	if !b.app.CoreCfg.Kafka.Enabled {
		return b
	}

	if !b.app.CoreCfg.Kafka.TaskProcessor.Enabled {
		return b
	}

	if b.app.DBPool == nil {
		b.err = fmt.Errorf("критическая ошибка: TaskProcessor требует пул БД (вызови WithDatabases раньше)")
		return b
	}

	concurrency := b.app.CoreCfg.Kafka.TaskProcessor.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	b.app.TaskProcessor = pkgkafka.NewTaskProcessor(b.app.CoreCfg.Kafka, b.app.AppCfg.App.IsProd)

	return b
}

func (b *Builder) WithCORS() *Builder {
	if b.err != nil {
		return b
	}

	cfg := b.app.CoreCfg.CORS

	if !cfg.Enabled {
		return b
	}

	//core:cors:legacy
	// Legacy CORS: статический список AllowOrigins из конфига (без Redis)
	corsConfig := cors.Config{
		AllowOrigins:     cfg.AllowOrigins,
		AllowMethods:     cfg.AllowMethods,
		AllowHeaders:     cfg.AllowHeaders,
		AllowCredentials: cfg.AllowCredentials,
	}

	b.app.FiberApp.Use(cors.New(corsConfig))

	slog.Info("CORS middleware успешно активирован (Legacy режим)")
	//core:cors:legacy:end

	//core:cors:redis
	// Redis CORS: динамическая проверка origin через Redis (для сотен политик)
	corsConfigRedis := cors.Config{
		AllowOriginsFunc: func(origin string) bool {
			if origin == "" || origin == "http://localhost:3000" {
				return true
			}
			isAllowed, err := b.app.RedisClient.SIsMember(context.Background(), "cors:allowed_origins", origin).Result()

			if err != nil {
				slog.Error("Ошибка проверки CORS в Redis", "error", err, "origin", origin)
				return false
			}

			return isAllowed
		},

		AllowMethods:     cfg.AllowMethods,
		AllowHeaders:     cfg.AllowHeaders,
		AllowCredentials: cfg.AllowCredentials,
	}

	b.app.FiberApp.Use(cors.New(corsConfigRedis))

	slog.Info("CORS middleware успешно активирован (Redis режим)")
	//core:cors:redis:end

	return b
}

func (b *Builder) WithTracing() *Builder {
	if b.err != nil {
		return b
	}

	cfg := b.app.CoreCfg.Jaeger

	shutdownFn, err := telemetry.InitJaeger(cfg.URL, cfg.ServiceName, cfg.Enabled)
	if err != nil {
		b.err = fmt.Errorf("ошибка инициализации Jaeger: %w", err)
		return b
	}

	if cfg.Enabled {
		slog.Info("Трейсинг Jaeger успешно активирован")
	}

	b.app.TracerShutdown = shutdownFn
	return b
}

func (b *Builder) Build() (*Microservice, error) {
	if b.err != nil {
		return nil, b.err
	}

	coreCfg := b.app.CoreCfg
	isPrometheusEnabled := coreCfg.Prometheus.Enabled
	isJaegerEnabled := coreCfg.Jaeger.Enabled

	midManager := middleware.NewManager(
		isPrometheusEnabled,
		isJaegerEnabled,
		coreCfg.Prometheus.Secure,
		coreCfg.Prometheus.User,
		coreCfg.Prometheus.Password,
		b.app.JWTManager,
		b.app.RedisClient,
		b.app.AppCfg.App.IsProd,
	)

	b.app.FiberApp.Use(midManager.Tracing())
	b.app.FiberApp.Use(midManager.Logging())
	b.app.FiberApp.Use(midManager.Metrics())

	if isPrometheusEnabled {
		slog.Info("Мониторинг Prometheus успешно активирован")
	} else {
		slog.Info("Мониторинг Prometheus отключен")
	}

	return b.app, nil
}

func (m *Microservice) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	if m.OutboxRelay != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.OutboxRelay.Start(ctx)
		}()
	}

	if m.StateReplicator != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.StateReplicator.Start(ctx)
		}()
	}

	if m.TaskProcessor != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.TaskProcessor.Start(ctx)
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		port := m.AppCfg.App.Port
		slog.Info("HTTP Сервер запускается", slog.String("port", port))

		if err := m.FiberApp.Listen(":" + port); err != nil {
			slog.Error("Ошибка HTTP сервера", slog.Any("error", err))
		}
	}()

	if m.GRPCServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			grpcPort := "50051"
			lis, err := net.Listen("tcp", ":"+grpcPort)
			if err != nil {
				slog.Error("Ошибка запуска gRPC listener", slog.Any("error", err))
				return
			}
			slog.Info("gRPC Сервер запускается", slog.String("port", grpcPort))
			if err := m.GRPCServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
				slog.Error("Ошибка gRPC сервера", slog.Any("error", err))
			}
		}()
	}

	<-sigChan
	slog.Info("Получен сигнал остановки, начинаем Graceful Shutdown...")

	if err := m.FiberApp.Shutdown(); err != nil {
		slog.Warn("Ошибка при остановке Fiber", slog.Any("error", err))
	}

	if m.GRPCServer != nil {
		slog.Info("Остановка gRPC сервера...")
		m.GRPCServer.GracefulStop()
	}

	slog.Info("Остановка фоновых воркеров Kafka...")
	cancel()

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		slog.Info("Все фоновые воркеры безопасно остановлены")
	case <-time.After(10 * time.Second):
		slog.Warn("Истекло время ожидания остановки воркеров (Принудительное продолжение)")
	}

	if m.TracerShutdown != nil {
		slog.Info("Остановка трейсера Jaeger...")
		ctxTimeout, cancelTrace := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelTrace()

		if err := m.TracerShutdown(ctxTimeout); err != nil {
			slog.Warn("Ошибка при остановке Jaeger", slog.Any("error", err))
		}
	}

	if m.DBPool != nil {
		slog.Info("Закрытие пула PostgreSQL...")
		m.DBPool.Close()
	}

	if m.RedisClient != nil {
		slog.Info("Закрытие соединений Redis...")
		if err := m.RedisClient.Close(); err != nil {
			slog.Warn("Ошибка при остановке Redis", slog.Any("error", err))
		}
	}

	slog.Info("Микросервис успешно остановлен. До свидания!")
	return nil
}
