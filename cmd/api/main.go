package main

import (
	//core:grpc
	"go-core/api/proto"
	//core:grpc:end
	"go-core/core/pkg/corehandler"
	"go-core/internal/app"
	"go-core/internal/handler"
	"go-core/internal/middleware"
	"go-core/internal/repository"
	"go-core/internal/router"
	"go-core/internal/service"
	"log"
)

func main() {
	// 1. Собираем ядро со всеми новыми компонентами
	ms, err := app.NewBuilder("config.yml").
		WithLogger().
		WithCORS().
		WithTracing().

		//core:postgresql:Migrations
		WithMigrations().
		//core:postgresql:Migrations:end
		//core:postgresql
		WithDatabases().
		//core:postgresql:end
		//core:redis
		WithRedis().
		//core:redis:end
		WithEncryptor().
		//core:kafka:OutboxRelay
		WithOutboxRelay().
		//core:kafka:OutboxRelay:end
		//core:kafka:StateReplicator
		WithStateReplicator().
		//core:kafka:StateReplicator:end
		//core:kafka:TaskProcessor
		WithTaskProcessor().
		//core:kafka:TaskProcessor:end
		//core:grpc
		WithGRPCServer().
		//core:grpc:end
		//core:s3
		WithS3Storage().
		//core:s3:end
		Build()

	if err != nil {
		log.Fatalf("❌ Критическая ошибка при сборке: %v", err)
	}

	// 2. Инициализируем бизнес-логику (Слои чистой архитектуры)
	DefaultRepo := repository.NewDefaultRepository(
		//core:postgresql
		ms.DBPool,
		//core:postgresql:end
		//core:redis
		ms.RedisClient,
		//core:redis:end
	)
	DefaultService := service.NewDefaultService(DefaultRepo, ms.Encryptor, ms.JWTManager, ms.AppCfg.App.IsProd)
	DefaultHandler := handler.NewDefaultHandler(DefaultService, ms.AppCfg.App.IsProd)

	CoreHandler := corehandler.NewDefaultHandler(DefaultService)

	//core:grpc
	myUserService := &service.UserServer{}
	proto.RegisterUserServiceServer(ms.GRPCServer, myUserService)
	//core:grpc:end

	//core:kafka:StateReplicator

	replicatorRepo := repository.NewReplicatorRepository(
		//core:postgresql
		ms.DBPool,
		//core:postgresql:end
		//core:redis
		ms.RedisClient,
		//core:redis:end
	)
	replicatorService := service.NewReplicatorService(
		replicatorRepo,
		ms.AppCfg,
		ms.FiberApp,
	)
	if ms.StateReplicator != nil {
		handler.SetupReplicatorHandlers(ms.StateReplicator, replicatorService)
	}
	//core:kafka:StateReplicator:end

	//core:kafka:TaskProcessor
	// 1. Инициализируем репозиторий
	taskRepo := repository.NewTaskRepository(
		//core:postgresql
		ms.DBPool,
		//core:postgresql:end
		//core:redis
		ms.RedisClient,
		//core:redis:end
	)

	// 2. Инициализируем воркер задач (Task Service)
	taskService := service.NewTaskService(taskRepo)

	// 3. Вызываем регистратор
	if ms.TaskProcessor != nil {
		handler.SetupTaskHandlers(ms.TaskProcessor, taskService)
	}
	//core:kafka:TaskProcessor:end

	// 5. Настраиваем HTTP Роутер Fiber
	// Обрати внимание: мы передаем FiberApp и хендлеры в отдельный пакет роутера,
	// чтобы не засорять main.go тысячей строк с эндпоинтами.
	//

	// Мидлвар-менеджер уже инициализирован в Build()
	// Если нужен доступ к RequireAuth / RateLimit / RequireAPIKey — используем его напрямую
	// Здесь получаем менеджер для прокидывания в роутер
	var midManager *middleware.Manager
	// Мидлвар-менеджер уже создан внутри Build, но нам нужен доступ к методам аутентификации.
	// Пересоздаём для доступа в роутере:
	midManager = middleware.NewManager(
		ms.CoreCfg.Prometheus.Enabled,
		ms.CoreCfg.Jaeger.Enabled,
		ms.CoreCfg.Prometheus.Secure,
		ms.CoreCfg.Prometheus.User,
		ms.CoreCfg.Prometheus.Password,
		ms.JWTManager,
		ms.RedisClient,
		ms.AppCfg.App.IsProd,
	)
	//core:telemetry:end

	router.SetupRoutes(
		ms.FiberApp,
		//core:telemetry
		midManager,
		//core:telemetry:end
		CoreHandler,
		DefaultHandler,
		ms.HealthCheckers,
		//core:telemetry
		ms.CoreCfg.Prometheus.Enabled,
		//core:telemetry:end
	)

	// 6. ЗАПУСК! (Эта функция блокирует поток и держит приложение живым)
	if err := ms.Run(); err != nil {
		log.Fatalf("❌ Ошибка при работе микросервиса: %v", err)
	}
}
