package service

import (
	"context"
	"log"

	"github.com/gofiber/fiber/v3"
	//core:telemetry
	"go.opentelemetry.io/otel"
	//core:telemetry:end

	"go-core/internal/repository"
	appconfig "go-core/pkg/config"
)

type ReplicatorService struct {
	repo   *repository.ReplicatorRepository
	appCfg *appconfig.AppConfig
	router fiber.Router // Храним ссылку на роутер для внутренних нужд
}

// NewReplicatorService принимает все высокоуровневые зависимости приложения
func NewReplicatorService(
	repo *repository.ReplicatorRepository,
	appCfg *appconfig.AppConfig,
	router fiber.Router,
) *ReplicatorService {
	return &ReplicatorService{
		repo:   repo,
		appCfg: appCfg,
		router: router,
	}
}

//core:telemetry
var replicatorTracer = otel.Tracer("replicator-service")

//core:telemetry:end

func (s *ReplicatorService) HandleUserRegistered(ctx context.Context, payload []byte) error {
	if !s.appCfg.App.IsProd {
		log.Println("🛠 [Dev] Обработка регистрации пользователя в ReplicatorService")
	}

	return nil
}
