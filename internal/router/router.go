package router

import (
	"go-core/core/pkg/corehandler"
	"go-core/core/pkg/corerouter"
	"go-core/core/pkg/health"
	"go-core/internal/handler"
	"go-core/internal/middleware"
	"time"

	"github.com/gofiber/fiber/v3"
)

// SetupRoutes настраивает все пути приложения
func SetupRoutes(
	app *fiber.App,
	midManager *middleware.Manager,
	coreHandler *corehandler.DefaultHandler,
	defaultHandler *handler.DefaultHandler,
	healthCheckers []health.Checker,
	promEnabled bool,
) {
	app.Use(midManager.Tracing())

	corerouter.RegisterSystemRoutes(app, healthCheckers, promEnabled, midManager.MetricsAuth())

	api := app.Group("/api/v1", midManager.Metrics())
}
