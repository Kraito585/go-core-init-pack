package handler

import (
	"context"
	pkgkafka "go-core/core/pkg/kafka"
	"go-core/internal/service"

	//core:telemetry
	"go.opentelemetry.io/otel"
	//core:telemetry:end
)

//core:telemetry
var replicatorHandlerTracer = otel.Tracer("http-handler")

//core:telemetry:end

// SetupReplicatorHandlers привязывает методы SyncService к событиям в Kafka
func SetupReplicatorHandlers(replicator *pkgkafka.StateReplicator, replicatorService *service.ReplicatorService) {

	replicator.RegisterHandler("user.registered", func(ctx context.Context, payload []byte) error {
		return replicatorService.HandleUserRegistered(ctx, payload)
	})
}
