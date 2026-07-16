package handler

import (
	"context"
	"encoding/json"
	"log/slog"

	pkgkafka "go-core/core/pkg/kafka"
	"go-core/internal/service"

	//core:telemetry
	"go.opentelemetry.io/otel"
	//core:telemetry:end
)

//core:telemetry
var taskHandlerTracer = otel.Tracer("http-handler")

//core:telemetry:end

// SetupTaskHandlers привязывает методы TaskService к задачам в Kafka
func SetupTaskHandlers(processor *pkgkafka.TaskProcessor, taskService *service.TaskService) {

	processor.RegisterHandler("send_email", func(ctx context.Context, payload []byte) error {
		var data service.SendEmailPayload

		// Хендлер принимает запрос и парсит байты
		if err := json.Unmarshal(payload, &data); err != nil {
			slog.Error("Ошибка парсинга payload для send_email", slog.Any("error", err))
			return err
		}

		// Отправляет данные в сервис и возвращает результат
		return taskService.ProcessSendEmail(ctx, data)
	})

	// По аналогии тут будут другие задачи:
	// processor.RegisterHandler("clean_cache", func(ctx context.Context, payload []byte) error { ... })
}
