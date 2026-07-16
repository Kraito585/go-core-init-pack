package service

import (
	"context"
	"log/slog"

	"go-core/internal/repository"
	//core:telemetry
	"go.opentelemetry.io/otel"
	//core:telemetry:end
)

type SendEmailPayload struct {
	TaskID  string `json:"task_id"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type TaskService struct {
	repo *repository.TaskRepository
}

func NewTaskService(repo *repository.TaskRepository) *TaskService {
	return &TaskService{repo: repo}
}

//core:telemetry
var taskTracer = otel.Tracer("replicator-service")

//core:telemetry:end

func (s *TaskService) ProcessSendEmail(ctx context.Context, payload SendEmailPayload) error {
	slog.Info("Эмуляция отправки письма", slog.String("to", payload.To))

	// Здесь логика обращения к внешнему SMTP...

	// Дергаем репозиторий
	err := s.repo.UpdateTaskStatus(ctx, payload.TaskID, "completed")
	if err != nil {
		slog.Error("Не удалось обновить статус задачи в БД", slog.Any("error", err))
		return err
	}

	return nil
}
