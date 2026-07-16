package pkgkafka

import (
	"context"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	coreconfig "go-core/core/config"
	//core:telemetry
	"go-core/core/pkg/coretelemetry"
	//core:telemetry:end

	"github.com/segmentio/kafka-go"
)

// TaskHandlerFunc — контракт для задач
type TaskHandlerFunc func(ctx context.Context, payload []byte) error

type TaskProcessor struct {
	reader    *kafka.Reader
	isHealthy atomic.Bool
	isProd    bool
	handlers  map[string]TaskHandlerFunc
}

func NewTaskProcessor(kCfg coreconfig.KafkaConfig, isProd bool) *TaskProcessor {
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: kCfg.SASLMechanism(),
		TLS:           kCfg.TLSConfig(),
	}

	return &TaskProcessor{
		isProd:   isProd,
		handlers: make(map[string]TaskHandlerFunc),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     kCfg.Brokers,
			Topic:       kCfg.TaskProcessor.Topic,   // Свой топик
			GroupID:     kCfg.TaskProcessor.GroupID, // Свой GroupID
			Dialer:      dialer,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     10 * time.Millisecond,
		}),
	}
}

func (p *TaskProcessor) RegisterHandler(taskType string, handler TaskHandlerFunc) {
	p.handlers[taskType] = handler
}

func (p *TaskProcessor) IsHealthy() bool {
	return p.isHealthy.Load()
}

func (p *TaskProcessor) Start(ctx context.Context) {
	log.Println("⚙️ Task Processor started")
	p.isHealthy.Store(true)

	defer p.isHealthy.Store(false)
	defer p.reader.Close()

	for {
		var m kafka.Message
		var err error

		// ==========================================
		// ЧТЕНИЕ ЗАДАЧИ (FETCH)
		// ==========================================
		//core:telemetry
		err = coretelemetry.ObserveKafka("fetch", func() error {
			//core:telemetry:end

			m, err = p.reader.FetchMessage(ctx)

			//core:telemetry
			return err
		})
		//core:telemetry:end

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("❌ Task Fetch error: %v", err)
			continue
		}

		taskType := p.getHeader(m.Headers, "x-event-type")

		// ==========================================
		// ЗАМЕР ЗАДЕРЖКИ
		// ==========================================
		//core:telemetry
		createdAtStr := p.getHeader(m.Headers, "x-created-at")
		if createdAtStr != "" {
			if nano, parseErr := strconv.ParseInt(createdAtStr, 10, 64); parseErr == nil {
				delay := time.Since(time.Unix(0, nano)).Seconds()
				coretelemetry.EventDeliveryDelay.WithLabelValues(taskType).Observe(delay)
			}
		}
		//core:telemetry:end

		if !p.isProd {
			log.Printf("📥 Processing task [%s]: key=%s", taskType, string(m.Key))
		}

		handler, exists := p.handlers[taskType]
		var processErr error

		if exists {
			processErr = handler(ctx, m.Value)
		} else {
			log.Printf("⚠️ Unknown task type (ignored): %s", taskType)
			processErr = nil
		}

		// ==========================================
		// КОММИТ И ТЕЛЕМЕТРИЯ РЕЗУЛЬТАТА
		// ==========================================
		if processErr == nil {
			var commitErr error

			//core:telemetry
			commitErr = coretelemetry.ObserveKafka("commit", func() error {
				//core:telemetry:end

				commitErr = p.reader.CommitMessages(ctx, m)

				//core:telemetry
				return commitErr
			})
			//core:telemetry:end

			if commitErr != nil {
				log.Printf("Failed to commit task: %v", commitErr)
			}

			// Избавляемся от else для успешного выполнения
			//core:telemetry
			if commitErr == nil {
				coretelemetry.TasksProcessedTotal.WithLabelValues(taskType, "success").Inc()
			}
			//core:telemetry:end

		} else {
			log.Printf("Task processing error (retrying later): %v", processErr)

			//core:telemetry
			coretelemetry.TasksProcessedTotal.WithLabelValues(taskType, "error").Inc()
			//core:telemetry:end
		}
	}
}

func (p *TaskProcessor) getHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
