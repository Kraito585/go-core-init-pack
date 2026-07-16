package pkgkafka

import (
	"context"
	"go-core/core/config"

	//core:telemetry
	"go-core/core/pkg/coretelemetry"
	//core:telemetry:end
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type EventHandlerFunc func(ctx context.Context, payload []byte) error

type StateReplicator struct {
	reader    *kafka.Reader
	isHealthy atomic.Bool
	isProd    bool
	handlers  map[string]EventHandlerFunc
	//core:kafka:StateReplicator:SyncCheck
	db    *pgxpool.Pool
	podID string
	//core:kafka:StateReplicator:SyncCheck:end
}

func NewStateReplicator(
	kCfg config.KafkaConfig,
	isProd bool,
	//core:kafka:StateReplicator:SyncCheck
	db *pgxpool.Pool,
	podID string,
	//core:kafka:StateReplicator:SyncCheck:end
) *StateReplicator {
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: kCfg.SASLMechanism(),
		TLS:           kCfg.TLSConfig(),
	}

	return &StateReplicator{
		isProd:   isProd,
		handlers: make(map[string]EventHandlerFunc),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     kCfg.Brokers,
			Topic:       kCfg.StateReplicator.Topic,
			GroupID:     kCfg.StateReplicator.GroupID,
			Dialer:      dialer,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     10 * time.Millisecond,
		}),
	}
}

func (r *StateReplicator) RegisterHandler(eventType string, handler EventHandlerFunc) {
	r.handlers[eventType] = handler
}

func (r *StateReplicator) IsHealthy() bool {
	return r.isHealthy.Load()
}

func (r *StateReplicator) Start(ctx context.Context) {
	log.Println("🚀 State Replicator started")
	r.isHealthy.Store(true)

	defer r.isHealthy.Store(false)
	defer r.reader.Close()

	for {
		var m kafka.Message
		var err error

		// ==========================================
		// ЧТЕНИЕ СООБЩЕНИЯ (FETCH)
		// ==========================================
		//core:telemetry
		err = coretelemetry.ObserveKafka("fetch", func() error {
			//core:telemetry:end

			m, err = r.reader.FetchMessage(ctx)

			//core:telemetry
			return err
		})
		//core:telemetry:end

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("❌ Fetch error: %v", err)
			continue
		}

		eventType := r.getHeader(m.Headers, "x-event-type")

		// ==========================================
		// ЗАМЕР ЗАДЕРЖКИ (Оборачиваем целиком, чтобы не тратить CPU)
		// ==========================================
		//core:telemetry
		createdAtStr := r.getHeader(m.Headers, "x-created-at")
		if createdAtStr != "" {
			if nano, parseErr := strconv.ParseInt(createdAtStr, 10, 64); parseErr == nil {
				delay := time.Since(time.Unix(0, nano)).Seconds()
				coretelemetry.EventDeliveryDelay.WithLabelValues(eventType).Observe(delay)
			}
		}
		//core:telemetry:end

		if !r.isProd {
			log.Printf("📥 Received event [%s]: key=%s", eventType, string(m.Key))
		}

		handler, exists := r.handlers[eventType]
		var syncErr error

		if exists {
			syncErr = handler(ctx, m.Value)
		} else {
			log.Printf("⚠️ Unknown event type (ignored): %s", eventType)
			syncErr = nil
		}

		//core:postgresql
		if r.db != nil {
			eventIDStr := r.getHeader(m.Headers, "x-event-id")
			if eventIDStr != "" {
				status := "success"
				if syncErr != nil {
					status = "error"
				} else if !exists {
					status = "ignored"
				}

				query := `INSERT INTO sync_worker_history (event_id, pod_id, event_type, status) VALUES ($1, $2, $3, $4)`
				_, dbErr := r.db.Exec(context.Background(), query, eventIDStr, r.podID, eventType, status)

				if dbErr != nil {
					log.Printf("⚠️ Не удалось записать историю синхронизации события %s: %v", eventIDStr, dbErr)
				}
			}
		}
		//core:postgresql:end

		if syncErr == nil {
			var commitErr error

			//core:telemetry
			commitErr = coretelemetry.ObserveKafka("commit", func() error {
				//core:telemetry:end

				commitErr = r.reader.CommitMessages(ctx, m)

				//core:telemetry
				return commitErr
			})
			//core:telemetry:end

			if commitErr != nil {
				log.Printf("Failed to commit message: %v", commitErr)
			}

			// Вместо else используем плоскую проверку (выполнится при успехе)
			//core:telemetry
			if commitErr == nil {
				coretelemetry.SyncEventsTotal.WithLabelValues(eventType, "success").Inc()
			}
			//core:telemetry:end

		} else {
			log.Printf("Sync error (retrying later): %v", syncErr)

			//core:telemetry
			coretelemetry.SyncEventsTotal.WithLabelValues(eventType, "error").Inc()
			//core:telemetry:end
		}
	}
}

func (r *StateReplicator) getHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
