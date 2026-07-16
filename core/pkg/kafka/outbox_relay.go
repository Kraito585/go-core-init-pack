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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type OutboxEvent struct {
	ID        uuid.UUID
	EventType string
	Topic     string
	Payload   []byte
	CreatedAt time.Time
}

type OutboxRelay struct {
	db        *pgxpool.Pool
	writer    *kafka.Writer
	tick      *time.Ticker
	isHealthy atomic.Bool
	isProd    bool
}

func NewOutboxRelay(db *pgxpool.Pool, kCfg config.KafkaConfig, isProd bool) *OutboxRelay {

	transport := &kafka.Transport{
		SASL:        kCfg.SASLMechanism(),
		IdleTimeout: 30 * time.Second,
		TLS:         kCfg.TLSConfig(),
	}

	if !isProd {
		client := &kafka.Client{
			Addr:      kafka.TCP(kCfg.Brokers...),
			Transport: transport,
		}

		log.Println("⏳ Ждем готовности Kafka...")
		for i := 0; i < 15; i++ {
			_, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
				Topics: []kafka.TopicConfig{
					{Topic: "synk_auth", NumPartitions: 1, ReplicationFactor: 1},
					{Topic: "mail_data", NumPartitions: 1, ReplicationFactor: 1},
				},
			})
			if err == nil {
				log.Println("✅ Kafka готова, топики проверены.")
				break
			}
			log.Printf("⏳ Kafka еще не готова (попытка %d): %v", i, err)
			time.Sleep(2 * time.Second)
		}
	}

	return &OutboxRelay{
		db:   db,
		tick: time.NewTicker(time.Duration(kCfg.OutboxRelay.Tick) * time.Millisecond),
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(kCfg.Brokers...),
			Balancer:               &kafka.Hash{},
			MaxAttempts:            5,
			RequiredAcks:           kafka.RequireOne,
			Transport:              transport,
			WriteTimeout:           10 * time.Second,
			AllowAutoTopicCreation: true,
			BatchTimeout:           10 * time.Millisecond,
		},
	}
}

func (r *OutboxRelay) IsHealthy() bool {
	return r.isHealthy.Load()
}

func (r *OutboxRelay) Start(ctx context.Context) {
	log.Println("Outbox Relay started")
	r.isHealthy.Store(true)
	defer r.isHealthy.Store(false)
	defer r.writer.Close()
	defer r.tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.tick.C:
			r.processBatch(ctx)
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)

	fetchEventsSQL := `
    UPDATE outbox_events
        SET status = 'processing'
        WHERE id IN (
            SELECT id
            FROM outbox_events
            WHERE status = 'pending' 
                AND (scheduled_at <= NOW())
            ORDER BY created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
    RETURNING id, event_type, topic, payload, created_at;`

	rows, err := tx.Query(ctx, fetchEventsSQL, 50)
	if err != nil {
		return
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Topic, &e.Payload, &e.CreatedAt); err != nil {
			return
		}
		events = append(events, e)
	}

	if len(events) == 0 {
		return
	}

	kafkaMsgs := make([]kafka.Message, len(events))
	ids := make([]uuid.UUID, len(events))

	for i, e := range events {
		timestampStr := strconv.FormatInt(e.CreatedAt.UnixNano(), 10)

		kafkaMsgs[i] = kafka.Message{
			Topic: e.Topic,
			Key:   []byte(e.ID.String()),
			Value: e.Payload,
			Headers: []kafka.Header{
				{Key: "x-event-id", Value: []byte(e.ID.String())},
				{Key: "x-event-type", Value: []byte(e.EventType)},
				{Key: "x-created-at", Value: []byte(timestampStr)},
			},
		}
		ids[i] = e.ID
	}

	//core:telemetry
	err = coretelemetry.ObserveKafka("write", func() error {
		//core:telemetry:end

		err = r.writer.WriteMessages(ctx, kafkaMsgs...)

		//core:telemetry
		return err
	})
	//core:telemetry:end

	if err != nil {
		log.Printf("Kafka write error: %v", err)
		return
	}

	_, err = tx.Exec(ctx, "DELETE FROM outbox_events WHERE id = ANY($1)", ids)
	if err != nil {
		log.Printf("Finalize error: %v", err)
		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Printf("Commit error: %v", err)
		return
	}

	// Выполнится только если Commit прошел успешно (err == nil)
	//core:telemetry
	coretelemetry.OutboxRelayedTotal.Add(float64(len(events)))
	//core:telemetry:end
}
