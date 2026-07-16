package health

import (
	"context"

	//core:kafka
	pkgkafka "go-core/core/pkg/kafka"

	//core:kafka:end
	//core:redis
	coreredis "go-core/core/pkg/redis"
	//core:redis:end
	"sync/atomic"
	"time"

	//core:postgresql
	"github.com/jackc/pgx/v5/pgxpool"
	//core:postgresql:end
)

type Checker interface {
	Name() string
	IsHealthy() bool
}

//core:postgresql:Migrations
type MigrationChecker struct {
	ready *atomic.Bool
}

func NewMigrationChecker(ready *atomic.Bool) *MigrationChecker {
	return &MigrationChecker{ready: ready}
}

func (m *MigrationChecker) Name() string {
	return "Database Migrations"
}

func (m *MigrationChecker) IsHealthy() bool {
	return m.ready.Load()
}

//core:postgresql:Migrations:end

//core:redis
type RedisChecker struct {
	client *coreredis.Wrapper
}

func NewRedisChecker(client *coreredis.Wrapper) *RedisChecker {
	return &RedisChecker{client: client}
}

func (r *RedisChecker) Name() string {
	return "Redis Cache"
}

func (r *RedisChecker) IsHealthy() bool {
	// 1. Проверяем, инициализирован ли вообще клиент
	if r.client == nil {
		return false
	}

	// 2. Задаем жесткий таймаут, чтобы проверка не зависла, если сеть пропадет
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 3. Делаем реальный сетевой запрос к Redis (используем метод Ping из нашего Wrapper'а)
	err := r.client.Ping(ctx).Err()

	// Если ошибки нет (err == nil), значит Redis жив и отвечает
	return err == nil
}

//core:redis:end

//core:postgresql
type PostgresChecker struct {
	pool *pgxpool.Pool
}

func NewPostgresChecker(pool *pgxpool.Pool) *PostgresChecker {
	return &PostgresChecker{pool: pool}
}

func (p *PostgresChecker) Name() string { return "PostgreSQL Database" }

func (p *PostgresChecker) IsHealthy() bool {
	if p.pool == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := p.pool.Ping(ctx)
	return err == nil
}

//core:postgresql:end

//core:kafka:OutboxRelay
type OutboxRelayChecker struct {
	relay *pkgkafka.OutboxRelay
}

func NewOutboxRelayChecker(relay *pkgkafka.OutboxRelay) *OutboxRelayChecker {
	return &OutboxRelayChecker{relay: relay}
}

func (c *OutboxRelayChecker) Name() string {
	return "Kafka Outbox Relay"
}

func (c *OutboxRelayChecker) IsHealthy() bool {
	if c.relay == nil {
		return false
	}
	// Вызываем твой готовый метод!
	return c.relay.IsHealthy()
}

//core:kafka:OutboxRelay:end

//core:kafka:StateReplicator
type StateReplicatorChecker struct {
	replicator *pkgkafka.StateReplicator
}

func NewStateReplicatorChecker(replicator *pkgkafka.StateReplicator) *StateReplicatorChecker {
	return &StateReplicatorChecker{replicator: replicator}
}

func (c *StateReplicatorChecker) Name() string { return "Kafka State Replicator" }
func (c *StateReplicatorChecker) IsHealthy() bool {
	if c.replicator == nil {
		return false
	}
	return c.replicator.IsHealthy()
}

//core:kafka:StateReplicator:end

//core:kafka:TaskProcessor
type TaskProcessorChecker struct {
	processor *pkgkafka.TaskProcessor
}

func NewTaskProcessorChecker(processor *pkgkafka.TaskProcessor) *TaskProcessorChecker {
	return &TaskProcessorChecker{processor: processor}
}

func (c *TaskProcessorChecker) Name() string { return "Kafka Task Processor" }
func (c *TaskProcessorChecker) IsHealthy() bool {
	if c.processor == nil {
		return false
	}
	return c.processor.IsHealthy()
}

//core:kafka:TaskProcessor:end

//core:s3
func (c *S3Checker) IsHealthy() bool {
	if c.client == nil {
		return false
	}

	// Для IsHealthy нам нужен контекст с таймаутом, чтобы не зависнуть
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.client.Ping(ctx)
	return err == nil
}

//core:s3:end
