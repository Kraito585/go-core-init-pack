package health

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Глобальная метрика для Prometheus
var DatabaseNeedsBootstrap = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "database_needs_bootstrap",
	Help: "Indicates if the database sync is stale and needs bootstrap (1 = yes, 0 = no)",
})

type SyncHistoryChecker struct {
	db         *pgxpool.Pool
	threshold  time.Duration
	blockReady bool
}

func NewSyncHistoryChecker(db *pgxpool.Pool, thresholdSeconds int, blockReady bool) *SyncHistoryChecker {
	return &SyncHistoryChecker{
		db:        db,
		threshold: time.Duration(thresholdSeconds) * time.Second,
	}
}

func (c *SyncHistoryChecker) Name() string {
	return "SyncHistory"
}

func (c *SyncHistoryChecker) IsHealthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var lastSync time.Time
	query := `SELECT created_at FROM sync_worker_history ORDER BY created_at DESC LIMIT 1`

	err := c.db.QueryRow(ctx, query).Scan(&lastSync)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			DatabaseNeedsBootstrap.Set(1)
			slog.Warn("Sync history is empty, bootstrap required", slog.Bool("blocking", c.blockReady))

			// Если блокировка ВКЛЮЧЕНА - возвращаем false (под не готов)
			// Если ВЫКЛЮЧЕНА - возвращаем true (под готов, просто собрали метрику)
			return !c.blockReady
		}

		// Если БД физически отвалилась (ошибка сети и т.д.), возвращаем false в любом случае
		slog.Error("Readiness Probe failed: error querying sync history", slog.Any("error", err))
		return false
	}

	timeSinceLastSync := time.Since(lastSync)
	if timeSinceLastSync > c.threshold {
		DatabaseNeedsBootstrap.Set(1)
		slog.Warn("Sync is stale",
			slog.String("last_sync", timeSinceLastSync.Round(time.Second).String()),
			slog.String("threshold", c.threshold.String()),
			slog.Bool("blocking", c.blockReady),
		)

		return !c.blockReady // Аналогично: !true = false (блокируем), !false = true (пропускаем)
	}

	// Всё отлично, база актуальна
	DatabaseNeedsBootstrap.Set(0)
	return true
}
