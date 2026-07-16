package repository

import (
	coreredis "go-core/core/pkg/redis"
	"go-core/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

type DefaultRepository struct {
	db *pgxpool.Pool
	r  *coreredis.Wrapper
}

func NewDefaultRepository(
	db *pgxpool.Pool,
	r *coreredis.Wrapper,
) *DefaultRepository {
	return &DefaultRepository{
		db: db,
		r:  r,
	}
}

var defaultRepoTracer = otel.Tracer("default-repository")

func (r *DefaultRepository) DefaultFunc() error {
	return nil
}