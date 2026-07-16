package repository

import (
	"context"
	//core:redis
	coreredis "go-core/core/pkg/redis"
	//core:redis:end

	//core:postgresql
	"github.com/jackc/pgx/v5/pgxpool"
	//core:postgresql:end
)

type TaskRepository struct {
	//core:postgresql
	db *pgxpool.Pool
	//core:postgresql:end
	//core:redis
	r *coreredis.Wrapper
	//core:redis:end
}

func NewTaskRepository(
	//core:postgresql
	db *pgxpool.Pool,
	//core:postgresql:end
	//core:redis
	r *coreredis.Wrapper,
	//core:redis:end
) *TaskRepository {
	return &TaskRepository{
		//core:postgresql
		db: db,
		//core:postgresql:end
		//core:redis
		r: r,
		//core:redis:end
	}
}

// UpdateTaskStatus обновляет статус выполнения задачи в БД
func (r *TaskRepository) UpdateTaskStatus(ctx context.Context, taskID string, status string) error {
	query := `UPDATE background_tasks SET status = $1, updated_at = NOW() WHERE id = $2`
	_, err := r.db.Exec(ctx, query, status, taskID)
	return err
}
