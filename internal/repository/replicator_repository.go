package repository

import (
	//core:redis
	coreredis "go-core/core/pkg/redis"
	//core:redis:end

	//core:postgresql
	"github.com/jackc/pgx/v5/pgxpool"
	//core:postgresql:end
)

type ReplicatorRepository struct {
	//core:postgresql
	db *pgxpool.Pool
	//core:postgresql:end
	//core:redis
	r *coreredis.Wrapper
	//core:redis:end
}

func NewReplicatorRepository(
	//core:postgresql
	db *pgxpool.Pool,
	//core:postgresql:end
	//core:redis
	r *coreredis.Wrapper,
	//core:redis:end
) *ReplicatorRepository {
	return &ReplicatorRepository{
		//core:postgresql
		db: db,
		//core:postgresql:end
		//core:redis
		r: r,
		//core:redis:end
	}
}
func (r *ReplicatorRepository) test() (string, error) {
	return "repo", nil
}
