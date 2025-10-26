package pgdatabase

import (
	"database/sql"
	"fmt"

	"github.com/alexkalak/go_market_analyze/src/helpers/envhelper"
	_ "github.com/lib/pq"
)

type PgDatabase struct {
	db *sql.DB
}

var singleton *PgDatabase

func (d *PgDatabase) GetDB() (*sql.DB, error) {
	if d.db == nil {
		dbStruct, err := New()
		if err != nil {
			return nil, err
		}
		d.db = dbStruct.db
		return d.db, nil
	}

	return d.db, nil
}

func New() (*PgDatabase, error) {
	if singleton != nil {
		return singleton, nil
	}

	env, err := envhelper.GetEnv()
	if err != nil {
		return nil, err
	}

	singleton = &PgDatabase{}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s ",
		env.POSTGRES_HOST, env.POSTGRES_PORT, env.POSTGRES_USER, env.POSTGRES_PASSWORD, env.POSTGRES_DB_NAME, env.POSTGRES_SSL_MODE)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		singleton = nil
		return nil, err
	}

	singleton.db = db
	return singleton, nil
}
