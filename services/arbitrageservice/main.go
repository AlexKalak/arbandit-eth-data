package main

import (
	"context"

	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	arbitrageservice "github.com/alexkalak/go_market_analyze/services/arbitrageservice/src"
)

func main() {

	env, err := envhelper.GetEnv()
	if err != nil {
		panic(err)
	}

	pgConf := pgdatabase.PgDatabaseConfig{
		Host:     env.POSTGRES_HOST,
		Port:     env.POSTGRES_PORT,
		User:     env.POSTGRES_USER,
		Password: env.POSTGRES_PASSWORD,
		DBName:   env.POSTGRES_DB_NAME,
		SSlMode:  env.POSTGRES_SSL_MODE,
	}
	pgDB, err := pgdatabase.New(pgConf)
	if err != nil {
		panic(err)
	}

	tokenRepo, err := tokenrepo.NewDBRepo(tokenrepo.TokenDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	V3PoolCacheRepo, err := v3poolsrepo.NewCacheRepo(context.Background(), v3poolsrepo.V3PoolCacheRepoConfig{
		RedisServer: env.REDIS_SERVER,
	})

	if err != nil {
		panic(err)
	}
	arbitrageServiceDependencies := arbitrageservice.ArbitrageServiceDependencies{
		TokenRepo:       tokenRepo,
		V3PoolCacheRepo: V3PoolCacheRepo,
	}

	var chainID uint = 1
	arbitrageService, err := arbitrageservice.New(chainID, arbitrageServiceDependencies)
	if err != nil {
		panic(err)
	}

	arbitrageService.Start(context.Background())
}
