package main

import (
	"context"

	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
	poolupdaterservice "github.com/alexkalak/go_market_analyze/services/poolupdaterservice/src"
)

func main() {
	env, err := envhelper.GetEnv()
	if err != nil {
		panic(err)
	}

	var chainID uint = 1

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
	tokenCacheRepo, err := tokenrepo.NewCacheRepo(context.Background(), tokenrepo.TokenCacheRepoConfig{
		RedisServer: env.REDIS_SERVER,
	})

	if err != nil {
		panic(err)
	}

	v3PoolsDBRepo, err := v3poolsrepo.NewDBRepo(v3poolsrepo.V3PoolDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	v3PoolsCacheRepo, err := v3poolsrepo.NewCacheRepo(context.Background(), v3poolsrepo.V3PoolCacheRepoConfig{
		RedisServer: env.REDIS_SERVER,
	})
	if err != nil {
		panic(err)
	}

	v3TransactionDBRepo, err := v3transactionrepo.NewDBRepo(v3transactionrepo.V3TransactionDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	v3TransactionCacheRepo, err := v3transactionrepo.NewCacheRepo(context.Background(), v3transactionrepo.V3TransacationCacheRepoConfig{
		RedisServer: env.REDIS_SERVER,
	})
	if err != nil {
		panic(err)
	}

	poolUpdaterServiceConfig := poolupdaterservice.PoolUpdaterServiceConfig{
		ChainID:                 chainID,
		KafkaServer:             env.KAFKA_SERVER,
		KafkaUpdateV3PoolsTopic: env.KAFKA_UPDATE_V3_POOLS_TOPIC,
	}
	poolUpdaterServiceDependencies := poolupdaterservice.PoolUpdaterServiceDependencies{
		TokenDBRepo:            tokenRepo,
		TokenCacheRepo:         tokenCacheRepo,
		V3PoolDBRepo:           v3PoolsDBRepo,
		V3PoolCacheRepo:        v3PoolsCacheRepo,
		V3TransactionDBRepo:    v3TransactionDBRepo,
		V3TransactionCacheRepo: v3TransactionCacheRepo,
	}
	poolUpdaterService, err := poolupdaterservice.New(poolUpdaterServiceConfig, poolUpdaterServiceDependencies)
	if err != nil {
		panic(err)
	}

	poolUpdaterService.Start(context.Background())

}
