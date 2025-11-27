package main

import (
	"context"
	"flag"

	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v2transactionrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
	"github.com/alexkalak/go_market_analyze/services/arbitrageservice/src/arbitrageservice"
	"github.com/alexkalak/go_market_analyze/services/arbitrageservice/src/controllers/arbitragegrpc"
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
	tokenCacheRepo, err := tokenrepo.NewCacheRepo(context.Background(), tokenrepo.TokenCacheRepoConfig{
		RedisServer: env.REDIS_SERVER,
	})
	if err != nil {
		panic(err)
	}

	v3PoolDBRepo, err := v3poolsrepo.NewDBRepo(v3poolsrepo.V3PoolDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	v3PoolCacheRepo, err := v3poolsrepo.NewCacheRepo(context.Background(), v3poolsrepo.V3PoolCacheRepoConfig{
		RedisServer: env.REDIS_SERVER,
	})
	if err != nil {
		panic(err)
	}

	v2PairDBRepo, err := v2pairsrepo.NewDBRepo(v2pairsrepo.V2PairDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}
	v2PairCacheRepo, err := v2pairsrepo.NewCacheRepo(context.Background(), v2pairsrepo.V2PairCacheRepoConfig{
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
	v2TransactionDBRepo, err := v2transactionrepo.NewDBRepo(v2transactionrepo.V2TransactionDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	arbitrageServiceDependencies := arbitrageservice.ArbitrageServiceDependencies{
		TokenRepo:      tokenRepo,
		TokenCacheRepo: tokenCacheRepo,

		V2PairDBRepo:    v2PairDBRepo,
		V2PairCacheRepo: v2PairCacheRepo,
		V3PoolCacheRepo: v3PoolCacheRepo,
		V3PoolDBRepo:    v3PoolDBRepo,

		V2TransactionsDBRepo: v2TransactionDBRepo,
		V3TransactionDBRepo:  v3TransactionDBRepo,
	}

	var chainID uint = 1
	arbitrageService, err := arbitrageservice.New(chainID, arbitrageServiceDependencies)
	if err != nil {
		panic(err)
	}

	command := flag.String("command", "", "What command to run, empty = grpc")
	flag.Parse()

	if command == nil || *command == "" || *command == "grpc" {
		gRPCConfig := arbitragegrpc.ArbitrageGRPCServerConfig{
			Port: env.ARBITRAGE_GRPC_PORT,
		}
		gRPCDependencies := arbitragegrpc.ArbitrageGRPCServerDependencies{
			ArbitrageService: arbitrageService,
		}
		gRPCServer, err := arbitragegrpc.New(gRPCConfig, gRPCDependencies)
		if err != nil {
			panic(err)
		}

		err = gRPCServer.Start()
		if err != nil {
			panic(err)
		}

	}

	switch *command {
	case "oldArbs":
		err := arbitrageService.FindOldArbs()
		if err != nil {
			panic(err)
		}
	case "findArbs":
		_, err := arbitrageService.FindAllArbs(1)
		if err != nil {
			panic(err)
		}
	case "startSearching":
		err := arbitrageService.Start(context.Background())
		if err != nil {
			panic(err)
		}
	}
}
