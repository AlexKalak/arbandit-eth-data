package main

import (
	"github.com/alexkalak/go_market_analyze/common/external/rpcclient"
	"github.com/alexkalak/go_market_analyze/common/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/services/merging/src/merger"
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

	subgraphClient, err := subgraphs.NewSubgraphClient(subgraphs.SubgraphClientConfig{
		APIKey: env.SUBGRAPH_API_TOKEN,
	})
	if err != nil {
		panic(err)
	}

	tokenRepo, err := tokenrepo.New(tokenrepo.TokenRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}
	v3PoolsRepo, err := v3poolsrepo.NewDBRepo(v3poolsrepo.V3PoolDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	rpcClient, err := rpcclient.NewRpcClient(rpcclient.RpcClientConfig{
		EthMainnetWs:   env.ETH_MAINNET_RPC_WS,
		EthMainnetHttp: env.ETH_MAINNET_RPC_HTTP,
	})
	if err != nil {
		panic(err)
	}

	mergerDependencies := merger.MergerDependencies{
		Database:       pgDB,
		SubgraphClient: subgraphClient,
		TokenRepo:      tokenRepo,
		V3PoolsDBRepo:  v3PoolsRepo,
		RpcClient:      rpcClient,
	}

	merger, err := merger.NewMerger(mergerDependencies)
	if err != nil {
		panic(err)
	}
	var chainID uint = 1
	// blockNumber := big.NewInt(23678329)

	err = merger.ValidateV3PoolsAndComputeAverageUSDPrice(chainID)
	if err != nil {
		panic(err)
	}
}
