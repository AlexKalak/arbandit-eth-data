package main

import (
	"context"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/external/rpcclient"
	"github.com/alexkalak/go_market_analyze/common/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/services/merging/src/merger"
)

var chainID uint = 56

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

	tokenRepo, err := tokenrepo.NewDBRepo(tokenrepo.TokenDBRepoDependencies{
		Database: pgDB,
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
	v2PairDBRepo, err := v2pairsrepo.NewDBRepo(v2pairsrepo.V2PairDBRepoDependencies{
		Database: pgDB,
	})
	if err != nil {
		panic(err)
	}

	rpcClient, err := rpcclient.NewRpcClient(rpcclient.RpcClientConfig{
		WsURL:   env.ETH_MAINNET_RPC_WS,
		HttpURL: env.ETH_MAINNET_RPC_HTTP,
	})
	if err != nil {
		panic(err)
	}

	mergerDependencies := merger.MergerDependencies{
		Database:       pgDB,
		SubgraphClient: subgraphClient,
		TokenRepo:      tokenRepo,
		V3PoolsDBRepo:  v3PoolDBRepo,
		V2PairDBRepo:   v2PairDBRepo,
		RpcClient:      rpcClient,
	}

	merger, err := merger.NewMerger(mergerDependencies)
	if err != nil {
		panic(err)
	}

	// mergeTokens(merger)
	// mergePools(merger)
	// mergePoolsTicks(merger)
	// mergePoolsData(merger)

	// validatePools(merger)

	// imitatePoolSwap(merger)

	// mergePairs(merger)
	mergePairsData(merger)
	// validatePairs(merger)
}

func mergeTokens(merger merger.Merger) {

	err := merger.MergeTokens(chainID)
	if err != nil {
		panic(err)
	}
}

func mergePools(merger merger.Merger) {
	err := merger.MergePools(chainID)
	if err != nil {
		panic(err)
	}
}

func mergePoolsTicks(merger merger.Merger) {
	err := merger.MergePoolsTicks(context.Background(), chainID)
	if err != nil {
		panic(err)
	}
}

func mergePoolsData(merger merger.Merger) {
	blockNumber := big.NewInt(int64(23821196))

	err := merger.MergePoolsData(context.Background(), chainID, blockNumber)
	if err != nil {
		panic(err)
	}
}

func validatePools(merger merger.Merger) {
	err := merger.ValidateV3PoolsAndComputeAverageUSDPrice(chainID)
	if err != nil {
		panic(err)
	}
}

func imitatePoolSwap(merger merger.Merger) {
	poolAddress := "0xa5f43b0ebaefbed5b1f1bfc809af15254ea1e9c4"
	err := merger.ImitateSwapForPool(models.V3PoolIdentificator{
		Address: poolAddress,
		ChainID: chainID,
	}, big.NewInt(10))

	if err != nil {
		panic(err)
	}
}

func mergePairs(merger merger.Merger) {
	err := merger.MergePairs(chainID)
	if err != nil {
		panic(err)
	}

}

func mergePairsData(merger merger.Merger) {
	blockNumber := big.NewInt(int64(23821196))

	err := merger.MergePairsData(context.Background(), chainID, blockNumber)
	if err != nil {
		panic(err)
	}
}

func validatePairs(merger merger.Merger) {
	err := merger.ValidateV2PairsAndComputeAverageUSDPrice(chainID)
	if err != nil {
		panic(err)
	}
}
