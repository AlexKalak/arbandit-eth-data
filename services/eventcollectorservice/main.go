package main

import (
	"context"

	"github.com/alexkalak/go_market_analyze/common/external/rpcclient"
	"github.com/alexkalak/go_market_analyze/common/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorservice"
	"github.com/alexkalak/go_market_analyze/services/merging/src/merger"
	"github.com/ethereum/go-ethereum/common"
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

	pools, err := v3PoolDBRepo.GetPoolsByChainID(chainID)
	if err != nil {
		panic(err)
	}
	pairs, err := v2PairDBRepo.GetPairsByChainID(chainID)
	if err != nil {
		panic(err)
	}

	exchangeAddresses := make([]common.Address, 0, len(pools)+len(pairs))

	for _, pool := range pools {
		exchangeAddresses = append(exchangeAddresses, common.HexToAddress(pool.Address))
	}
	for _, pair := range pairs {
		exchangeAddresses = append(exchangeAddresses, common.HexToAddress(pair.Address))
	}

	// poolAddresses = poolAddresses[:100]

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

	eventCollectorServiceConfig := eventcollectorservice.RPCEventsCollectorServiceConfig{
		ChainID:                 chainID,
		MainnetRPCWS:            env.ETH_MAINNET_RPC_WS,
		MainnetRPCHTTP:          env.ETH_MAINNET_RPC_HTTP,
		KafkaServer:             env.KAFKA_SERVER,
		KafkaUpdateV3PoolsTopic: env.KAFKA_UPDATE_V3_POOLS_TOPIC,
	}
	eventCollectorServiceDependencies := eventcollectorservice.RPCEventCollectorServiceDependencies{
		Merger: merger,
	}

	eventCollectorService, err := eventcollectorservice.New(eventCollectorServiceConfig, eventCollectorServiceDependencies)
	if err != nil {
		panic(err)
	}

	err = eventCollectorService.StartFromBlock(context.Background(), exchangeAddresses)
	if err != nil {
		panic(err)
	}
}
