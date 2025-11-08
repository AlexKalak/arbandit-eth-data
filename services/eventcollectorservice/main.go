package main

import (
	"context"

	"github.com/alexkalak/go_market_analyze/common/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorservice"
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

	pools, err := v3PoolDBRepo.GetPoolsByChainID(chainID)
	if err != nil {
		panic(err)
	}

	poolAddresses := []common.Address{}

	for _, pool := range pools {
		poolAddresses = append(poolAddresses, common.HexToAddress(pool.Address))
	}

	// poolAddresses = poolAddresses[:100]

	eventCollectorServiceConfig := eventcollectorservice.RPCEventsCollectorServiceConfig{
		ChainID:                 chainID,
		MainnetRPCWS:            env.ETH_MAINNET_RPC_WS,
		MainnetRPCHTTP:          env.ETH_MAINNET_RPC_HTTP,
		KafkaServer:             env.KAFKA_SERVER,
		KafkaUpdateV3PoolsTopic: env.KAFKA_UPDATE_V3_POOLS_TOPIC,
	}
	eventCollectorServiceDependencies := eventcollectorservice.RPCEventCollectorServiceDependencies{}

	eventCollectorService, err := eventcollectorservice.New(eventCollectorServiceConfig, eventCollectorServiceDependencies)
	if err != nil {
		panic(err)
	}

	eventCollectorService.StartFromBlockV3(context.Background(), poolAddresses)
}
