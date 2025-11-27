package envhelper

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Environment struct {
	BYBIT_API_KEY      string
	BYBIT_API_SECRET   string
	SUBGRAPH_API_TOKEN string
	POSTGRES_HOST      string
	POSTGRES_PORT      string
	POSTGRES_USER      string
	POSTGRES_PASSWORD  string
	POSTGRES_DB_NAME   string
	POSTGRES_SSL_MODE  string

	ETH_MAINNET_RPC_WS   string
	ETH_MAINNET_RPC_HTTP string
	BSC_MAINNET_RPC_WS   string
	BSC_MAINNET_RPC_HTTP string

	KAFKA_SERVER                string
	KAFKA_UPDATE_V3_POOLS_TOPIC string

	REDIS_SERVER        string
	ARBITRAGE_GRPC_PORT uint
}

var env *Environment

func GetEnv() (*Environment, error) {
	if env != nil {
		return env, nil
	}

	env = &Environment{}
	err := load()
	if err != nil {
		env = nil
		return nil, err
	}
	return env, nil
}

const _BYBIT_API_KEY = "BYBIT_API_KEY"
const _BYBIT_API_SECRET = "BYBIT_API_SECRET"
const _SUBGRAPH_API_TOKEN = "SUBGRAPH_API_TOKEN"

const _POSTGRES_HOST = "POSTGRES_HOST"
const _POSTGRES_PORT = "POSTGRES_PORT"
const _POSTGRES_USER = "POSTGRES_USER"
const _POSTGRES_PASSWORD = "POSTGRES_PASSWORD"
const _POSTGRES_DB_NAME = "POSTGRES_DB_NAME"
const _POSTGRES_SSL_MODE = "POSTGRES_SSL_MODE"

const _ETH_MAINNET_RPC_WS = "ETH_MAINNET_RPC_WS"
const _ETH_MAINNET_RPC_HTTP = "ETH_MAINNET_RPC_HTTP"

const _BSC_MAINNET_RPC_WS = "BSC_MAINNET_RPC_WS"
const _BSC_MAINNET_RPC_HTTP = "BSC_MAINNET_RPC_HTTP"

const _KAFKA_SERVER = "KAFKA_SERVER"
const _KAFKA_UPDATE_V3_POOLS_TOPIC = "KAFKA_UPDATE_V3_POOLS_TOPIC"

const _REDIS_SERVER = "REDIS_SERVER"

const _ARBITRAGE_GRPC_PORT = "ARBITRAGE_GRPC_PORT"

func load() error {
	godotenv.Load()
	env.BYBIT_API_KEY = os.Getenv(_BYBIT_API_KEY)
	if env.BYBIT_API_KEY == "" {
		return buildLoadingEnvError(_BYBIT_API_KEY)
	}

	env.BYBIT_API_SECRET = os.Getenv(_BYBIT_API_SECRET)
	if env.BYBIT_API_SECRET == "" {
		return buildLoadingEnvError(_BYBIT_API_SECRET)
	}

	env.SUBGRAPH_API_TOKEN = os.Getenv(_SUBGRAPH_API_TOKEN)
	if env.SUBGRAPH_API_TOKEN == "" {
		return buildLoadingEnvError(_SUBGRAPH_API_TOKEN)
	}

	env.POSTGRES_HOST = os.Getenv(_POSTGRES_HOST)
	if env.POSTGRES_HOST == "" {
		return buildLoadingEnvError(_POSTGRES_HOST)
	}

	env.POSTGRES_PORT = os.Getenv(_POSTGRES_PORT)
	if env.POSTGRES_PORT == "" {
		return buildLoadingEnvError(_POSTGRES_PORT)
	}

	env.POSTGRES_DB_NAME = os.Getenv(_POSTGRES_DB_NAME)
	if env.POSTGRES_DB_NAME == "" {
		return buildLoadingEnvError(_POSTGRES_DB_NAME)
	}

	env.POSTGRES_USER = os.Getenv(_POSTGRES_USER)
	if env.POSTGRES_USER == "" {
		return buildLoadingEnvError(_POSTGRES_USER)
	}

	env.POSTGRES_PASSWORD = os.Getenv(_POSTGRES_PASSWORD)
	if env.POSTGRES_PASSWORD == "" {
		return buildLoadingEnvError(_POSTGRES_PASSWORD)
	}

	env.POSTGRES_SSL_MODE = os.Getenv(_POSTGRES_SSL_MODE)
	if env.POSTGRES_SSL_MODE == "" {
		return buildLoadingEnvError(_POSTGRES_SSL_MODE)
	}

	env.ETH_MAINNET_RPC_WS = os.Getenv(_ETH_MAINNET_RPC_WS)
	if env.ETH_MAINNET_RPC_WS == "" {
		return buildLoadingEnvError(_ETH_MAINNET_RPC_WS)
	}
	env.ETH_MAINNET_RPC_HTTP = os.Getenv(_ETH_MAINNET_RPC_HTTP)
	if env.ETH_MAINNET_RPC_HTTP == "" {
		return buildLoadingEnvError(_ETH_MAINNET_RPC_HTTP)
	}

	env.BSC_MAINNET_RPC_WS = os.Getenv(_BSC_MAINNET_RPC_WS)
	if env.BSC_MAINNET_RPC_WS == "" {
		return buildLoadingEnvError(_BSC_MAINNET_RPC_WS)
	}
	env.BSC_MAINNET_RPC_HTTP = os.Getenv(_BSC_MAINNET_RPC_HTTP)
	if env.BSC_MAINNET_RPC_HTTP == "" {
		return buildLoadingEnvError(_BSC_MAINNET_RPC_HTTP)
	}

	env.KAFKA_SERVER = os.Getenv(_KAFKA_SERVER)
	if env.KAFKA_SERVER == "" {
		return buildLoadingEnvError(_KAFKA_SERVER)
	}
	env.KAFKA_UPDATE_V3_POOLS_TOPIC = os.Getenv(_KAFKA_UPDATE_V3_POOLS_TOPIC)
	if env.KAFKA_UPDATE_V3_POOLS_TOPIC == "" {
		return buildLoadingEnvError(_KAFKA_UPDATE_V3_POOLS_TOPIC)
	}

	env.REDIS_SERVER = os.Getenv(_REDIS_SERVER)
	if env.REDIS_SERVER == "" {
		return buildLoadingEnvError(_REDIS_SERVER)
	}

	arbGRPCPortStr := os.Getenv(_ARBITRAGE_GRPC_PORT)
	arbGRPCPort, err := strconv.Atoi(arbGRPCPortStr)
	if err != nil {
		return buildLoadingEnvError(_ARBITRAGE_GRPC_PORT)
	}
	env.ARBITRAGE_GRPC_PORT = uint(arbGRPCPort)

	if env.REDIS_SERVER == "" {
		return buildLoadingEnvError(_ARBITRAGE_GRPC_PORT)
	}
	return nil
}

func buildLoadingEnvError(key string) error {
	return fmt.Errorf("error with variable: %s", key)
}
