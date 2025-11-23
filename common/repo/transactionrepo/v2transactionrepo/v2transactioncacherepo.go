package v2transactionrepo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/redisdb"
	"github.com/redis/go-redis/v9"
)

const SWAP_STREAM = "v2swaps"

func getV2TransactionStreamByChainID(chainID uint) string {
	return fmt.Sprintf("%d_%s", chainID, SWAP_STREAM)
}

type V2TransactionCacheRepo interface {
	StreamSwap(transaction models.V2Swap) error
}

type V2TransacationCacheRepoConfig struct {
	RedisServer string
}

type v2transactionCacheRepo struct {
	redisDB *redisdb.RedisDatabase
	ctx     context.Context
}

func NewCacheRepo(ctx context.Context, config V2TransacationCacheRepoConfig) (V2TransactionCacheRepo, error) {
	redisDatabase, err := redisdb.New(redisdb.RedisDatabaseConfig{
		RedisServer: config.RedisServer,
	})
	if err != nil {
		return &v2transactionCacheRepo{}, err
	}

	return &v2transactionCacheRepo{
		redisDB: redisDatabase,
		ctx:     ctx,
	}, nil
}

func (r *v2transactionCacheRepo) StreamSwap(transaction models.V2Swap) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return nil
	}

	// fmt.Println("Adding in cache: ", getV3TransactionStreamByChainID(transaction.ChainID), transaction)
	formattedTransaction, err := json.Marshal(&transaction)
	if err != nil {
		return err
	}

	res := rdb.XAdd(r.ctx, &redis.XAddArgs{
		Stream: getV2TransactionStreamByChainID(transaction.ChainID),
		Values: map[string]any{
			"swap": formattedTransaction,
		},
	})
	if res.Err() != nil {
		fmt.Println(err)
		return res.Err()
	}

	return nil
}
