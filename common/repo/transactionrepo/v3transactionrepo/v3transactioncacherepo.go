package v3transactionrepo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/redisdb"
	"github.com/redis/go-redis/v9"
)

const TRANSACTION_STREAM = "v3transactions"

func getV3TransactionStreamByChainID(chainID uint) string {
	return fmt.Sprintf("%d_%s", chainID, TRANSACTION_STREAM)
}

type V3TransactionCacheRepo interface {
	StreamTransaction(transaction models.V3Transaction) error
}

type V3TransacationCacheRepoConfig struct {
	RedisServer string
}

type v3transactionCacheRepo struct {
	redisDB *redisdb.RedisDatabase
	ctx     context.Context
}

func NewCacheRepo(ctx context.Context, config V3TransacationCacheRepoConfig) (V3TransactionCacheRepo, error) {
	redisDatabase, err := redisdb.New(redisdb.RedisDatabaseConfig{
		RedisServer: config.RedisServer,
	})
	if err != nil {
		return &v3transactionCacheRepo{}, err
	}

	return &v3transactionCacheRepo{
		redisDB: redisDatabase,
		ctx:     ctx,
	}, nil
}

func (r *v3transactionCacheRepo) StreamTransaction(transaction models.V3Transaction) error {
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
		Stream: getV3TransactionStreamByChainID(transaction.ChainID),
		Values: map[string]any{
			"transaction": formattedTransaction,
		},
	})
	if res.Err() != nil {
		fmt.Println(err)
		return res.Err()
	}

	return nil
}
