package tokenrepo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/redisdb"
)

const TOKENS_HASH = "tokens"

func getTokensHashByChainID(chainID uint) string {
	return fmt.Sprintf("%d_%s", chainID, TOKENS_HASH)
}

type TokenCacheRepo interface {
	SetToken(token *models.Token) error
}

type TokenCacheRepoConfig struct {
	RedisServer string
}

type tokenCacheRepo struct {
	redisDB *redisdb.RedisDatabase
	ctx     context.Context
}

func NewCacheRepo(ctx context.Context, config TokenCacheRepoConfig) (TokenCacheRepo, error) {
	redisDatabase, err := redisdb.New(redisdb.RedisDatabaseConfig{
		RedisServer: config.RedisServer,
	})
	if err != nil {
		return &tokenCacheRepo{}, err
	}

	return &tokenCacheRepo{
		redisDB: redisDatabase,
		ctx:     ctx,
	}, nil
}

func (r *tokenCacheRepo) SetToken(token *models.Token) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	tokenIdentificator := token.GetIdentificator()
	tokenJSON, err := json.Marshal(&token)
	if err != nil {
		return err
	}

	resp := rdb.HSet(r.ctx, getTokensHashByChainID(token.ChainID), tokenIdentificator, tokenJSON)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}
