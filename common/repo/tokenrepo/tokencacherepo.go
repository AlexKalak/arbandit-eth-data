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
	SetTokens(chainID uint, tokens []*models.Token) error
	SetToken(token *models.Token) error
	GetTokens(chainID uint) ([]*models.Token, error)
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

func (r *tokenCacheRepo) SetTokens(chainID uint, tokens []*models.Token) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	tokensForRedis := make([]string, 0, len(tokens)*2)
	for _, token := range tokens {
		tokenIdentificator := token.GetIdentificator()
		tokenJSON, err := json.Marshal(token)
		if err != nil {
			return err
		}

		tokensForRedis = append(tokensForRedis, tokenIdentificator.String())
		tokensForRedis = append(tokensForRedis, string(tokenJSON))
	}

	resp := rdb.HSet(r.ctx, getTokensHashByChainID(chainID), tokensForRedis)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
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

func (r *tokenCacheRepo) GetTokens(chainID uint) ([]*models.Token, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return nil, err
	}

	resp := rdb.HGetAll(r.ctx, getTokensHashByChainID(chainID))
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	tokensMap, err := resp.Result()
	if err != nil {
		return nil, err
	}

	tokens := make([]*models.Token, 0, len(tokensMap))
	for _, tokenStr := range tokensMap {
		token := &models.Token{}
		err = json.Unmarshal([]byte(tokenStr), &token)
		if err != nil {
			continue
		}

		tokens = append(tokens, token)
	}

	return tokens, nil
}
