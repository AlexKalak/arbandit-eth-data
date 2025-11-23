package v2pairsrepo

import (
	"context"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/redisdb"
)

const PAIRS_HASH = "v3pairs"

func getPairsHashByChainID(chainID uint) string {
	return fmt.Sprintf("%d.%s", chainID, PAIRS_HASH)
}

const BLOCK_NUMBER_KEY = "block_number"

type V2PairCacheRepo interface {
	GetPairs(chainID uint) ([]models.UniswapV2Pair, error)
	GetNonDustyPairs(chainID uint) ([]models.UniswapV2Pair, error)

	GetPairByIdentificator(pairIdentificator models.V2PairIdentificator) (models.UniswapV2Pair, error)

	SetPairs(chainID uint, pairs []models.UniswapV2Pair) error
	SetPair(pair models.UniswapV2Pair) error
	SetBlockNumber(chainID uint, blockNumber uint64) error

	ClearPairs(chainID uint) error
}

type V2PairCacheRepoConfig struct {
	RedisServer string
}

type v2pairCacheRepo struct {
	redisDB *redisdb.RedisDatabase
	ctx     context.Context
}

func NewCacheRepo(ctx context.Context, config V2PairCacheRepoConfig) (V2PairCacheRepo, error) {
	redisDatabase, err := redisdb.New(redisdb.RedisDatabaseConfig{
		RedisServer: config.RedisServer,
	})
	if err != nil {
		return &v2pairCacheRepo{}, err
	}

	return &v2pairCacheRepo{
		redisDB: redisDatabase,
		ctx:     ctx,
	}, nil
}

func (r *v2pairCacheRepo) GetPairs(chainID uint) ([]models.UniswapV2Pair, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return nil, err
	}

	resp := rdb.HGetAll(r.ctx, getPairsHashByChainID(chainID))
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	pairsMap, err := resp.Result()
	if err != nil {
		return nil, err
	}

	pairs := make([]models.UniswapV2Pair, 0, len(pairsMap))
	for _, pairStr := range pairsMap {
		pair := models.UniswapV2Pair{}
		err = pair.FillFromJSON([]byte(pairStr))
		if err != nil {
			continue
		}

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairCacheRepo) GetNonDustyPairs(chainID uint) ([]models.UniswapV2Pair, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return nil, err
	}

	resp := rdb.HGetAll(r.ctx, getPairsHashByChainID(chainID))
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	pairsMap, err := resp.Result()
	if err != nil {
		return nil, err
	}

	pairs := make([]models.UniswapV2Pair, 0, len(pairsMap))
	for _, pairStr := range pairsMap {
		pair := models.UniswapV2Pair{}
		err = pair.FillFromJSON([]byte(pairStr))
		if err != nil {
			continue
		}

		if pair.IsDusty {
			continue
		}

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairCacheRepo) GetPairByIdentificator(pairIdentificator models.V2PairIdentificator) (models.UniswapV2Pair, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return models.UniswapV2Pair{}, err
	}

	resp := rdb.HGet(r.ctx, getPairsHashByChainID(pairIdentificator.ChainID), pairIdentificator.String())
	if resp.Err() != nil {
		return models.UniswapV2Pair{}, resp.Err()
	}
	pairStr, err := resp.Result()
	if err != nil {
		return models.UniswapV2Pair{}, err
	}

	pair := models.UniswapV2Pair{}
	err = pair.FillFromJSON([]byte(pairStr))
	if err != nil {
		return models.UniswapV2Pair{}, err
	}

	return pair, nil
}

func (r *v2pairCacheRepo) SetPairs(chainID uint, pairs []models.UniswapV2Pair) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	pairsForRedis := make([]string, 0, len(pairs)*2)
	for _, pair := range pairs {
		pairIdentificator := pair.GetIdentificator().String()
		pairJSON, err := pair.GetJSON()
		if err != nil {
			return err
		}

		pairsForRedis = append(pairsForRedis, pairIdentificator)
		pairsForRedis = append(pairsForRedis, string(pairJSON))
	}

	resp := rdb.HSet(r.ctx, getPairsHashByChainID(chainID), pairsForRedis)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

func (r *v2pairCacheRepo) SetPair(pair models.UniswapV2Pair) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	pairIdentificator := pair.GetIdentificator().String()
	pairJSON, err := pair.GetJSON()
	if err != nil {
		return err
	}

	resp := rdb.HSet(r.ctx, getPairsHashByChainID(pair.ChainID), pairIdentificator, pairJSON)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

func (r *v2pairCacheRepo) SetBlockNumber(chainID uint, blockNumber uint64) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	resp := rdb.HSet(r.ctx, getPairsHashByChainID(chainID), BLOCK_NUMBER_KEY, blockNumber)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

func (r *v2pairCacheRepo) ClearPairs(chainID uint) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	resp := rdb.Del(r.ctx, getPairsHashByChainID(chainID))
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}
