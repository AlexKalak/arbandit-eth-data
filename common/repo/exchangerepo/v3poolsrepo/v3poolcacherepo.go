package v3poolsrepo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/redisdb"
)

const POOLS_HASH = "v3pools"

func getPoolsHashByChainID(chainID uint) string {
	return fmt.Sprintf("%d.%s", chainID, POOLS_HASH)
}

const BLOCK_NUMBER_KEY = "block_number"

type V3PoolCacheRepo interface {
	GetPools(chainID uint) ([]models.UniswapV3Pool, error)
	GetNonDustyPools(chainID uint) ([]models.UniswapV3Pool, error)

	GetPoolByIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error)

	SetPools(chainID uint, pools []models.UniswapV3Pool) error
	SetPool(pool models.UniswapV3Pool) error
	SetBlockNumber(chainID uint, blockNumber uint64) error

	ClearPools(chainID uint) error
}

type V3PoolCacheRepoConfig struct {
	RedisServer string
}

type v3poolCacheRepo struct {
	redisDB *redisdb.RedisDatabase
	ctx     context.Context
}

func NewCacheRepo(ctx context.Context, config V3PoolCacheRepoConfig) (V3PoolCacheRepo, error) {
	redisDatabase, err := redisdb.New(redisdb.RedisDatabaseConfig{
		RedisServer: config.RedisServer,
	})
	if err != nil {
		return &v3poolCacheRepo{}, err
	}

	return &v3poolCacheRepo{
		redisDB: redisDatabase,
		ctx:     ctx,
	}, nil
}

func (r *v3poolCacheRepo) GetPools(chainID uint) ([]models.UniswapV3Pool, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return nil, err
	}

	resp := rdb.HGetAll(r.ctx, getPoolsHashByChainID(chainID))
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	poolsMap, err := resp.Result()
	if err != nil {
		return nil, err
	}

	pools := make([]models.UniswapV3Pool, 0, len(poolsMap))
	for _, poolStr := range poolsMap {
		pool := models.UniswapV3Pool{}
		err = json.Unmarshal([]byte(poolStr), &pool)
		if err != nil {
			return nil, err
		}

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolCacheRepo) GetNonDustyPools(chainID uint) ([]models.UniswapV3Pool, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return nil, err
	}

	resp := rdb.HGetAll(r.ctx, getPoolsHashByChainID(chainID))
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	poolsMap, err := resp.Result()
	if err != nil {
		return nil, err
	}

	pools := make([]models.UniswapV3Pool, 0, len(poolsMap))
	for _, poolStr := range poolsMap {
		pool := models.UniswapV3Pool{}
		err = json.Unmarshal([]byte(poolStr), &pool)
		if err != nil {
			return nil, err
		}

		if pool.IsDusty {
			continue
		}

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolCacheRepo) GetPoolByIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error) {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return models.UniswapV3Pool{}, err
	}

	resp := rdb.HGet(r.ctx, getPoolsHashByChainID(poolIdentificator.ChainID), poolIdentificator.String())
	if resp.Err() != nil {
		return models.UniswapV3Pool{}, resp.Err()
	}
	poolStr, err := resp.Result()
	if err != nil {
		return models.UniswapV3Pool{}, err
	}

	pool := models.UniswapV3Pool{}
	err = json.Unmarshal([]byte(poolStr), &pool)
	if err != nil {
		return models.UniswapV3Pool{}, err
	}

	return pool, nil
}

func (r *v3poolCacheRepo) SetPools(chainID uint, pools []models.UniswapV3Pool) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	poolsForRedis := make([]string, 0, len(pools)*2)
	for _, pool := range pools {
		poolIdentificator := pool.GetIdentificator().String()
		poolJSON, err := json.Marshal(&pool)
		if err != nil {
			return err
		}

		poolsForRedis = append(poolsForRedis, poolIdentificator)
		poolsForRedis = append(poolsForRedis, string(poolJSON))
	}

	resp := rdb.HSet(r.ctx, getPoolsHashByChainID(chainID), poolsForRedis)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

func (r *v3poolCacheRepo) SetPool(pool models.UniswapV3Pool) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	poolIdentificator := pool.GetIdentificator().String()
	poolJSON, err := json.Marshal(&pool)
	if err != nil {
		return err
	}

	resp := rdb.HSet(r.ctx, getPoolsHashByChainID(pool.ChainID), poolIdentificator, poolJSON)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

func (r *v3poolCacheRepo) SetBlockNumber(chainID uint, blockNumber uint64) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	resp := rdb.HSet(r.ctx, fmt.Sprint(BLOCK_NUMBER_KEY, getPoolsHashByChainID(chainID)), BLOCK_NUMBER_KEY, blockNumber)
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

func (r *v3poolCacheRepo) ClearPools(chainID uint) error {
	rdb, err := r.redisDB.GetDB()
	if err != nil {
		return err
	}

	resp := rdb.Del(r.ctx, getPoolsHashByChainID(chainID))
	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}
