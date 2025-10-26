package poolcacheservice

import (
	"github.com/alexkalak/go_market_analyze/src/errors/poolcacheerrors"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v3poolsrepo"
)

type V3PoolCacheService interface {
	GetPools(chainID uint) ([]models.UniswapV3Pool, error)
	GetNonDustyPools(chainID uint) ([]models.UniswapV3Pool, error)
	GetPoolByIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error)
	SetPools(chainID uint, pools []models.UniswapV3Pool) error
}

type v3PoolCacheService struct {
	cacheRepo v3poolsrepo.V3PoolCacheRepo
}

type V3PoolCacheServiceDependencies struct {
	CacheRepo v3poolsrepo.V3PoolCacheRepo
}

var singleton V3PoolCacheService

func Init(chainID uint, blockNumber uint64, pools []models.UniswapV3Pool, dependencies V3PoolCacheServiceDependencies) (V3PoolCacheService, error) {
	if singleton != nil {
		return singleton, nil
	}

	if dependencies.CacheRepo == nil {
		return nil, poolcacheerrors.ErrInvalidPoolCacheServiceDependencies
	}

	err := dependencies.CacheRepo.ClearPools(chainID)
	if err != nil {
		return nil, err
	}

	err = dependencies.CacheRepo.SetBlockNumber(chainID, blockNumber)
	if err != nil {
		return nil, err
	}

	if len(pools) > 0 {
		err = dependencies.CacheRepo.SetPools(chainID, pools)
		if err != nil {
			return nil, err
		}

	}

	singleton = &v3PoolCacheService{
		cacheRepo: dependencies.CacheRepo,
	}
	return singleton, nil
}

func (r *v3PoolCacheService) GetPools(chainID uint) ([]models.UniswapV3Pool, error) {
	return r.cacheRepo.GetPools(chainID)
}

func (r *v3PoolCacheService) GetNonDustyPools(chainID uint) ([]models.UniswapV3Pool, error) {
	return r.cacheRepo.GetNonDustyPools(chainID)
}

func (r *v3PoolCacheService) GetPoolByIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error) {
	return r.cacheRepo.GetPoolByIdentificator(poolIdentificator)
}

func (r *v3PoolCacheService) SetPools(chainID uint, pools []models.UniswapV3Pool) error {
	return r.cacheRepo.SetPools(chainID, pools)
}
