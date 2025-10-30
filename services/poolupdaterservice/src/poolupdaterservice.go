package poolupdaterservice

import (
	"context"
	"errors"
	"math"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
)

type PoolUpdaterService interface {
	Start(ctx context.Context) error
}

type PoolUpdaterServiceDependencies struct {
	V3PoolDBRepo    v3poolsrepo.V3PoolDBRepo
	V3PoolCacheRepo v3poolsrepo.V3PoolCacheRepo
}

func (d *PoolUpdaterServiceDependencies) validate() error {
	if d.V3PoolDBRepo == nil {
		return errors.New("pool updater service dependencies V3PoolDBRepo cannot be nil")
	}

	if d.V3PoolCacheRepo == nil {

		return errors.New("pool updater service dependencies V3PoolCacheRepo cannot be nil")
	}

	return nil
}

type PoolUpdaterServiceConfig struct {
	ChainID                 uint
	KafkaServer             string
	KafkaUpdateV3PoolsTopic string
}

func (d *PoolUpdaterServiceConfig) validate() error {
	if d.ChainID == 0 {
		return errors.New("pool updater service config ChainID not set")
	}

	if d.KafkaServer == "" {
		return errors.New("pool updater service config KafkaServer not set")
	}

	if d.KafkaUpdateV3PoolsTopic == "" {
		return errors.New("pool updater service config KafkaUpdateV3PoolsTopic not set")
	}

	return nil
}

type poolUpdaterService struct {
	dbRepo    v3poolsrepo.V3PoolDBRepo
	cacheRepo v3poolsrepo.V3PoolCacheRepo

	currentCheckingBlock    uint64
	currentBlockPoolChanges map[models.V3PoolIdentificator]models.UniswapV3Pool

	config PoolUpdaterServiceConfig

	v3PoolDBRepo    v3poolsrepo.V3PoolDBRepo
	v3PoolCacheRepo v3poolsrepo.V3PoolCacheRepo
}

func New(config PoolUpdaterServiceConfig, dependencies PoolUpdaterServiceDependencies) (PoolUpdaterService, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	service := poolUpdaterService{
		config:                  config,
		v3PoolDBRepo:            dependencies.V3PoolDBRepo,
		v3PoolCacheRepo:         dependencies.V3PoolCacheRepo,
		currentCheckingBlock:    0,
		currentBlockPoolChanges: map[models.V3PoolIdentificator]models.UniswapV3Pool{},
	}

	pools, err := service.v3PoolDBRepo.GetPoolsByChainID(config.ChainID)
	if err != nil {
		return nil, err
	}

	service.ConfigureCache(pools)

	return &service, nil
}

func (s *poolUpdaterService) ConfigureCache(pools []models.UniswapV3Pool) error {
	var minBlockNumber int64 = math.MaxInt64
	for _, pool := range pools {
		if int64(pool.BlockNumber) < minBlockNumber {
			minBlockNumber = int64(pool.BlockNumber)
		}
	}

	err := s.v3PoolCacheRepo.ClearPools(s.config.ChainID)
	if err != nil {
		return err
	}

	if len(pools) > 0 {
		err = s.v3PoolCacheRepo.SetPools(s.config.ChainID, pools)
		if err != nil {
			return err
		}

	}

	return nil
}
