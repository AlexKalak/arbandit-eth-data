package arbitrageservice

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/core/exchangegraph"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
)

type ArbitrageService interface {
	Start(ctx context.Context) error
}

type arbitrageService struct {
	currentCheckingBlock uint64
	chainID              uint
	exchangeGraph        exchangegraph.ExchangesGraph
}

type ArbitrageServiceDependencies struct {
	TokenRepo       tokenrepo.TokenRepo
	V3PoolCacheRepo v3poolsrepo.V3PoolCacheRepo
	V2PairRepo      v2pairsrepo.V2PairRepo
}

func (d *ArbitrageServiceDependencies) validate() error {
	if d.TokenRepo == nil {
		return errors.New("arbitrage service dependencies token repo cannot be nil")

	}
	if d.V3PoolCacheRepo == nil {
		return errors.New("arbitrage service dependencies token repo cannot be nil")

	}
	// if d.V2PairRepo== nil {
	// 	return errors.New("arbitrage service dependencies token repo cannot be nil")
	//
	// }
	return nil
}

func New(chainID uint, dependencies ArbitrageServiceDependencies) (ArbitrageService, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	tokens, err := dependencies.TokenRepo.GetTokens()
	if err != nil {
		panic(err)
	}
	tokenIDs := map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		tokenIDs[token.GetIdentificator()] = token
	}

	// pairs, err := dependencies.V2PairRepo.GetNonDustyPairsByChainID(chainID)
	// if err != nil {
	// 	panic(err)
	// }

	pools, err := dependencies.V3PoolCacheRepo.GetNonDustyPools(chainID)
	if err != nil {
		panic(err)
	}

	// exchangablesArray := make([]exchangables.Exchangable, 0, len(pairs)+len(pools))
	exchangablesArray := make([]exchangables.Exchangable, 0, len(pools))

	// for _, pair := range pairs {
	// 	token0, ok := tokenIDs[models.TokenIdentificator{Address: pair.Token0, ChainID: chainID}]
	// 	if !ok {
	// 		continue
	// 	}
	// 	token1, ok := tokenIDs[models.TokenIdentificator{Address: pair.Token1, ChainID: chainID}]
	// 	if !ok {
	// 		continue
	// 	}
	//
	// 	v2Exchangable, err := v2pairexchangable.New(
	// 		&pair,
	// 		token0,
	// 		token1,
	// 	)
	// 	if err != nil {
	// 		continue
	// 	}
	//
	// 	exchangablesArray = append(exchangablesArray, &v2Exchangable)
	// }

	for _, pool := range pools {
		token0, ok := tokenIDs[models.TokenIdentificator{Address: pool.Token0, ChainID: chainID}]
		if !ok {
			continue
		}
		token1, ok := tokenIDs[models.TokenIdentificator{Address: pool.Token1, ChainID: chainID}]
		if !ok {
			continue
		}

		v3Exchangable, err := v3poolexchangable.NewV3ExchangablePool(
			&pool,
			token0,
			token1,
		)
		if err != nil {
			continue
		}

		exchangablesArray = append(exchangablesArray, &v3Exchangable)
	}

	fmt.Println("Len exchangables: ", len(exchangablesArray))

	exGraphDependencies := exchangegraph.ExchangeGraphDependencies{
		TokenRepo: dependencies.TokenRepo,
	}

	exchangeGraph, err := exchangegraph.New(exchangablesArray, exGraphDependencies)
	if err != nil {
		panic(err)
	}

	return &arbitrageService{
		exchangeGraph: exchangeGraph,
		chainID:       chainID,
	}, nil
}

func (s *arbitrageService) Start(ctx context.Context) error {
	s.exchangeGraph.FindAllArbs(3, big.NewInt(int64(s.chainID)))
	return nil
}
