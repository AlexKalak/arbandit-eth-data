package arbitrageservice

import (
	"context"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/src/core/exchangables"
	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/src/core/exchangegraph"
	"github.com/alexkalak/go_market_analyze/src/errors/arberrors"
	"github.com/alexkalak/go_market_analyze/src/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/src/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/src/services/poolcacheservice"
)

type ArbitrageService interface {
	Start(ctx context.Context) error
}

type arbitrageService struct {
	env                  *envhelper.Environment
	currentCheckingBlock uint64
	chainID              uint
	exchangeGraph        exchangegraph.ExchangesGraph
}

type ArbitrageServiceDependencies struct {
	Env                *envhelper.Environment
	TokenRepo          tokenrepo.TokenRepo
	V3PoolCacheService poolcacheservice.V3PoolCacheService
	V2PairRepo         v2pairsrepo.V2PairRepo
}

func New(chainID uint, dependencies ArbitrageServiceDependencies) (ArbitrageService, error) {
	if dependencies.Env == nil ||
		dependencies.TokenRepo == nil ||
		dependencies.V2PairRepo == nil ||
		dependencies.V3PoolCacheService == nil {
		return nil, arberrors.ErrInvalidArbitrageServiceDependencies
	}

	tokens, err := dependencies.TokenRepo.GetTokens()
	if err != nil {
		panic(err)
	}
	tokenIDs := map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		tokenIDs[token.GetIdentificator()] = &token
	}

	pairs, err := dependencies.V2PairRepo.GetNonDustyPairsByChainID(chainID)
	if err != nil {
		panic(err)
	}
	pools, err := dependencies.V3PoolCacheService.GetNonDustyPools(chainID)
	if err != nil {
		panic(err)
	}

	exchangablesArray := make([]exchangables.Exchangable, 0, len(pairs)+len(pools))

	for _, pair := range pairs {
		token0, ok := tokenIDs[models.TokenIdentificator{Address: pair.Token0, ChainID: chainID}]
		if !ok {
			continue
		}
		token1, ok := tokenIDs[models.TokenIdentificator{Address: pair.Token1, ChainID: chainID}]
		if !ok {
			continue
		}

		v2Exchangable, err := v2pairexchangable.New(
			&pair,
			token0,
			token1,
		)
		if err != nil {
			continue
		}

		exchangablesArray = append(exchangablesArray, &v2Exchangable)
	}

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
		env:           dependencies.Env,
		exchangeGraph: exchangeGraph,
		chainID:       chainID,
	}, nil
}

func (s *arbitrageService) Start(ctx context.Context) error {
	s.exchangeGraph.FindAllArbs(4, big.NewInt(int64(s.chainID)))
	return nil
}
