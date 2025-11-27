package arbitrageservice

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/core/exchangegraph"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v2transactionrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
)

type ArbitrageService interface {
	FindOldArbs() error
	FindAllArbs(chainID uint) ([]Arbitrage, error)
	Start(ctx context.Context) error
}

type arbitrageService struct {
	chainID       uint
	exchangeGraph exchangegraph.ExchangesGraph

	v3PoolDBRepo    v3poolsrepo.V3PoolDBRepo
	v3PoolCacheRepo v3poolsrepo.V3PoolCacheRepo
	v2PairDBRepo    v2pairsrepo.V2PairDBRepo
	v2PairCacheRepo v2pairsrepo.V2PairCacheRepo

	v3TransactionsDBRepo v3transactionrepo.V3TransactionDBRepo
	v2TransactionsDBRepo v2transactionrepo.V2TransactionDBRepo

	tokenDBRepo    tokenrepo.TokenRepo
	tokenCacheRepo tokenrepo.TokenCacheRepo
}

type ArbitrageServiceDependencies struct {
	TokenRepo      tokenrepo.TokenRepo
	TokenCacheRepo tokenrepo.TokenCacheRepo

	V3PoolCacheRepo v3poolsrepo.V3PoolCacheRepo
	V3PoolDBRepo    v3poolsrepo.V3PoolDBRepo

	V2PairDBRepo    v2pairsrepo.V2PairDBRepo
	V2PairCacheRepo v2pairsrepo.V2PairCacheRepo

	V3TransactionDBRepo  v3transactionrepo.V3TransactionDBRepo
	V2TransactionsDBRepo v2transactionrepo.V2TransactionDBRepo
}

func (d *ArbitrageServiceDependencies) validate() error {
	if d.TokenRepo == nil {
		return errors.New("arbitrage service dependencies token repo cannot be nil")

	}
	if d.TokenCacheRepo == nil {
		return errors.New("arbitrage service dependencies token cache repo cannot be nil")

	}

	if d.V3PoolCacheRepo == nil {
		return errors.New("arbitrage service dependencies token repo cannot be nil")
	}
	if d.V3PoolDBRepo == nil {
		return errors.New("arbitrage service dependencies V3PoolDBRepo cannot be nil")
	}

	if d.V2PairCacheRepo == nil {
		return errors.New("arbitrage service dependencies V2PairCacheRepo cannot be nil")
	}
	if d.V3TransactionDBRepo == nil {
		return errors.New("arbitrage service dependencies V3TransactionRepo cannot be nil")
	}
	if d.V2TransactionsDBRepo == nil {
		return errors.New("arbitrage service dependencies V2TransactionDBRepo cannot be nil")
	}

	return nil
}

func New(chainID uint, dependencies ArbitrageServiceDependencies) (ArbitrageService, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	service := &arbitrageService{
		chainID:              chainID,
		v3PoolDBRepo:         dependencies.V3PoolDBRepo,
		v3PoolCacheRepo:      dependencies.V3PoolCacheRepo,
		v2PairDBRepo:         dependencies.V2PairDBRepo,
		v2PairCacheRepo:      dependencies.V2PairCacheRepo,
		v3TransactionsDBRepo: dependencies.V3TransactionDBRepo,
		v2TransactionsDBRepo: dependencies.V2TransactionsDBRepo,
		tokenDBRepo:          dependencies.TokenRepo,
		tokenCacheRepo:       dependencies.TokenCacheRepo,
	}
	err := service.updateGraph()
	if err != nil {
		return nil, err
	}

	return service, nil
}

type oldArb struct {
	t          int
	amountIn   *big.Int
	amountOut  *big.Int
	pathString string
	txHash     string
	tokens     []string
}

type TradesChainNode struct {
	TradeID       string
	NextTradeNode *TradesChainNode
	PrevTradeNode *TradesChainNode
}

func (s *arbitrageService) Start(ctx context.Context) error {
	ticker := time.NewTicker(4 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return errors.New("arbitrage service ctx done")
		case <-ticker.C:
			fmt.Println("-------SEARCHING-------")
			s.updateGraph()
			s.FindAllArbs(s.chainID)
			fmt.Println("-------END SEARCHING-------\n\n")
		}
	}
}

func (s *arbitrageService) updateGraph() error {
	tokens, err := s.tokenDBRepo.GetTokensByChainID(s.chainID)
	if err != nil {
		panic(err)
	}
	tokenIDs := map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		tokenIDs[token.GetIdentificator()] = token
	}

	pools, err := s.v3PoolDBRepo.GetNotDustyPoolsByChainID(s.chainID)
	if err != nil {
		panic(err)
	}
	pairs, err := s.v2PairDBRepo.GetNonDustyPairsByChainID(s.chainID)
	if err != nil {
		panic(err)
	}

	exchangablesArray := make([]exchangables.Exchangable, 0, len(pools))

	for _, pool := range pools {
		token0, ok := tokenIDs[models.TokenIdentificator{Address: pool.Token0, ChainID: s.chainID}]
		if !ok {
			continue
		}
		token1, ok := tokenIDs[models.TokenIdentificator{Address: pool.Token1, ChainID: s.chainID}]
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
	for _, pair := range pairs {
		token0, ok := tokenIDs[models.TokenIdentificator{Address: pair.Token0, ChainID: s.chainID}]
		if !ok {
			continue
		}
		token1, ok := tokenIDs[models.TokenIdentificator{Address: pair.Token1, ChainID: s.chainID}]
		if !ok {
			continue
		}

		v2Exchangable, err := v2pairexchangable.NewV2ExchangablePair(
			&pair,
			token0,
			token1,
		)
		if err != nil {
			continue
		}

		exchangablesArray = append(exchangablesArray, &v2Exchangable)
	}

	fmt.Println("Len exchangables: ", len(exchangablesArray))

	exGraphDependencies := exchangegraph.ExchangeGraphDependencies{}

	exchangeGraph, err := exchangegraph.New(exchangablesArray, exGraphDependencies)
	if err != nil {
		return err
	}

	s.exchangeGraph = exchangeGraph
	return nil
}

type ArbitragePathUnit struct {
	TokenInAddress  string
	TokenOutAddress string
	PoolAddress     string
	AmountIn        *big.Int
	AmountOut       *big.Int
}

type Arbitrage struct {
	Path []ArbitragePathUnit
}

func (s *arbitrageService) FindAllArbs(chainID uint) ([]Arbitrage, error) {
	err := s.updateGraph()
	if err != nil {
		return nil, err
	}

	arbs, err := s.exchangeGraph.FindAllArbs(3, big.NewInt(10))
	if err != nil {
		return nil, err
	}
	slices.SortFunc(arbs, func(a1, a2 exchangegraph.Arbitrage) int {
		return new(big.Float).Sub(a2.ResultUSD, a1.ResultUSD).Sign()
	})
	fmt.Println("Len arbs: ", len(arbs))
	if len(arbs) > 0 {
		fmt.Println("best: ", arbs[0])
		fmt.Println("worst: ", arbs[len(arbs)-1])
	}

	usedExchangables := map[string]any{}
	uniqueArbs := map[string]exchangegraph.Arbitrage{}

arbLoop:
	for _, arb := range arbs {
		for _, edge := range arb.UsedEdges {
			if _, ok := usedExchangables[edge.Exchangable.Address()]; ok {
				continue arbLoop
			}
		}
		for _, edge := range arb.UsedEdges {
			usedExchangables[edge.Exchangable.Address()] = new(any)
		}

		outAmount := arb.Amounts[len(arb.Amounts)-1]

		if prevArb, ok := uniqueArbs[arb.UsedEdges[0].Exchangable.Address()]; ok {
			prevAmount := prevArb.Amounts[len(prevArb.Amounts)-1]
			if prevAmount.Cmp(outAmount) >= 0 {
				continue
			}
		}
		uniqueArbs[arb.UsedEdges[0].Exchangable.Address()] = arb
	}

	for _, arb := range uniqueArbs {
		if len(arb.UsedEdges) == 0 {
			continue
		}

		for i, hop := range arb.Hops {
			token, err := s.exchangeGraph.GetTokenByIndex(hop)
			if err != nil {
				fmt.Println("=====FAILED====")
				break
			}

			if i == 0 {
				fmt.Printf(" -> %s - %s \n", arb.Amounts[i], token.Symbol)
			} else {
				edge := arb.UsedEdges[i-1]
				fmt.Printf(" -> %s %s - %s \n", arb.Amounts[i], token.Symbol, edge.Exchangable.Address())
			}
		}
		fmt.Printf(" RESULT USD: %s$ \n", arb.ResultUSD.Text('f', -1))
		fmt.Println("")

	}

	res := []Arbitrage{}

	for _, arb := range arbs {
		resArb := Arbitrage{
			Path: []ArbitragePathUnit{},
		}
		for i, edge := range arb.UsedEdges {
			pathUnitRes := ArbitragePathUnit{
				PoolAddress:     edge.Exchangable.Address(),
				TokenInAddress:  edge.Exchangable.GetToken0().Address,
				TokenOutAddress: edge.Exchangable.GetToken1().Address,
				AmountIn:        arb.Amounts[i],
				AmountOut:       arb.Amounts[i+1],
			}

			if !edge.Zfo {
				pathUnitRes = ArbitragePathUnit{
					PoolAddress:     edge.Exchangable.Address(),
					TokenInAddress:  edge.Exchangable.GetToken1().Address,
					TokenOutAddress: edge.Exchangable.GetToken0().Address,
					AmountIn:        arb.Amounts[i],
					AmountOut:       arb.Amounts[i+1],
				}
			}

			resArb.Path = append(resArb.Path, pathUnitRes)
		}

		res = append(res, resArb)
	}

	return res, nil
}
