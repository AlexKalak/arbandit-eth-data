package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/src/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/src/merging"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/src/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/src/services/arbitrageservice"
	"github.com/alexkalak/go_market_analyze/src/services/poolcacheservice"
)

func main() {
	configure()
}

func configure() {

	// env, err := envhelper.GetEnv()
	// if err != nil {
	// 	panic(err)
	// }
	blockNumber := big.NewInt(23656921)
	var chainID uint = 1
	// amount, ok := new(big.Int).SetString("50392341407227568", 10)
	// if !ok {
	// 	panic("Wrong amount")
	// }
	// testV3Swap("0xd3bc30079210bef8a1f9c7c21e3c5beccfc1dfcb", chainID, amount)

	// v3poolsDBRepo, err := v3poolsrepo.NewDBRepo()
	// if err != nil {
	// 	panic(err)
	// }
	// startArbService(chainID, blockNumber)
	//

	merger, err := merging.New()
	if err != nil {
		panic(err)
	}

	// merger.MergeV2PairsNotFilled(chainID)
	// merger.MergeV2PairsDataAndUpdateUSDPriceForTokens(chainID, blockNumber)
	// validateV2(chainID)
	// merger.MergeExchangeNamesForV2Pairs(chainID)

	// merger.MergeV3PoolsNotFilled(chainID)
	merger.MergeV3PoolsData(chainID, blockNumber)
	// merger.MergeV3PoolsTicks(chainID, blockNumber)
	// validateV3(chainID)
	// merger.MergeExchangeNamesForV3Pools(chainID)

	// rpcEventCollectorServiceDependencies := rpceventscollectorservice.RPCEventCollectorServiceDependencies{
	// 	Env:        env,
	// 	V3PoolRepo: v3poolsDBRepo,
	// }
	//
	// _, err = rpceventscollectorservice.New(chainID, rpcEventCollectorServiceDependencies)
	//
	// if err != nil {
	// 	panic(err)
	// }
	// go rpcEventsCollectorService.Start(context.Background())

	// poolUpdaterServiceDependencies := poolupdaterservice.PoolUpdaterServiceDependencies{
	// 	Env:                env,
	// 	V3PoolDBRepo:       v3poolsDBRepo,
	// 	V3PoolCacheService: poolCacheService,
	// }
	//
	// poolUpdaterService, err := poolupdaterservice.New(chainID, poolUpdaterServiceDependencies)
	// if err != nil {
	// 	panic(err)
	// }
	//

	// poolUpdaterService.Start(context.Background())

}

func startArbService(chainID uint, blockNumber *big.Int) {
	env, err := envhelper.GetEnv()
	if err != nil {
		panic(err)
	}

	v3poolsDBRepo, err := v3poolsrepo.NewDBRepo()
	if err != nil {
		panic(err)
	}

	pools, err := v3poolsDBRepo.GetPoolsByChainID(chainID)
	if err != nil {
		panic(err)
	}

	v3poolsCacheRepo, err := v3poolsrepo.NewCacheRepo(context.Background())
	if err != nil {
		panic(err)
	}

	poolCacheServiceDependencies := poolcacheservice.V3PoolCacheServiceDependencies{
		CacheRepo: v3poolsCacheRepo,
	}

	poolCacheService, err := poolcacheservice.Init(chainID, uint64(blockNumber.Int64()), pools, poolCacheServiceDependencies)
	if err != nil {
		panic(err)
	}

	tokenRepo, err := tokenrepo.New()
	if err != nil {
		panic(err)
	}

	v2pairsRepo, err := v2pairsrepo.New()
	if err != nil {
		panic(err)
	}
	arbServiceDependencies := arbitrageservice.ArbitrageServiceDependencies{
		Env:                env,
		TokenRepo:          tokenRepo,
		V2PairRepo:         v2pairsRepo,
		V3PoolCacheService: poolCacheService,
	}
	arbService, err := arbitrageservice.New(chainID, arbServiceDependencies)
	if err != nil {
		panic(err)
	}
	arbService.Start(context.Background())

}

func validateV2(chainID uint) {
	merger, err := merging.New()
	if err != nil {
		panic(err)
	}
	v2pairsRepo, err := v2pairsrepo.New()
	if err != nil {
		panic(err)
	}
	pairs, err := v2pairsRepo.GetPairsByChainID(chainID)
	if err != nil {
		panic(err)
	}
	merger.ValidateV2PairsAndComputeAvergeUSDPrice(pairs, chainID)
}

func validateV3(chainID uint) {
	merger, err := merging.New()
	if err != nil {
		panic(err)
	}
	v3poolsRepo, err := v3poolsrepo.NewDBRepo()
	if err != nil {
		panic(err)
	}
	pools, err := v3poolsRepo.GetPoolsByChainID(chainID)
	if err != nil {
		panic(err)
	}
	merger.ValidateV3PoolsAndComputeAverageUSDPrice(pools, chainID)
}

func testV2Swap(pairAddress string, chainID uint, amount *big.Int) {
	tokensRepo, err := tokenrepo.New()
	if err != nil {
		panic(err)
	}
	tokens, err := tokensRepo.GetTokens()
	if err != nil {
		panic(err)
	}
	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		tokensMap[token.Address] = &token
	}
	v2pairsDBRepo, err := v2pairsrepo.New()
	if err != nil {
		panic(err)
	}

	pairs, err := v2pairsDBRepo.GetPairsByChainID(chainID)
	if err != nil {
		panic(err)
	}
	pair := models.UniswapV2Pair{}
	for _, pairFromDB := range pairs {
		if pairFromDB.Address == pairAddress {
			pair = pairFromDB
		}
	}

	token0, ok1 := tokensMap[pair.Token0]
	token1, ok2 := tokensMap[pair.Token1]
	if !ok1 || !ok2 {
		panic("IDI NAHER")
	}

	v2Ex, err := v2pairexchangable.New(&pair, token0, token1)
	if err != nil {
		panic(err)
	}

	out, err := v2Ex.ImitateSwap(amount, false)
	if err != nil {
		panic(err)
	}

	fmt.Println(new(big.Float).SetInt(out).String())
}

func testV3Swap(poolAddress string, chainID uint, amount *big.Int) {
	tokensRepo, err := tokenrepo.New()
	if err != nil {
		panic(err)
	}
	tokens, err := tokensRepo.GetTokens()
	if err != nil {
		panic(err)
	}
	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		tokensMap[token.Address] = &token
	}
	v3poolsDBRepo, err := v3poolsrepo.NewDBRepo()
	if err != nil {
		panic(err)
	}

	pool, err := v3poolsDBRepo.GetPoolByPoolIdentificator(models.V3PoolIdentificator{Address: poolAddress, ChainID: chainID})
	if err != nil {
		panic(err)
	}

	token0, ok1 := tokensMap[pool.Token0]
	token1, ok2 := tokensMap[pool.Token1]
	if !ok1 || !ok2 {
		fmt.Println("wrong pool")
		return
	}
	v3Ex, err := v3poolexchangable.NewV3ExchangablePool(&pool, token0, token1)
	if err != nil {
		fmt.Println("wrong pool exchangable")
		return
	}

	initAmount := big.NewInt(1000)

	token0AmountForOneUSD := new(big.Float).Quo(big.NewFloat(1), token0.DefiUSDPrice)
	token0AmountNeeded := new(big.Float).Mul(token0AmountForOneUSD, new(big.Float).SetInt(initAmount))

	amount0 := new(big.Float).Mul(
		new(big.Float).SetInt(
			new(big.Int).Exp(
				big.NewInt(10),
				big.NewInt(int64(token0.Decimals)),
				nil,
			),
		), token0AmountNeeded)

	amountInt0, _ := amount0.Int(nil)
	if amountInt0 == nil {
		fmt.Println("Bad amount")
		return
	}
	fmt.Printf("Imitating %s (%s) -> (%s): %s \n", pool.Address, token0.Symbol, token1.Symbol, amountInt0)
	_, err = v3Ex.ImitateSwap(amountInt0, true)
	if err != nil {
		fmt.Println("Error zfo imitating: ", err, "\n\n")
		return
	}
	fmt.Println("End Imitating ==========================\n\n")

	token1AmountForOneUSD := new(big.Float).Quo(big.NewFloat(1), token1.DefiUSDPrice)
	token1AmountNeeded := new(big.Float).Mul(token1AmountForOneUSD, new(big.Float).SetInt(initAmount))

	amount1 := new(big.Float).Mul(
		new(big.Float).SetInt(
			new(big.Int).Exp(
				big.NewInt(10),
				big.NewInt(int64(token1.Decimals)),
				nil,
			),
		), token1AmountNeeded)

	amountInt1, _ := amount1.Int(nil)
	if amountInt1 == nil {
		fmt.Println("Bad amount")
		return
	}

	fmt.Printf("Imitating %s (%s) -> (%s): %s \n", pool.Address, token1.Symbol, token0.Symbol, amountInt1)
	_, err = v3Ex.ImitateSwap(amountInt1, false)
	if err != nil {
		fmt.Println("Error !zfo imitating: ", err, "\n\n")
		return
	}
	fmt.Println("End Imitating ==========================\n\n")

}
