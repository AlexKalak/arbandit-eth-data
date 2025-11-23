package poolupdaterservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

type swapEventData struct {
	Sender       common.Address `json:"sender"`
	Recipient    common.Address `json:"repcipient"`
	Amount0      *big.Int       `json:"amount0"`
	Amount1      *big.Int       `json:"amount1"`
	SqrtPriceX96 *big.Int       `json:"sqrt_price_x96"`
	Liquidity    *big.Int       `json:"liquidity"`
	Tick         *big.Int       `json:"tick"`
}

type mintEventData struct {
	Sender    common.Address `json:"sender"`
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type burnEventData struct {
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type poolEventData[T swapEventData | mintEventData | burnEventData] struct {
	Data T `json:"data"`
}

type poolUpdaterDependencies struct {
	tokenDBRepo tokenrepo.TokenRepo

	V3PoolDBRepo           v3poolsrepo.V3PoolDBRepo
	V3PoolCacheRepo        v3poolsrepo.V3PoolCacheRepo
	V3TransactionDBRepo    v3transactionrepo.V3TransactionDBRepo
	V3TransactionCacheRepo v3transactionrepo.V3TransactionCacheRepo
}

type poolUpdater struct {
	updatedTokensSet  map[string]any
	tokensMapForCache map[models.TokenIdentificator]*models.Token
	tokensMapForDB    map[models.TokenIdentificator]*models.Token
	tokenDBRepo       tokenrepo.TokenRepo

	v3PoolDBRepo           v3poolsrepo.V3PoolDBRepo
	v3PoolCacheRepo        v3poolsrepo.V3PoolCacheRepo
	v3TransactionDBRepo    v3transactionrepo.V3TransactionDBRepo
	v3TransactionCacheRepo v3transactionrepo.V3TransactionCacheRepo

	chainID                 uint
	currentBlockPoolChanges map[models.V3PoolIdentificator]models.UniswapV3Pool
}

func newPoolUpdater(chainID uint, tokensMapForCache, tokensMapForDB map[models.TokenIdentificator]*models.Token, updatedTokensSet map[string]any, dependencies poolUpdaterDependencies) (poolUpdater, error) {
	pools, err := dependencies.V3PoolDBRepo.GetPoolsByChainID(chainID)
	if err != nil {
		return poolUpdater{}, err
	}

	var maxBlockNumber int64 = math.MinInt64

	for _, pool := range pools {
		if int64(pool.BlockNumber) > maxBlockNumber {
			maxBlockNumber = int64(pool.BlockNumber)
		}
	}

	if maxBlockNumber == math.MinInt64 {
		return poolUpdater{}, errors.New("unable to define block_number for pools")
	}

	err = dependencies.V3PoolCacheRepo.ClearPools(chainID)
	if err != nil {
		return poolUpdater{}, err
	}

	err = dependencies.V3PoolCacheRepo.SetBlockNumber(chainID, uint64(maxBlockNumber))
	if err != nil {
		return poolUpdater{}, err
	}

	if len(pools) > 0 {
		err = dependencies.V3PoolCacheRepo.SetPools(chainID, pools)
		if err != nil {
			return poolUpdater{}, err
		}

	}

	return poolUpdater{
		updatedTokensSet: updatedTokensSet,

		tokensMapForDB:    tokensMapForDB,
		tokensMapForCache: tokensMapForCache,
		tokenDBRepo:       dependencies.tokenDBRepo,

		v3PoolDBRepo:           dependencies.V3PoolDBRepo,
		v3PoolCacheRepo:        dependencies.V3PoolCacheRepo,
		v3TransactionDBRepo:    dependencies.V3TransactionDBRepo,
		v3TransactionCacheRepo: dependencies.V3TransactionCacheRepo,

		chainID:                 chainID,
		currentBlockPoolChanges: map[models.V3PoolIdentificator]models.UniswapV3Pool{},
	}, nil
}

func (s *poolUpdater) onBlockOver(m *kafka.Message, blockNumber uint64) error {
	metaData := eventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	pools := make([]models.UniswapV3Pool, 0, len(s.currentBlockPoolChanges))
	for _, pool := range s.currentBlockPoolChanges {
		pool.BlockNumber = int(metaData.BlockNumber)
		pools = append(pools, pool)
	}

	for vpi := range s.currentBlockPoolChanges {
		delete(s.currentBlockPoolChanges, vpi)
	}

	err := s.v3PoolCacheRepo.SetPools(s.chainID, pools)
	if err != nil {
		return err
	}
	err = s.v3PoolCacheRepo.SetBlockNumber(s.chainID, blockNumber)
	if err != nil {
		return err
	}

	return nil
}

func (s *poolUpdater) handlePoolEventMessageForCache(m *kafka.Message) error {
	if m == nil {
		return errors.New("nil message")
	}

	metaData := eventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	switch metaData.Type {
	case "SwapV3":
		s.handleSwapEventForCache(metaData, m)
	case "MintV3":
		s.handleMintEventForCache(metaData, m)
	case "BurnV3":
		s.handleBurnEventForCache(metaData, m)
	default:
		// return errors.New("message type not found")
	}

	return nil
}

func (s *poolUpdater) handleSwapEventForCache(metaData eventMetaData, m *kafka.Message) error {
	fmt.Println("Swap for cache", helpers.GetJSONString(metaData))
	data := poolEventData[swapEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pool := models.UniswapV3Pool{}

	if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
		pool = existingPool
	} else {
		var err error
		pool, err = s.v3PoolCacheRepo.GetPoolByIdentificator(poolIdentificator)
		if err != nil {
			return err
		}
	}

	token0, ok0 := s.tokensMapForCache[models.TokenIdentificator{Address: pool.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForCache[models.TokenIdentificator{Address: pool.Token1, ChainID: s.chainID}]
	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	err := s.updatePoolForSwapEvent(&pool, data, token0, token1)
	if err != nil {
		return err
	}

	err = s.updateImpactsForCache(&pool, token0, token1)
	if err != nil {
		return err
	}

	s.updatedTokensSet[token0.Address] = new(any)
	s.updatedTokensSet[token1.Address] = new(any)

	s.currentBlockPoolChanges[poolIdentificator] = pool

	v3Swap := models.V3Swap{
		TxHash:                metaData.TxHash,
		TxTimestamp:           metaData.TxTimestamp,
		PoolAddress:           metaData.Address,
		ChainID:               s.chainID,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               data.Data.Amount0,
		Amount1:               data.Data.Amount1,
		ArchiveToken0USDPrice: token0.USDPrice,
		ArchiveToken1USDPrice: token1.USDPrice,
	}

	return s.v3TransactionCacheRepo.StreamSwap(v3Swap)

}

func (s *poolUpdater) handleMintEventForCache(metaData eventMetaData, m *kafka.Message) error {
	fmt.Println("Mint evet")
	data := poolEventData[mintEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pool := models.UniswapV3Pool{}
	if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
		pool = existingPool
	} else {
		var err error
		pool, err = s.v3PoolCacheRepo.GetPoolByIdentificator(poolIdentificator)
		if err != nil {
			return err
		}
	}

	err := s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
	}

	s.updatedTokensSet[pool.Token0] = new(any)
	s.updatedTokensSet[pool.Token1] = new(any)

	s.currentBlockPoolChanges[poolIdentificator] = pool

	return err
}

func (s *poolUpdater) handleBurnEventForCache(metaData eventMetaData, m *kafka.Message) error {
	fmt.Println("Burn evet")
	data := poolEventData[mintEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pool := models.UniswapV3Pool{}
	if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
		pool = existingPool
	} else {
		var err error
		pool, err = s.v3PoolCacheRepo.GetPoolByIdentificator(poolIdentificator)
		if err != nil {
			return err
		}
	}

	err := s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
	}

	s.updatedTokensSet[pool.Token0] = new(any)
	s.updatedTokensSet[pool.Token1] = new(any)

	s.currentBlockPoolChanges[poolIdentificator] = pool

	return err
}

func (s *poolUpdater) updateImpactsForCache(pool *models.UniswapV3Pool, token0, token1 *models.Token) error {
	_, err := s.updateTokensImpactsForV3Swap(pool, token0, token1)
	if err != nil {
		return err
	}

	return err
}

func (s *poolUpdater) updatePoolForSwapEvent(pool *models.UniswapV3Pool, data poolEventData[swapEventData], token0 *models.Token, token1 *models.Token) error {
	newTick := int(data.Data.Tick.Int64())
	pool.Tick = newTick
	if pool.Tick < pool.TickLower || pool.Tick > pool.TickUpper {
		pool.UpdateTickLowerUpper()
	}

	oldZfo := new(big.Float)
	oldZfo.Set(pool.Zfo10USDRate)

	pool.Liquidity = data.Data.Liquidity
	pool.SqrtPriceX96 = data.Data.SqrtPriceX96

	err := v3poolexchangable.UpdateRateFor10USD(pool, token0, token1)
	if err != nil {
		pool.Zfo10USDRate = big.NewFloat(0)
		pool.NonZfo10USDRate = big.NewFloat(0)
		pool.IsDusty = true
	}

	return nil
}

func (s *poolUpdater) handleTicksForMintBurn(pool *models.UniswapV3Pool, tickLower, tickUpper int, amount *big.Int) error {
	positionLowerTick := models.UniswapV3PoolTick{
		TickIdx:      tickLower,
		LiquidityNet: amount,
	}
	positionUpperTick := models.UniswapV3PoolTick{
		TickIdx:      tickUpper,
		LiquidityNet: new(big.Int).Neg(amount),
	}
	newTicks := []models.UniswapV3PoolTick{positionLowerTick, positionUpperTick}

	s.addNewTicksForPool(pool, newTicks)

	if positionLowerTick.TickIdx < pool.Tick && pool.Tick < positionUpperTick.TickIdx {
		pool.Liquidity.Add(pool.Liquidity, amount)
		err := s.updatePoolRates(pool)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *poolUpdater) addNewTicksForPool(pool *models.UniswapV3Pool, newTicks []models.UniswapV3PoolTick) {
	initializedTicks := pool.GetTicks()
	updatedInitializedTicks := []models.UniswapV3PoolTick{}

	fmt.Println("prevTicks: ", len(initializedTicks))

	for _, newTick := range newTicks {
		for i := range initializedTicks {
			if newTick.TickIdx == initializedTicks[i].TickIdx {
				initializedTicks[i].LiquidityNet.Add(initializedTicks[i].LiquidityNet, newTick.LiquidityNet)
				break
			}

			if i == len(initializedTicks)-1 {
				if newTick.TickIdx > initializedTicks[i].TickIdx {
					initializedTicks = append(
						initializedTicks,
						newTick,
					)
				}

				break
			}

			if newTick.TickIdx > initializedTicks[i].TickIdx && newTick.TickIdx < initializedTicks[i+1].TickIdx {
				updatedInitializedTicks = append(initializedTicks[:i+1], newTick)
				updatedInitializedTicks = append(updatedInitializedTicks, initializedTicks[i+1:]...)
				break
			}

		}
	}

	fmt.Println("newTicks: ", len(updatedInitializedTicks))

	pool.SetTicks(initializedTicks)
}

////DATABASE HANDLING

func (s *poolUpdater) startPostgresUpdater(ctx context.Context, chanel <-chan *kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-chanel:
			err := s.handlePoolEventMessageForPostgres(m)
			if err != nil {
				// fmt.Println("error handling postgres message: ", err)
				continue
			}

		}
	}

}

func (s *poolUpdater) handlePoolEventMessageForPostgres(m *kafka.Message) error {
	// fmt.Println("handling postgres message")
	if m == nil {
		return errors.New("nil message")
	}

	metaData := eventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	switch metaData.Type {
	case "SwapV3":
		return s.handleSwapEventDB(metaData, m)

	case "MintV3":
		return s.handleMintEventDB(metaData, m)

	case "BurnV3":
		return s.handleBurnEventDB(metaData, m)

	default:
		return errors.New("message type not found")
	}
}

func (s *poolUpdater) handleSwapEventDB(metaData eventMetaData, m *kafka.Message) error {
	data := poolEventData[swapEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pool, err := s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
	if err != nil {
		return err
	}

	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token1, ChainID: s.chainID}]

	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	err = s.updatePoolForSwapEvent(&pool, data, token0, token1)
	if err != nil {
		return err
	}
	err = s.updateImpactsForDB(&pool, token0, token1)
	if err != nil {
		return err
	}

	err = s.v3PoolDBRepo.UpdatePoolColumns(pool, []string{
		models.UNISWAP_V3_POOL_LIQUIDITY,
		models.UNISWAP_V3_POOL_SQRTPRICEX96,
		models.UNISWAP_V3_POOL_TICK,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
	})
	if err != nil {
		return err
	}

	v3Swap := models.V3Swap{
		TxHash:                metaData.TxHash,
		TxTimestamp:           metaData.TxTimestamp,
		PoolAddress:           metaData.Address,
		ChainID:               s.chainID,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               data.Data.Amount0,
		Amount1:               data.Data.Amount1,
		ArchiveToken0USDPrice: token0.USDPrice,
		ArchiveToken1USDPrice: token1.USDPrice,
	}
	fmt.Println("TXTIMESTAMP: ", v3Swap.TxTimestamp)

	err = s.v3TransactionDBRepo.CreateV3Swap(&v3Swap)
	if err != nil {
		fmt.Println("error creating tx:", err)
		return err
	}

	return nil
}

func (s *poolUpdater) updateImpactsForDB(pool *models.UniswapV3Pool, token0, token1 *models.Token) error {
	updatedImpact, err := s.updateTokensImpactsForV3Swap(pool, token0, token1)
	if err != nil {
		return err
	}

	if updatedImpact != nil {
		if updatedImpact.TokenAddress == token0.Address {
			err = s.tokenDBRepo.UpdateTokenPriceImpactAndTokenPrice(token0, updatedImpact)
		} else {
			err = s.tokenDBRepo.UpdateTokenPriceImpactAndTokenPrice(token1, updatedImpact)
		}
		if err != nil {
			fmt.Println("Unable to write update token price impact and token price")
			return err
		} else {
			fmt.Println("Updated pool impact", helpers.GetJSONString(updatedImpact))
		}
	}

	return err
}

func (s *poolUpdater) handleMintEventDB(metaData eventMetaData, m *kafka.Message) error {
	data := poolEventData[mintEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	fmt.Println("DB Mint")

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pool, err := s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
	if err != nil {
		return err
	}

	pool.BlockNumber = int(metaData.BlockNumber)
	err = s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
	}

	err = s.v3PoolDBRepo.UpdatePoolColumns(pool, []string{
		models.UNISWAP_V3_POOL_LIQUIDITY,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *poolUpdater) handleBurnEventDB(metaData eventMetaData, m *kafka.Message) error {
	fmt.Println("DB Burn")

	data := poolEventData[burnEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pool, err := s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
	if err != nil {
		return err
	}
	pool.BlockNumber = int(metaData.BlockNumber)

	err = s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
	}

	err = s.v3PoolDBRepo.UpdatePoolColumns(pool, []string{
		models.UNISWAP_V3_POOL_LIQUIDITY,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *poolUpdater) updatePoolRates(pool *models.UniswapV3Pool) error {
	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token1, ChainID: s.chainID}]
	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}
	err := v3poolexchangable.UpdateRateFor10USD(pool, token0, token1)
	if err != nil {
		pool.Zfo10USDRate = big.NewFloat(0)
		pool.NonZfo10USDRate = big.NewFloat(0)
		pool.IsDusty = true
	}

	return nil
}

func (s *poolUpdater) updateTokensImpactsForV3Swap(pool *models.UniswapV3Pool, token0, token1 *models.Token) (*models.TokenPriceImpact, error) {
	var token0CurrentPoolImpact *models.TokenPriceImpact = nil
	for _, imp := range token0.GetImpacts() {
		if imp.ChainID == pool.ChainID &&
			imp.TokenAddress == pool.Token0 &&
			imp.ExchangeIdentifier == models.GetExchangeIdentifierForV3Pool(
				pool.ChainID,
				pool.Address,
			) {
			token0CurrentPoolImpact = imp
			break
		}
	}

	var token1CurrentPoolImpact *models.TokenPriceImpact = nil
	for _, imp := range token1.GetImpacts() {
		if imp.ChainID == pool.ChainID &&
			imp.TokenAddress == pool.Token1 &&
			imp.ExchangeIdentifier == models.GetExchangeIdentifierForV3Pool(
				pool.ChainID,
				pool.Address,
			) {
			token1CurrentPoolImpact = imp
			break
		}
	}

	var updatedImpact *models.TokenPriceImpact

	if token0CurrentPoolImpact != nil {
		// fmt.Println("Updating impact for token0...", token0.Symbol)
		// fmt.Println("old value: ", token0CurrentPoolImpact.USDPrice)
		tokenRate, err := v3poolexchangable.GetRateForPool(pool, pool.Token0, token1.Decimals, token0.Decimals)
		if err != nil {
			// fmt.Println("unable to get price: ", err)
			return nil, nil
		}

		token0USDPrice := new(big.Float).Mul(tokenRate, token1.USDPrice)
		if token0USDPrice.Cmp(big.NewFloat(0)) == 0 {
			return nil, nil
		}

		token0CurrentPoolImpact.USDPrice = token0USDPrice
		token0CurrentPoolImpact.Impact = v3poolexchangable.GetImpactForPool(pool, int64(token1.Decimals), token1.USDPrice)

		token0.USDPrice, err = token0.AveragePrice()
		if err != nil {
			return nil, err
		}
		updatedImpact = token0CurrentPoolImpact

		// fmt.Println("new value: ", token0CurrentPoolImpact.USDPrice)
		// fmt.Println("")

	} else if token1CurrentPoolImpact != nil {
		// fmt.Println("Updating impact for token1...", token1.Symbol)
		// fmt.Println("old value: ", token1CurrentPoolImpact.USDPrice)
		tokenRate, err := v3poolexchangable.GetRateForPool(pool, pool.Token1, token0.Decimals, token1.Decimals)
		if err != nil {
			// fmt.Println("unable to get price: ", err)
			return nil, nil
		}

		token1USDPrice := new(big.Float).Mul(tokenRate, token0.USDPrice)
		if token1USDPrice.Cmp(big.NewFloat(0)) == 0 {
			return nil, nil
		}

		token1CurrentPoolImpact.USDPrice = token1USDPrice
		token1CurrentPoolImpact.Impact = v3poolexchangable.GetImpactForPool(pool, int64(token0.Decimals), token0.USDPrice)

		token1.USDPrice, err = token1.AveragePrice()
		if err != nil {
			return nil, err
		}

		updatedImpact = token1CurrentPoolImpact

		// fmt.Println("new value: ", token1CurrentPoolImpact.USDPrice)
		// fmt.Println("")
	}

	return updatedImpact, nil

}
