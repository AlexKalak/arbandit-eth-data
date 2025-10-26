package merging

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/src/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/src/external/subgraphs/v3subgraphs"
	"github.com/alexkalak/go_market_analyze/src/models"
)

func (m *Merger) MergeV3PoolsNotFilled(chainID uint) error {

	//All tokens from database
	tokens, err := m.TokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		fmt.Println("Error getting tokens from db")
		return err
	}

	//Set to not add the same pair in db
	alreadyAddedPoolAddresses := map[string]any{}

	//Set to check if pair's token is in database
	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		tokensMap[token.Address] = &token
	}

	subgraphClient, err := subgraphs.New()
	if err != nil {
		return err
	}

	//add all tokens pairs to query
	for _, token := range tokens {
		prioPreloadChunks := 8
		totalPreloadChunks := 100

		if token.Address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" || token.Address == "0xdac17f958d2ee523a2206206994597c13d831ec7" {
			prioPreloadChunks = 20
			totalPreloadChunks = 1000
		}

		streamer, err := subgraphs.NewV3Streamer(subgraphClient, token.Address, chainID, prioPreloadChunks, totalPreloadChunks)
		if err != nil {
			fmt.Println("Error initializing streamer v3", err)
			continue
		}

		for {
			v3Pools, ok := streamer.Next()
			if !ok {
				break
			}

			m.addV3PoolsInDB(v3Pools, chainID, alreadyAddedPoolAddresses, tokensMap)
		}

	}

	return nil

}

func (m *Merger) addV3PoolsInDB(pools []v3subgraphs.ExchangeV3, chainID uint, alreadyAddedPoolAddresses map[string]any, tokensMap map[string]*models.Token) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}
	chunkSize := 1000
	for chunk := 0; chunk < len(pools); chunk += chunkSize {
		slice := pools[chunk:]

		if chunk+chunkSize < len(pools) {
			slice = pools[chunk:chunkSize]
		}

		query := psql.
			Insert(models.UNISWAP_V3_POOL_TABLE).
			Columns(
				models.UNISWAP_V3_POOL_ADDRESS,
				models.UNISWAP_V3_POOL_EXCHANGE_NAME,
				models.UNISWAP_V3_POOL_CHAINID,
				models.UNISWAP_V3_POOL_TOKEN0,
				models.UNISWAP_V3_POOL_TOKEN1,
				models.UNISWAP_V3_POOL_SQRTPRICEX96,
				models.UNISWAP_V3_POOL_LIQUIDITY,
				models.UNISWAP_V3_POOL_TICK,
				models.UNISWAP_V3_POOL_TICK_SPACING,
				models.UNISWAP_V3_POOL_TICK_LOWER,
				models.UNISWAP_V3_POOL_TICK_UPPER,
				models.UNISWAP_V3_POOL_NEAR_TICKS,
				models.UNISWAP_V3_POOL_FEE_TIER,
				models.UNISWAP_V3_POOL_IS_DUSTY,
				models.UNISWAP_V3_POOL_BLOCK_NUMBER,

				models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
				models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
			)

		for _, pool := range slice {
			if _, ok := alreadyAddedPoolAddresses[pool.Address]; ok {
				continue
			}

			// if !ok1 || !ok2 {
			// 	continue
			// }

			query = query.Values(
				pool.Address,
				pool.ExchangeName,
				chainID,
				pool.Token0,
				pool.Token1,
				defaultSqrtPriceX96,
				defaultLiquidity,
				defaultTick,
				defaultTickSpacing,
				defaultTickLower,
				defaultTickUpper,
				defaultNearTicks,
				pool.FeeTier,
				defaultIsDusty,
				defaultBlockNumber,
				defaultToken0Holding,
				defaultToken1Holding,
			)

			alreadyAddedPoolAddresses[pool.Address] = new(any)

		}

		res, err := query.RunWith(db).Exec()
		if err != nil {
			fmt.Println("Error runngin merge v3 query: ", err)
			continue
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			fmt.Println("error getting rows affected of v3 merging: ", err)
			continue
		}

		fmt.Println("Rows affecting while v3 merging: ", rowsAffected)

	}

	return nil
}

func (m *Merger) MergeExchangeNamesForV3Pools(chainID uint) error {
	//All tokens from database
	tokens, err := m.TokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		fmt.Println("Error getting tokens from db")
		return err
	}

	//Set to not add the same pair in db
	alreadyAddedPoolAddresses := map[string]any{}

	//Set to check if pair's token is in database
	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		tokensMap[token.Address] = &token
	}

	subgraphClient, err := subgraphs.New()
	if err != nil {
		return err
	}

	//add all tokens pairs to query
	for _, token := range tokens {
		if token.Address != "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" {
			continue

		}
		streamer, err := subgraphs.NewV3Streamer(subgraphClient, token.Address, chainID, 8, 100)
		if err != nil {
			fmt.Println("Error initializing streamer v3", err)
			continue
		}

		for {
			v3Pools, ok := streamer.Next()
			if !ok {
				break
			}
			m.updateExchangeNamesForV3PoolsInDB(v3Pools, chainID, alreadyAddedPoolAddresses, tokensMap)

		}

	}

	return nil
}

func (m *Merger) updateExchangeNamesForV3PoolsInDB(pools []v3subgraphs.ExchangeV3, chainID uint, alreadyAddedPoolAddresses map[string]any, tokensMap map[string]*models.Token) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	for _, pool := range pools {
		query := psql.
			Update(models.UNISWAP_V3_POOL_TABLE).
			Set(models.UNISWAP_V3_POOL_EXCHANGE_NAME, pool.ExchangeName).
			Where(sq.Eq{models.UNISWAP_V3_POOL_ADDRESS: pool.Address, models.UNISWAP_V3_POOL_CHAINID: chainID})

		if _, ok := alreadyAddedPoolAddresses[pool.Address]; ok {
			continue
		}

		// check if both of pair's tokens exist in database, else skip
		_, ok1 := tokensMap[pool.Token0]
		_, ok2 := tokensMap[pool.Token1]

		if !ok1 || !ok2 {
			continue
		}

		alreadyAddedPoolAddresses[pool.Address] = new(any)

		_, err := query.RunWith(db).Exec()
		if err != nil {
			fmt.Println("Error runngin merge v3 query: ", err)
			continue
		}
	}

	return nil
}

func (m *Merger) MergeV3PoolsData(chainID uint, blockNumber *big.Int) error {
	poolsFromDB, err := m.V3PoolsRepo.GetPoolsByChainID(chainID)
	if err != nil {
		panic(err)
	}

	pools, err := m.getLiquiditySqrtPriceAndTickForV3Pools(poolsFromDB, chainID, blockNumber)
	for i := range pools {
		pools[i].IsDusty = true
		pools[i].BlockNumber = int(blockNumber.Int64())
	}
	if err != nil {
		panic(err)
	}

	err = m.V3PoolsRepo.UpdatePoolsLiquiditySqrtPriceTickAndTokenHoldingsBlockNumber(pools)
	if err != nil {
		panic(err)
	}

	return nil
}

func (m *Merger) MergeV3PoolsTicks(chainID uint, blockNumber *big.Int) error {
	poolsFromDB, err := m.V3PoolsRepo.GetPoolsByChainID(chainID)
	if err != nil {
		panic(err)
	}

	pools, err := m.getLowerUpperAndNearTicks(poolsFromDB, chainID, blockNumber)
	for i := range pools {
		pools[i].IsDusty = true
		pools[i].BlockNumber = int(blockNumber.Int64())
	}
	if err != nil {
		panic(err)
	}

	err = m.V3PoolsRepo.UpdatePoolsLowerUpperNearTicks(pools)
	if err != nil {
		panic(err)
	}

	return nil
}

func (m *Merger) getLiquiditySqrtPriceAndTickForV3Pools(poolsFromDB []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error) {
	chunkSize := 25
	resPools := make([]models.UniswapV3Pool, 0, len(poolsFromDB))
	poolsMap := make(map[string]*models.UniswapV3Pool)

	repeatedTimes := 0
	for i := 0; i < len(poolsFromDB); i += chunkSize {
		fmt.Println("Chunk: ", i/chunkSize)
		poolsChunk := make([]models.UniswapV3Pool, 0, chunkSize)
		if i+chunkSize > len(poolsFromDB) {
			poolsChunk = append(poolsChunk, poolsFromDB[i:]...)
		} else {
			poolsChunk = append(poolsChunk, poolsFromDB[i:i+chunkSize]...)
		}

		poolsForCall := make([]*models.UniswapV3Pool, 0, len(poolsChunk))
		for _, pool := range poolsChunk {
			poolsForCall = append(poolsForCall, &pool)
			poolsMap[pool.Address] = &pool
		}

		poolsData, err := m.RpcClient.GetV3PoolsData(poolsForCall, chainID, blockNumber)
		if err != nil {
			fmt.Println("Sleeping 3 second")
			if repeatedTimes < 3 {
				i -= chunkSize
				repeatedTimes++
			}
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		repeatedTimes = 0

		for _, poolResp := range poolsData {
			poolFromDB, ok := poolsMap[poolResp.Address]
			if !ok {
				continue
			}

			poolFromDB.Liquidity = poolResp.Liquidity
			poolFromDB.SqrtPriceX96 = poolResp.Slot0.SqrtPriceX96
			poolFromDB.Tick = int(poolResp.Slot0.Tick.Int64())
			poolFromDB.TickSpacing = int(poolResp.TickSpacing.Int64())

			poolFromDB.Token0Holding = poolResp.Token0Holding
			if poolFromDB.Token0Holding == nil {
				poolFromDB.Token0Holding = big.NewInt(0)
			}
			poolFromDB.Token1Holding = poolResp.Token1Holding
			if poolFromDB.Token1Holding == nil {
				poolFromDB.Token1Holding = big.NewInt(0)
			}

			pool := models.UniswapV3Pool{
				Address:       poolFromDB.Address,
				ChainID:       poolFromDB.ChainID,
				Token0:        poolFromDB.Token0,
				Token1:        poolFromDB.Token1,
				SqrtPriceX96:  poolFromDB.SqrtPriceX96,
				Liquidity:     poolFromDB.Liquidity,
				Tick:          poolFromDB.Tick,
				TickSpacing:   poolFromDB.TickSpacing,
				FeeTier:       poolFromDB.FeeTier,
				IsDusty:       poolFromDB.IsDusty,
				Token0Holding: poolFromDB.Token0Holding,
				Token1Holding: poolFromDB.Token1Holding,
			}

			resPools = append(resPools, pool)
		}
	}

	return resPools, nil

}

func (m *Merger) getLowerUpperAndNearTicks(poolsFromDB []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error) {
	resPools := make([]models.UniswapV3Pool, 0, len(poolsFromDB))

	for i := range poolsFromDB {
		pool := poolsFromDB[i]

		var lowerTick int64 = math.MinInt64
		var upperTick int64 = math.MaxInt64

		if pool.Tick == 0 {
			continue
		}

		ticks, err := m.RpcClient.GetV3PoolNearTicks(&pool, chainID, blockNumber)
		if err != nil {
			i--
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, tick := range ticks {
			if tick < pool.Tick && int64(tick) > lowerTick {
				lowerTick = int64(tick)
			} else if tick > pool.Tick && int64(tick) < upperTick {
				upperTick = int64(tick)
			}

		}

		ticksJSON, err := json.Marshal(ticks)
		if err != nil {
			fmt.Println("Error marshaling ticks: ", err)
			continue
		}

		fmt.Println(lowerTick, upperTick)
		fmt.Println(string(ticksJSON))

		if lowerTick > math.MinInt64 && upperTick < math.MaxInt64 {
			pool.TickLower = int(lowerTick)
			pool.TickUpper = int(upperTick)
			pool.NearTicksJSON = string(ticksJSON)

		} else {
			pool.TickLower = 0
			pool.TickUpper = 0
			pool.NearTicksJSON = "[]"
		}

		resPools = append(resPools, pool)

	}

	return resPools, nil

}

func (m *Merger) ValidateV3PoolsAndComputeAverageUSDPrice(pools []models.UniswapV3Pool, chainID uint) error {
	stableCoins, err := m.TokenRepo.GetTokensBySymbolsAndChainID(USD_STABLECOIN_SYMBOLS, chainID)
	if err != nil {
		return err
	}
	tokens, err := m.TokenRepo.GetTokens()
	if err != nil {
		return err
	}

	tokenPricesMap, notDustyPoolsMap, err := v3poolexchangable.ValidateV3PoolsAndGetAverageUSDPriceForTokens(pools, stableCoins)
	if err != nil {
		return err
	}
	fmt.Println("Not dusty pools: ", len(notDustyPoolsMap))

	updatingTokens := make([]models.Token, 0, len(tokenPricesMap))
	for _, token := range tokens {
		if price, ok := tokenPricesMap[token.Address]; ok {
			token.DefiUSDPrice = price
			updatingTokens = append(updatingTokens, token)
		}
	}

	err = m.TokenRepo.UpdateTokens(updatingTokens)
	if err != nil {
		return err
	}

	for i, pool := range pools {
		if _, ok := notDustyPoolsMap[pool.Address]; ok {
			pools[i].IsDusty = false
		} else {
			pools[i].IsDusty = true
		}
	}

	err = m.V3PoolsRepo.UpdatePoolsIsDusty(pools)
	if err != nil {
		return err
	}

	return nil
}
