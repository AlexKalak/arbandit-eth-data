package merger

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/models"
)

func (m *merger) MergePools(chainID uint) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	tokens, err := m.tokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		return err
	}
	fmt.Println("Len tokens: ", len(tokens))

	pools, err := m.subgraphClient.GetV3Pools(context.Background(), chainID)
	if err != nil {
		return err
	}

	tokensMap := map[string]any{}
	for _, token := range tokens {
		tokensMap[token.Address] = new(any)
	}

	query := psql.Insert(models.UNISWAP_V3_POOL_TABLE).Columns(
		models.UNISWAP_V3_POOL_ADDRESS,
		models.UNISWAP_V3_POOL_CHAINID,
		models.UNISWAP_V3_POOL_EXCHANGE_NAME,
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

		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
	)

	poolsMap := map[string]any{}

	badPools := 0
	for i, pool := range pools {
		_, ok1 := tokensMap[pool.Token0]
		_, ok2 := tokensMap[pool.Token1]
		if !ok1 || !ok2 {
			badPools++
			continue
		}

		if _, ok := poolsMap[pool.Address]; ok {
			fmt.Println("found duplicate")
			continue
		}

		poolsMap[pool.Address] = new(any)

		query = query.Values(
			pool.Address,
			chainID,
			pool.ExchangeName,
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

			defaultZfo10USDRate,
			defaultNonZfo10USDRate,
		)

		if (i+1)%2000 == 0 {
			resp, err := query.RunWith(db).Exec()
			if err != nil {
				return err
			}

			rowsAff, err := resp.RowsAffected()
			if err != nil {
				return err
			}
			fmt.Println("Pools inserted", rowsAff)

			query = psql.Insert(models.UNISWAP_V3_POOL_TABLE).Columns(
				models.UNISWAP_V3_POOL_ADDRESS,
				models.UNISWAP_V3_POOL_CHAINID,
				models.UNISWAP_V3_POOL_EXCHANGE_NAME,
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

				models.UNISWAP_V3_ZFO_10USD_RATE,
				models.UNISWAP_V3_NON_ZFO_10USD_RATE,
			)
		}
	}

	fmt.Println("Bad pools: ", badPools)

	resp, err := query.RunWith(db).Exec()
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	rowsAff, err := resp.RowsAffected()
	if err != nil {
		return err
	}
	fmt.Println("End pools inserted", rowsAff)

	return nil
}

func (m *merger) MergePoolsData(ctx context.Context, chainID uint, blockNumber *big.Int) error {
	pools, err := m.v3PoolsDBRepo.GetPoolsByChainIDWith0BlockNumberOrdered(chainID)
	if err != nil {
		return err
	}

	poolsForQuery := 1000

	for startPoolIndex := 0; startPoolIndex < len(pools); startPoolIndex += poolsForQuery {
		fmt.Println("start pool index: ", startPoolIndex)
		slice := pools[startPoolIndex:]
		if startPoolIndex+poolsForQuery < len(pools) {
			slice = pools[startPoolIndex : startPoolIndex+poolsForQuery]
		}

		updatedPools, err := m.rpcClient.GetPoolsData(ctx, slice, chainID, blockNumber)
		if err != nil {
			return err
		}

		err = m.v3PoolsDBRepo.UpdatePoolsLiquiditySqrtPriceTickAndBlockNumber(
			updatedPools,
		)

		if err != nil {
			fmt.Println("Error updating database", err)
			return err
		}

	}

	return nil
}

func (m *merger) MergePoolsTicks(ctx context.Context, chainID uint, blockNumber *big.Int) error {
	pools, err := m.v3PoolsDBRepo.GetPoolsByChainIDOrdered(chainID)
	if err != nil {
		return err
	}
	poolsForQuery := 1000
	lenPools := len(pools)

	for startPoolIndex := 0; startPoolIndex < lenPools; startPoolIndex += poolsForQuery {
		fmt.Println("start pool index: ", startPoolIndex)

		slice := pools[startPoolIndex:lenPools]
		if startPoolIndex+poolsForQuery < lenPools {
			slice = pools[startPoolIndex : startPoolIndex+poolsForQuery]
		}
		fmt.Println("slice len: ", len(slice))

		updatedPools, err := m.rpcClient.GetPoolsTicks(ctx, slice, chainID, blockNumber)
		if err != nil {
			return err
		}

		fmt.Println("UpdatedPools", updatedPools)

		err = m.v3PoolsDBRepo.UpdatePoolsLowerUpperNearTicks(
			updatedPools,
		)

		if err != nil {
			fmt.Println("Error updating database", err)
			return err
		}

	}

	return nil
}

func (m *merger) ValidateV3PoolsAndComputeAverageUSDPrice(chainID uint) error {
	stableCoins, err := m.tokenRepo.GetTokensByAddressesAndChainID(USD_STABLECOIN_ADDRESSES, chainID)
	if err != nil {
		return err
	}
	fmt.Println(stableCoins)

	pools, err := m.v3PoolsDBRepo.GetPoolsByChainID(chainID)
	if err != nil {
		return err
	}
	tokens, err := m.tokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		return err
	}

	tokenPricesMap, notDustyPoolsMap, err := v3poolexchangable.
		ValidateV3PoolsAndGetAverageUSDPriceForTokens(
			chainID,
			tokens,
			pools,
			stableCoins,
		)

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

	err = m.tokenRepo.UpdateTokens(updatingTokens)
	if err != nil {
		return err
	}

	notDustyPools := make([]models.UniswapV3Pool, 0, len(notDustyPoolsMap))
	for i, _ := range pools {
		if nonDustyPool, ok := notDustyPoolsMap[pools[i].Address]; ok {
			pools[i].IsDusty = false

			pools[i].Zfo10USDRate = nonDustyPool.Zfo10USDRate
			pools[i].NonZfo10USDRate = nonDustyPool.NonZfo10USDRate

			fmt.Println(pools[i].Address, pools[i].Zfo10USDRate)
			fmt.Println(pools[i].Address, pools[i].NonZfo10USDRate)

			notDustyPools = append(notDustyPools, pools[i])
		}
	}

	err = m.v3PoolsDBRepo.SetAllPoolsToDusty(chainID)
	if err != nil {
		return err
	}

	err = m.v3PoolsDBRepo.UpdatePoolsIsDusty(notDustyPools)
	if err != nil {
		return err
	}

	return nil
}
