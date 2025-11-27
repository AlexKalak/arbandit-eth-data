package v3poolexchangable

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
)

var bTen = big.NewInt(10)

func ValidateV3PoolsAndGetAverageUSDPriceForTokens(
	chainID uint,
	allTokens []*models.Token,
	pools []models.UniswapV3Pool,
	stableCoins []*models.Token,
) ([]*models.Token, map[string]*models.UniswapV3Pool, error) {
	//Map stable coins (address) -> token
	stableCoinsMap := map[string]*models.Token{}
	for _, stableCoin := range stableCoins {
		stableCoinsMap[stableCoin.Address] = stableCoin
	}

	//Map tokens (address) -> token
	tokenDataMap := map[string]*models.Token{}
	for _, token := range allTokens {
		fileteredImpacts := []*models.TokenPriceImpact{}
		for _, impact := range token.GetImpacts() {
			//Remove v3 pool impacts
			if strings.Contains(impact.ExchangeIdentifier, "v3pool") {
				continue
			}

			fileteredImpacts = append(fileteredImpacts, impact)
		}
		token.SetImpacts(fileteredImpacts)

		if token.ChainID != chainID {
			continue
		}

		tokenDataMap[token.Address] = token
	}

	definedTokensMap := map[string]*models.Token{}
	checkedPools := map[string]any{}
	notDustyPools := map[string]*models.UniswapV3Pool{}

	defineStableTokens(stableCoinsMap, definedTokensMap)

	// How much away from stable coin can pairs checking token be
	checkingDeepness := 4
	for i := range checkingDeepness {
		amountErrs := 0
		fmt.Println("step: ", i)
		avoidTokensNew := map[string]any{}

		for _, pool := range pools {

			if pool.Liquidity.Cmp(big.NewInt(0)) == 0 || pool.SqrtPriceX96.Cmp(big.NewInt(0)) == 0 {
				continue
			}

			err := checkPairForDefinedTokens(avoidTokensNew, tokenDataMap, definedTokensMap, checkedPools, &pool)
			if err != nil {
				amountErrs++
			}
			if !pool.IsDusty {
				if _, ok := notDustyPools[pool.Address]; !ok {
					notDustyPools[pool.Address] = &pool
				}
			}
		}

		fmt.Println("Amount err: ", amountErrs)
	}

	fmt.Println("not dusty pools before checking: ", len(notDustyPools))

	//Check imitate swap to tell if pool is inactive
	for key, pool := range notDustyPools {
		token0, ok1 := definedTokensMap[pool.Token0]
		token1, ok2 := definedTokensMap[pool.Token1]

		if !ok1 || !ok2 {
			fmt.Println("Not ok")
			delete(notDustyPools, key)
			continue
		}

		err := UpdateRateFor10USD(pool, token0, token1)
		if err != nil {
			fmt.Println(pool.Address, err)
			delete(notDustyPools, key)
			continue
		}
	}

	updatedTokens := make([]*models.Token, 0, len(definedTokensMap))
	for _, v := range definedTokensMap {
		updatedTokens = append(updatedTokens, v)
	}

	return updatedTokens, notDustyPools, nil
}

func UpdateRateFor10USD(pool *models.UniswapV3Pool, token0 *models.Token, token1 *models.Token) error {
	if token0.USDPrice.Cmp(big.NewFloat(0)) == 0 ||
		token1.USDPrice.Cmp(big.NewFloat(0)) == 0 {
		return errors.New("invalid usd price")
	}

	exchangablePool, err := NewV3ExchangablePool(pool, token0, token1)
	if err != nil {
		return errors.New("invalid usd price")
	}

	usdAmount := big.NewInt(5)

	amount0Init := token0.FromUSD(usdAmount)
	amount1Init := token1.FromUSD(usdAmount)

	amount1Out, err := exchangablePool.ImitateSwap(amount0Init, true)
	if err != nil {
		return err
	}
	reverted0, err := exchangablePool.ImitateSwap(amount1Out, false)
	if err != nil {
		return err
	}

	amount0Out, err := exchangablePool.ImitateSwap(amount1Init, false)
	if err != nil {
		return err
	}
	reverted1, err := exchangablePool.ImitateSwap(amount0Out, true)
	if err != nil {
		return err
	}
	if pool.Address == "0xc07044d4b947e7c5701f1922db048c6b47799b84" {
		fmt.Println("amount0Init: ", amount0Init)
		fmt.Println("reverted0: ", reverted0)
		fmt.Println("amount1Init: ", amount1Init)
		fmt.Println("reverted1: ", reverted1)
	}

	if reverted0.Cmp(big.NewInt(0)) == 0 ||
		reverted1.Cmp(big.NewInt(0)) == 0 {
		return errors.New("Invalid return values")
	}

	percentZfo := new(big.Float).Quo(new(big.Float).SetInt(reverted0), new(big.Float).SetInt(amount0Init))
	if percentZfo.Cmp(big.NewFloat(0.97)) < 0 {
		return errors.New("too much token0 -> token1 (zfo) slippage")
	}
	percentNonZfo := new(big.Float).Quo(new(big.Float).SetInt(reverted1), new(big.Float).SetInt(amount1Init))
	if percentNonZfo.Cmp(big.NewFloat(0.97)) < 0 {
		return errors.New("too much token1 -> token0 (non zfo) slippage")
	}

	pool.Zfo10USDRate = new(big.Float).Quo(token1.GetRealAmount(amount1Out), token0.GetRealAmount(amount0Init))
	pool.NonZfo10USDRate = new(big.Float).Quo(token0.GetRealAmount(amount0Out), token1.GetRealAmount(amount1Init))

	return nil
}

func divideSqrtPriceX96(sqrtPriceX96 *big.Int) *big.Float {
	sqrtPrice := new(big.Float)
	sqrtPrice.Quo(new(big.Float).SetInt(sqrtPriceX96), Q96Float)
	return sqrtPrice
}

func GetImpactForPool(pool *models.UniswapV3Pool, dtDecimals int64, dtUSDPrice *big.Float) *big.Int {
	div := new(big.Int).Div(pool.Liquidity, big.NewInt(dtDecimals))
	res, _ := new(big.Float).Mul(dtUSDPrice, new(big.Float).SetInt(div)).Int(nil)
	return res
}

func GetRateForPool(pool *models.UniswapV3Pool, tokenAddress string, dtDecimals int, tokenDecimals int) (*big.Float, error) {
	// always token1 -> token0
	//Rate initally is sqrt
	rate := divideSqrtPriceX96(pool.SqrtPriceX96)
	rate.Mul(rate, rate)

	if pool.Token0 == tokenAddress {
		dtDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(dtDecimals)), nil)
		tokenDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenDecimals)), nil)

		rate.Mul(rate, new(big.Float).SetInt(tokenDecimalsPow))
		rate.Quo(rate, new(big.Float).SetInt(dtDecimalsPow))

		return rate, nil
	} else if pool.Token1 == tokenAddress {
		dtDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(dtDecimals)), nil)
		tokenDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenDecimals)), nil)

		rate.Mul(rate, new(big.Float).SetInt(dtDecimalsPow))
		rate.Quo(rate, new(big.Float).SetInt(tokenDecimalsPow))
		rate.Quo(big.NewFloat(1), rate)
		return rate, nil
	}
	return nil, errors.New("cannot get price, token not found in pool")
}

func defineStableTokens(stableCoinsMap map[string]*models.Token, definedTokensMap map[string]*models.Token) {
	for _, stableCoin := range stableCoinsMap {
		fmt.Println("defining: ", stableCoin.Symbol)
		if len(stableCoin.GetImpacts()) == 0 {
			fmt.Println("impacts not found")
			impactValue, _ := new(big.Int).SetString("10000000000000000000000000000", 10)

			impact := &models.TokenPriceImpact{
				ChainID:            stableCoin.ChainID,
				USDPrice:           big.NewFloat(1),
				TokenAddress:       stableCoin.Address,
				ExchangeIdentifier: models.GetExchangeIdentifierForV3Pool(0, "phantom_pool"),
				Impact:             impactValue,
			}
			stableCoin.SetImpacts([]*models.TokenPriceImpact{impact})
		}
		fmt.Println("defined: ", stableCoin.Symbol)

		definedTokensMap[stableCoin.Address] = stableCoin
	}
}

func checkPairForDefinedTokens(
	avoidTokensNew map[string]any,
	tokenDataMap map[string]*models.Token,
	definedTokensMap map[string]*models.Token,
	checkedPools map[string]any,
	pool *models.UniswapV3Pool,
) error {
	if _, ok := checkedPools[pool.Address]; ok {
		return nil
	}

	if !pool.TicksValid() {
		checkedPools[pool.Address] = new(any)
		return nil
	}

	dt := &models.Token{}
	dt0, hasDefinedToken0 := definedTokensMap[pool.Token0]
	dt1, hasDefinedToken1 := definedTokensMap[pool.Token1]
	atAddress := ""

	_, avoidDt0 := avoidTokensNew[pool.Token0]
	_, avoidDt1 := avoidTokensNew[pool.Token1]

	if avoidDt0 && avoidDt1 {
		return nil
	}

	isBothDefined := false
	if hasDefinedToken0 && hasDefinedToken1 {
		isBothDefined = true
		if dt0.GetTotalImpactInUSD().Cmp(dt1.GetTotalImpactInUSD()) > 0 {
			dt = dt0
			atAddress = dt1.Address
		} else {
			dt = dt1
			atAddress = dt0.Address
		}
	} else if hasDefinedToken0 {
		if avoidDt0 {
			return nil
		}

		dt = dt0
		atAddress = pool.Token1
	} else if hasDefinedToken1 {
		if avoidDt1 {
			return nil
		}

		dt = dt1
		atAddress = pool.Token0
	} else {
		return nil
	}

	isDTZero := pool.Token0 == dt.Address

	// Mark that checked this pair for liquidity
	atData, ok := tokenDataMap[atAddress]
	if !ok {
		fmt.Println("Another token not found ============================================", atAddress, helpers.GetJSONString(pool))
		return nil
	}

	at := models.Token{
		Name:     atData.Name,
		Symbol:   atData.Symbol,
		Address:  atData.Address,
		ChainID:  atData.ChainID,
		LogoURI:  atData.LogoURI,
		Decimals: atData.Decimals,
	}

	exchangable := ExchangableUniswapV3PoolRaw{
		Pool: pool,
	}

	pool.IsDusty = true

	checkedPools[pool.Address] = new(any)

	pool.Zfo10USDRate = big.NewFloat(0)
	pool.NonZfo10USDRate = big.NewFloat(0)

	//Changing dt for another token
	dtAmountFor10USD := dt.FromUSD(big.NewInt(10))

	atOutFor10USD, err := exchangable.ImitateSwap(dtAmountFor10USD, isDTZero)
	if err != nil {
		return err
	}

	resultAmountOfDt, err := exchangable.ImitateSwap(atOutFor10USD, !isDTZero)
	if err != nil {
		return err
	}

	percent := new(big.Float).Quo(new(big.Float).SetInt(resultAmountOfDt), new(big.Float).SetInt(dtAmountFor10USD))
	if percent.Cmp(big.NewFloat(0.98)) < 0 {
		return nil
	}

	// fmt.Println(dt.Token.Address, dt.Token.Decimals, dtAmountFor10USD.String(), "->", resultAmountOfDt.String())

	tokenRate, err := GetRateForPool(pool, at.Address, dt.Decimals, at.Decimals)
	if err != nil {
		return nil
	}

	atUSDPrice := new(big.Float).Mul(tokenRate, dt.USDPrice)
	if atUSDPrice.Cmp(big.NewFloat(0)) == 0 {
		return nil
	}

	// If token already added check if new amount of usd provided by pair is bigger than previous
	pool.IsDusty = false
	if !isBothDefined {
		avoidTokensNew[at.Address] = new(any)

	}

	fmt.Printf(
		"(%s) %s$ -> (%s) %s$ %s \n",
		dt.Symbol,
		dt.USDPrice.String(),
		at.Symbol,
		atUSDPrice.String(),
		pool.Address,
	)

	if token, ok := definedTokensMap[at.Address]; ok {
		impacts := token.GetImpacts()
		impactInt := new(big.Int)
		impactInt = GetImpactForPool(pool, int64(dt.Decimals), dt.USDPrice)

		impacts = append(
			impacts,
			&models.TokenPriceImpact{
				ChainID:            at.ChainID,
				USDPrice:           atUSDPrice,
				TokenAddress:       at.Address,
				ExchangeIdentifier: models.GetExchangeIdentifierForV3Pool(pool.ChainID, pool.Address),
				Impact:             impactInt, //in usd impact
			},
		)
		// fmt.Printf("adding  amount: %s -> %s, %s -> %s \n", token.getTotalUSDAmountReal().String(), token.Token.DefiUSDPrice.String(), pool.Liquidity.String(), anotherTokenPriceInUSD.String())

		token.SetImpacts(impacts)
		definedTokensMap[token.Address] = token
	} else {
		impactInt := new(big.Int)
		impactInt = GetImpactForPool(pool, int64(dt.Decimals), dt.USDPrice)

		impact := models.TokenPriceImpact{
			ChainID:            at.ChainID,
			USDPrice:           atUSDPrice,
			TokenAddress:       at.Address,
			ExchangeIdentifier: models.GetExchangeIdentifierForV3Pool(pool.ChainID, pool.Address),
			Impact:             impactInt, //in usd impact
		}
		at.SetImpacts([]*models.TokenPriceImpact{&impact})
		definedTokensMap[at.Address] = &at
	}

	return nil
}
