package v3poolexchangable

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
)

var bTen = big.NewInt(10)

type usdAmountProvidedToPrice struct {
	TokenAmount *big.Int
	Price       *big.Float
}

type tokenWithUSDPrice struct {
	Token                 models.Token
	USDPriceAmountToPrice []usdAmountProvidedToPrice
}

func (t *tokenWithUSDPrice) getTotalTokenAmount() *big.Int {
	total := big.NewInt(0)
	for _, amount2Price := range t.USDPriceAmountToPrice {
		total.Add(total, amount2Price.TokenAmount)
	}

	return total
}

func (t *tokenWithUSDPrice) getTotalUSDAmountInt() *big.Int {
	totalAmountInUSD := big.NewFloat(0)

	for _, amount2Price := range t.USDPriceAmountToPrice {
		totalAmountInUSD.Add(totalAmountInUSD, new(big.Float).Mul(new(big.Float).SetInt(amount2Price.TokenAmount), amount2Price.Price))
	}

	res := new(big.Int)
	totalAmountInUSD.Int(res)

	return res
}

func (t *tokenWithUSDPrice) getTotalUSDAmountReal() *big.Float {
	totalAmountInUSD := big.NewFloat(0)

	for _, amount2Price := range t.USDPriceAmountToPrice {
		totalAmountInUSD.Add(totalAmountInUSD, new(big.Float).Mul(new(big.Float).SetInt(amount2Price.TokenAmount), amount2Price.Price))
	}

	decimalsPower := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Token.Decimals)), nil)

	realAmountInUSD := new(big.Float).Quo(totalAmountInUSD, new(big.Float).SetInt(decimalsPower))
	return realAmountInUSD
}

func (t *tokenWithUSDPrice) countAverageUSDPrice() *big.Float {
	totalTokenAmount := new(big.Float).SetInt(t.getTotalTokenAmount())
	totalAmountInUSD := t.getTotalUSDAmountInt()

	return new(big.Float).Quo(new(big.Float).SetInt(totalAmountInUSD), totalTokenAmount)
}

func getNeededAmountInUSD(USDPrice *big.Float, amountUSD *big.Int, decimals int64) *big.Int {
	dtAmountRealForOneUSD := new(big.Float).Quo(big.NewFloat(1), USDPrice)
	dtAmountRealNeeded := new(big.Float).Mul(dtAmountRealForOneUSD, new(big.Float).SetInt(amountUSD))

	dtAmount := new(big.Float).Mul(new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil)), dtAmountRealNeeded)
	dtAmountInt, _ := dtAmount.Int(nil)

	return dtAmountInt

}

func ValidateV3PoolsAndGetAverageUSDPriceForTokens(
	chainID uint,
	allTokens []models.Token,
	pools []models.UniswapV3Pool,
	stableCoins []models.Token,
) (map[string]*big.Float, map[string]*models.UniswapV3Pool, error) {
	stableCoinsMap := map[string]models.Token{}
	for _, stableCoin := range stableCoins {
		stableCoinsMap[stableCoin.Address] = stableCoin
	}

	tokenDataMap := map[string]*models.Token{}
	for _, token := range allTokens {
		if token.ChainID != chainID {
			continue
		}
		tokenDataMap[token.Address] = &token
	}

	tokensMap := map[string]tokenWithUSDPrice{}
	checkedPools := map[string]any{}
	notDustyPools := map[string]*models.UniswapV3Pool{}

	defineStableTokens(stableCoinsMap, tokensMap)

	// How much away from stable coin can pairs checking token be
	checkingDeepness := 4
	for range checkingDeepness {
		fmt.Println("Checking deepness: ", checkingDeepness)
		for _, pool := range pools {
			if pool.Liquidity.Cmp(big.NewInt(0)) == 0 || pool.SqrtPriceX96.Cmp(big.NewInt(0)) == 0 {
				continue
			}

			checkPairForDefinedTokens(tokenDataMap, tokensMap, checkedPools, &pool)
			if !pool.IsDusty {
				if _, ok := notDustyPools[pool.Address]; !ok {
					notDustyPools[pool.Address] = &pool
				}
			}
		}
	}

	fmt.Println("not dusty pools before checking: ", len(notDustyPools))

	//Check imitate swap to tell if pool is inactive
	for key, pool := range notDustyPools {
		token0, ok1 := tokensMap[pool.Token0]
		token1, ok2 := tokensMap[pool.Token1]

		if !ok1 || !ok2 {
			fmt.Println("Not ok")
			delete(notDustyPools, key)
			continue
		}

		err := UpdateRateFor10USD(pool, &token0.Token, &token1.Token)
		if err != nil {
			continue
		}
	}

	tokenPricesMap := map[string]*big.Float{}
	for k, v := range tokensMap {
		tokenPricesMap[k] = v.Token.DefiUSDPrice
	}

	return tokenPricesMap, notDustyPools, nil
}

func UpdateRateFor10USD(pool *models.UniswapV3Pool, token0 *models.Token, token1 *models.Token) error {
	if token0.DefiUSDPrice.Cmp(big.NewFloat(0)) == 0 ||
		token1.DefiUSDPrice.Cmp(big.NewFloat(0)) == 0 {
		fmt.Println("invalid defi price", token0.DefiUSDPrice, token1.DefiUSDPrice)
		fmt.Println(helpers.GetJSONString(token0))
		fmt.Println(helpers.GetJSONString(token1))
		return errors.New("invalid defi price")
	}

	exchangablePool, err := NewV3ExchangablePool(pool, token0, token1)
	if err != nil {
		fmt.Println("Unable to init exchangablePool")
		return errors.New("invalid defi price")
	}

	usdAmount := big.NewInt(10)

	amount0Init := getNeededAmountInUSD(token0.DefiUSDPrice, usdAmount, int64(token0.Decimals))
	amount1Init := getNeededAmountInUSD(token1.DefiUSDPrice, usdAmount, int64(token1.Decimals))

	amount1Out, err := exchangablePool.ImitateSwap(amount0Init, true)
	if err != nil {
		fmt.Println("Not passed zfo: ", helpers.GetJSONString(pool))
		return err
	}
	reverted0, err := exchangablePool.ImitateSwap(amount1Out, false)
	if err != nil {
		fmt.Println("Not passed reverted zfo: ", helpers.GetJSONString(pool))
		return err
	}

	amount0Out, err := exchangablePool.ImitateSwap(amount1Init, false)
	if err != nil {
		fmt.Println("Not passed !zfo: ", helpers.GetJSONString(pool))
		return err
	}
	reverted1, err := exchangablePool.ImitateSwap(amount0Out, true)
	if err != nil {
		fmt.Println("Not passed reverted !zfo: ", helpers.GetJSONString(pool))
		return err
	}

	percentZfo := new(big.Float).Quo(new(big.Float).SetInt(reverted0), new(big.Float).SetInt(amount0Init))
	if percentZfo.Cmp(big.NewFloat(0.97)) < 0 {
		fmt.Println("Not passed percent zfo: ", helpers.GetJSONString(pool))
		fmt.Println(percentZfo.Text('f', 2))
		return err
	}
	percentNonZfo := new(big.Float).Quo(new(big.Float).SetInt(reverted1), new(big.Float).SetInt(amount1Init))
	if percentNonZfo.Cmp(big.NewFloat(0.97)) < 0 {
		fmt.Println("Not passed percent !zfo: ", helpers.GetJSONString(pool))
		fmt.Println(percentNonZfo.Text('f', 2))
		return err
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

func getPrice(pool *models.UniswapV3Pool, tokenAddress string, dtDecimals int, tokenDecimals int) (*big.Float, error) {
	// always token1 -> token0
	price := divideSqrtPriceX96(pool.SqrtPriceX96)

	price.Mul(price, price)

	if pool.Token0 == tokenAddress {
		dtDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(dtDecimals)), nil)
		tokenDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenDecimals)), nil)

		price.Mul(price, new(big.Float).SetInt(tokenDecimalsPow))
		price.Quo(price, new(big.Float).SetInt(dtDecimalsPow))

		return price, nil
	} else if pool.Token1 == tokenAddress {
		dtDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(dtDecimals)), nil)
		tokenDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenDecimals)), nil)

		price.Mul(price, new(big.Float).SetInt(dtDecimalsPow))
		price.Quo(price, new(big.Float).SetInt(tokenDecimalsPow))
		price.Quo(big.NewFloat(1), price)
		return price, nil
	}
	return nil, errors.New("cannot get price, token not found in pool")
}

func defineStableTokens(stableCoinsMap map[string]models.Token, tokensMap map[string]tokenWithUSDPrice) {
	for _, stableCoin := range stableCoinsMap {
		stableCoin.DefiUSDPrice = big.NewFloat(1)
		tokensMap[stableCoin.Address] = tokenWithUSDPrice{
			Token: stableCoin,
			USDPriceAmountToPrice: []usdAmountProvidedToPrice{
				{
					TokenAmount: new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil),
					Price:       big.NewFloat(1),
				},
			},
		}
	}
}

func checkPairForDefinedTokens(
	tokenDataMap map[string]*models.Token,
	tokensMap map[string]tokenWithUSDPrice,
	checkedPools map[string]any,
	pool *models.UniswapV3Pool,
) {
	if _, ok := checkedPools[pool.Address]; ok {
		return
	}

	dt := tokenWithUSDPrice{}
	dt0, hasDefinedToken0 := tokensMap[pool.Token0]
	dt1, hasDefinedToken1 := tokensMap[pool.Token1]
	atAddress := ""

	if hasDefinedToken0 && hasDefinedToken1 {
		if dt0.getTotalUSDAmountInt().Cmp(dt1.getTotalUSDAmountInt()) > 0 {
			dt = dt0
			atAddress = dt1.Token.Address
		} else {
			dt = dt1
			atAddress = dt0.Token.Address
		}
	} else if hasDefinedToken0 {
		dt = dt0
		atAddress = pool.Token1
	} else if hasDefinedToken1 {
		dt = dt1
		atAddress = pool.Token0
	} else {
		return
	}

	isDTZero := pool.Token0 == dt.Token.Address

	// Mark that checked this pair for liquidity

	atData, ok := tokenDataMap[atAddress]
	if !ok {
		fmt.Println("Another token not found ============================================", atAddress, helpers.GetJSONString(pool))
		return
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

	dtUSDPrice := dt.countAverageUSDPrice()

	//Changing dt for another token
	dtAmountFor10USD := getNeededAmountInUSD(dtUSDPrice, big.NewInt(10), int64(dt.Token.Decimals))

	atOutFor10USD, err := exchangable.ImitateSwap(dtAmountFor10USD, isDTZero)
	if err != nil {
		return
	}

	resultAmountOfDt, err := exchangable.ImitateSwap(atOutFor10USD, !isDTZero)
	if err != nil {
		return
	}

	percent := new(big.Float).Quo(new(big.Float).SetInt(resultAmountOfDt), new(big.Float).SetInt(dtAmountFor10USD))
	if percent.Cmp(big.NewFloat(0.97)) < 0 {
		return
	}

	// fmt.Println(dt.Token.Address, dt.Token.Decimals, dtAmountFor10USD.String(), "->", resultAmountOfDt.String())

	tokenPrice, err := getPrice(pool, at.Address, dt.Token.Decimals, at.Decimals)
	if err != nil {
		fmt.Println("unable to get price: ", err)
		return
	}
	atUSDPrice := new(big.Float).Mul(tokenPrice, dtUSDPrice)

	if atUSDPrice.Cmp(big.NewFloat(0)) == 0 {
		fmt.Println("zero at usd price: ", atUSDPrice.String())
	}
	// If token already added check if new amount of usd provided by pair is bigger than previous
	pool.IsDusty = false

	fmt.Printf(
		"(%s) -> (%s) %s: \n\t (%s): %s$ (%s):%s$ \n\n",
		dt.Token.Symbol,
		at.Symbol,
		pool.Address,
		dt.Token.Symbol,
		dtUSDPrice.String(),
		at.Symbol,
		atUSDPrice.String(),
	)

	if token, ok := tokensMap[at.Address]; ok {
		token.USDPriceAmountToPrice = append(
			token.USDPriceAmountToPrice,
			usdAmountProvidedToPrice{
				TokenAmount: new(big.Int).Div(pool.Liquidity, big.NewInt(int64(dt.Token.Decimals))),
				Price:       atUSDPrice,
			},
		)
		// fmt.Printf("adding  amount: %s -> %s, %s -> %s \n", token.getTotalUSDAmountReal().String(), token.Token.DefiUSDPrice.String(), pool.Liquidity.String(), anotherTokenPriceInUSD.String())

		token.Token.DefiUSDPrice = token.countAverageUSDPrice()
		tokensMap[token.Token.Address] = token
	} else {
		newToken := tokenWithUSDPrice{
			Token: at,
			USDPriceAmountToPrice: []usdAmountProvidedToPrice{
				{
					TokenAmount: pool.Liquidity,
					Price:       atUSDPrice,
				},
			},
		}

		newToken.Token.DefiUSDPrice = atUSDPrice
		tokensMap[at.Address] = newToken
	}
}
