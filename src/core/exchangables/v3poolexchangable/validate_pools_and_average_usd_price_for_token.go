package v3poolexchangable

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/src/helpers"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/repo/tokenlist"
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

func ValidateV3PoolsAndGetAverageUSDPriceForTokens(
	pools []models.UniswapV3Pool,
	stableCoins []models.Token,
) (map[string]*big.Float, map[string]*models.UniswapV3Pool, error) {
	stableCoinsMap := map[string]models.Token{}
	for _, stableCoin := range stableCoins {
		stableCoinsMap[stableCoin.Address] = stableCoin
	}

	tokenList, err := tokenlist.New()
	if err != nil {
		return nil, nil, err
	}

	tokensMap := map[string]tokenWithUSDPrice{}
	checkedPools := map[string]any{}
	notDustyPools := map[string]*models.UniswapV3Pool{}

	defineStableTokens(stableCoinsMap, tokensMap)

	// How much away from stable coin can pairs checking token be
	checkingDeepness := 5
	for range checkingDeepness {
		for _, pool := range pools {
			if pool.Liquidity.Cmp(big.NewInt(0)) == 0 || pool.SqrtPriceX96.Cmp(big.NewInt(0)) == 0 {
				continue
			}

			checkPairForDefinedTokens(tokenList, tokensMap, checkedPools, &pool)
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
			delete(notDustyPools, key)
			continue
		}

		exchangablePool, err := NewV3ExchangablePool(pool, &token0.Token, &token1.Token)
		if err != nil {
			delete(notDustyPools, key)
			continue
		}

		amountOfRealToken0Needed := new(big.Float).Quo(big.NewFloat(1), token0.Token.DefiUSDPrice)
		amountNeeded0 := new(big.Float).
			Mul(
				amountOfRealToken0Needed,
				new(big.Float).SetInt(
					new(big.Int).Mul(
						new(big.Int).Exp(
							big.NewInt(10),
							big.NewInt(int64(token0.Token.Decimals)),
							nil,
						),
						//5 dollars
						big.NewInt(5),
					),
				),
			)
		amountNeeded0Int, _ := amountNeeded0.Int64()

		amountOfRealToken1Needed := new(big.Float).Quo(big.NewFloat(1), token1.Token.DefiUSDPrice)
		amountNeeded1 := new(big.Float).
			Mul(
				amountOfRealToken1Needed,
				new(big.Float).SetInt(
					new(big.Int).Mul(
						new(big.Int).Exp(
							big.NewInt(10),
							big.NewInt(int64(token1.Token.Decimals)),
							nil,
						),
						//5 dollars
						big.NewInt(5),
					),
				),
			)
		amountNeeded1Int, _ := amountNeeded1.Int64()

		zfo := true
		amount0Out, err := exchangablePool.ImitateSwap(big.NewInt(amountNeeded0Int), zfo)
		if pool.Address == "0x5d752f322befb038991579972e912b02f61a3dda" {
			fmt.Println("zfo")
			fmt.Println("token0In=", amountNeeded0Int)
			fmt.Println("token1Out=", amount0Out.String())
		}
		if err != nil {
			fmt.Println(
				"Not passed validation0: ",
				helpers.GetJSONString(pool),
			)
			delete(notDustyPools, key)
			continue
		}

		amount1Out, err := exchangablePool.ImitateSwap(big.NewInt(amountNeeded1Int), !zfo)
		if pool.Address == "0x5d752f322befb038991579972e912b02f61a3dda" {
			fmt.Println("!zfo")
			fmt.Println("token1In=", amountNeeded1Int)
			fmt.Println("token0Out=", amount1Out.String())

		}
		if err != nil {
			// fmt.Println(
			// 	"Not passed validation1: ",
			// 	helpers.GetJSONString(pool),
			// )
			delete(notDustyPools, key)
		}
	}

	tokenPricesMap := map[string]*big.Float{}
	for k, v := range tokensMap {
		tokenPricesMap[k] = v.Token.DefiUSDPrice
	}

	return tokenPricesMap, notDustyPools, nil
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

func getTokenHoldingOfToken(pool *models.UniswapV3Pool, tokenAddress string, decimals int64) (*big.Int, *big.Float, error) {
	tokenHolding := new(big.Int)
	if pool.Token0 == tokenAddress {
		tokenHolding = pool.Token0Holding
	} else if pool.Token1 == tokenAddress {
		tokenHolding = pool.Token1Holding
	} else {
		return nil, nil, errors.New("cannot get token holding, token not found in pool")
	}

	tokenAmount := new(big.Float).SetInt(tokenHolding)

	bDecimals := big.NewInt(decimals)
	pow := new(big.Int).Exp(bTen, bDecimals, nil)
	powF := new(big.Float).SetInt(pow)

	return tokenHolding, tokenAmount.Quo(tokenAmount, powF), nil
}

func defineStableTokens(stableCoinsMap map[string]models.Token, tokensMap map[string]tokenWithUSDPrice) {
	for _, stableCoin := range stableCoinsMap {
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
	tokenList *tokenlist.TokenList,
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

	// Mark that checked this pair for liquidity

	atData, err := tokenList.GetTokenByAddress(atAddress)
	if err != nil {
		fmt.Println("Another token not found ============================================", atAddress, helpers.GetJSONString(pool))
		return
	}

	anotherToken := models.Token{
		Name:     atData.Name,
		Symbol:   atData.Symbol,
		Address:  atData.Address,
		ChainID:  atData.ChainID,
		LogoURI:  atData.LogoURI,
		Decimals: atData.Decimals,
	}

	dtTokenHolding, dtTokenHoldingReal, err := getTokenHoldingOfToken(pool, dt.Token.Address, int64(dt.Token.Decimals))
	if err != nil {
		fmt.Println("Error getting real amoujnt of defined token: ", err)
		return
	}

	amountOfAnotherTokenHolding, realAmountOfAnotherTokenHolding, err := getTokenHoldingOfToken(pool, anotherToken.Address, int64(anotherToken.Decimals))
	if err != nil {
		fmt.Println("Error getting real amoujnt of usd: ", err)
		return
	}

	if dtTokenHolding.Cmp(big.NewInt(0)) == 0 ||
		amountOfAnotherTokenHolding.Cmp(big.NewInt(0)) == 0 ||
		pool.Liquidity.Cmp(big.NewInt(0)) == 0 {
		return
	}

	tokenPrice, err := getPrice(pool, anotherToken.Address, dt.Token.Decimals, anotherToken.Decimals)
	if err != nil {
		fmt.Println("unable to get price: ", err)
		return
	}

	dtUsdPrice := dt.countAverageUSDPrice()
	anotherTokenPriceInUSD := new(big.Float).Mul(tokenPrice, dtUsdPrice)

	amountOfDtHoldingInUSD := new(big.Float).Mul(dtUsdPrice, dtTokenHoldingReal)
	amountOfAnotherTokenHoldingInUSD := new(big.Float).Mul(realAmountOfAnotherTokenHolding, anotherTokenPriceInUSD)

	if dt.Token.Symbol == anotherToken.Symbol {
		fmt.Println("HELLO? ", pool)
		fmt.Println("DT:? ", dt)
		fmt.Println("anotherToken:? ", anotherToken)
	}

	// If token already added check if new amount of usd provided by pair is bigger than previous
	if amountOfAnotherTokenHoldingInUSD.Cmp(big.NewFloat(10000)) > 0 &&
		amountOfDtHoldingInUSD.Cmp(big.NewFloat(10000)) > 0 {

		pool.IsDusty = false
		checkedPools[pool.Address] = new(any)

		fmt.Println("amountOfDt: ", dtTokenHoldingReal)
		fmt.Println("amountOfAnother: ", realAmountOfAnotherTokenHolding)

		fmt.Printf(
			"(%s) -> (%s) %s: \n\t %s$ %s$ \n (%s): %s$ (%s):%s$ \n\n",
			dt.Token.Symbol,
			anotherToken.Symbol,
			pool.Address,
			amountOfDtHoldingInUSD.String(),
			amountOfAnotherTokenHoldingInUSD.String(),
			dt.Token.Symbol,
			dtUsdPrice.String(),
			anotherToken.Symbol,
			anotherTokenPriceInUSD.String(),
		)

		if token, ok := tokensMap[anotherToken.Address]; ok {
			token.USDPriceAmountToPrice = append(
				token.USDPriceAmountToPrice,
				usdAmountProvidedToPrice{
					TokenAmount: amountOfAnotherTokenHolding,
					Price:       anotherTokenPriceInUSD,
				},
			)
			fmt.Printf("adding  amount: %s -> %s, %s -> %s \n", token.getTotalUSDAmountReal().String(), token.Token.DefiUSDPrice.String(), amountOfAnotherTokenHoldingInUSD.String(), anotherTokenPriceInUSD.String())

			token.Token.DefiUSDPrice = token.countAverageUSDPrice()
			tokensMap[token.Token.Address] = token
		} else {
			newToken := tokenWithUSDPrice{
				Token: anotherToken,
				USDPriceAmountToPrice: []usdAmountProvidedToPrice{
					{
						TokenAmount: amountOfAnotherTokenHolding,
						Price:       anotherTokenPriceInUSD,
					},
				},
			}

			newToken.Token.DefiUSDPrice = anotherTokenPriceInUSD
			tokensMap[anotherToken.Address] = newToken
		}
	} else {
		pool.IsDusty = true
	}
}
