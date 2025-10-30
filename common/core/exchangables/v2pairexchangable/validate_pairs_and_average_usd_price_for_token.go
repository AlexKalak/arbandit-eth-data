package v2pairexchangable

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

func ValidateV2PairsAndGetAverageUSDPriceForTokens(pairs []models.UniswapV2Pair, stableCoins []models.Token) (map[string]*big.Float, map[string]*models.UniswapV2Pair, error) {
	stableCoinsMap := map[string]models.Token{}
	for _, stableCoin := range stableCoins {
		stableCoinsMap[stableCoin.Address] = stableCoin
	}

	tokenList, err := tokenlist.New()
	if err != nil {
		return nil, nil, err
	}

	tokensMap := map[string]tokenWithUSDPrice{}
	checkedPairs := map[string]any{}
	notDustyPairs := map[string]*models.UniswapV2Pair{}

	defineStableTokens(stableCoinsMap, tokensMap)

	//How much away from stable coin can pairs checking token be
	checkingDeepness := 100
	for range checkingDeepness {
		for _, pair := range pairs {
			checkPairForDefinedTokens(tokenList, tokensMap, checkedPairs, &pair)
			if !pair.IsDusty {
				if _, ok := notDustyPairs[pair.Address]; !ok {
					notDustyPairs[pair.Address] = &pair
				}
			}
		}
	}

	for _, token := range tokensMap {
		fmt.Printf("(%s) -> %s \n", token.Token.Symbol, token.countAverageUSDPrice().String())
	}

	tokenPricesMap := map[string]*big.Float{}
	for k, v := range tokensMap {
		tokenPricesMap[k] = v.Token.DefiUSDPrice
	}

	return tokenPricesMap, notDustyPairs, nil
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

func checkPairForDefinedTokens(tokenList *tokenlist.TokenList, tokensMap map[string]tokenWithUSDPrice, checkedPairs map[string]any, pair *models.UniswapV2Pair) {
	if _, ok := checkedPairs[pair.Address]; ok {
		return
	}

	dt := tokenWithUSDPrice{}
	dt0, hasDefinedToken0 := tokensMap[pair.Token0]
	dt1, hasDefinedToken1 := tokensMap[pair.Token1]
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
		atAddress = pair.Token1
	} else if hasDefinedToken1 {
		dt = dt1
		atAddress = pair.Token0
	} else {
		return
	}

	atData, err := tokenList.GetTokenByAddress(atAddress)
	if err != nil {
		fmt.Println("Another token not found ============================================", atAddress, helpers.GetJSONString(pair))
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

	amountOfDefinedToken, realAmountOfDt, err := getCoinAmountInPair(pair, dt.Token.Address, int64(dt.Token.Decimals))
	if err != nil {
		fmt.Println("Error getting real amoujnt of defined token: ", err)
		return
	}

	amountOfAnotherToken, realAmountOfAnotherToken, err := getCoinAmountInPair(pair, anotherToken.Address, int64(anotherToken.Decimals))
	if err != nil {
		fmt.Println("Error getting real amoujnt of usd: ", err)
		return
	}

	if amountOfAnotherToken.Cmp(big.NewInt(0)) == 0 || amountOfDefinedToken.Cmp(big.NewInt(0)) == 0 {
		return
	}

	tokenPrice := getPrice(amountOfAnotherToken, amountOfDefinedToken, int64(anotherToken.Decimals), int64(dt.Token.Decimals))

	anotherTokenPriceInUSD := new(big.Float).Mul(tokenPrice, dt.countAverageUSDPrice())
	amountOfDtInUSD := new(big.Float).Mul(dt.countAverageUSDPrice(), realAmountOfDt)

	amountOfAnotherTokenInUSD := new(big.Float).Mul(anotherTokenPriceInUSD, realAmountOfAnotherToken)

	// If token already added check if new amount of usd provided by pair is bigger than previous
	if amountOfDtInUSD.Cmp(big.NewFloat(10000)) > 0 &&
		amountOfAnotherTokenInUSD.Cmp(big.NewFloat(10000)) > 0 {

		checkedPairs[pair.Address] = new(any)
		pair.IsDusty = false

		if token, ok := tokensMap[anotherToken.Address]; ok {
			fmt.Printf("adding  amount: %s -> %s, %s -> %s \n", token.getTotalUSDAmountReal().String(), token.Token.DefiUSDPrice.String(), amountOfAnotherTokenInUSD.String(), anotherTokenPriceInUSD.String())
			token.USDPriceAmountToPrice = append(
				token.USDPriceAmountToPrice,
				usdAmountProvidedToPrice{
					TokenAmount: amountOfAnotherToken,
					Price:       anotherTokenPriceInUSD,
				},
			)

			token.Token.DefiUSDPrice = token.countAverageUSDPrice()
			tokensMap[token.Token.Address] = token
		} else {

			newToken := tokenWithUSDPrice{
				Token: anotherToken,
				USDPriceAmountToPrice: []usdAmountProvidedToPrice{
					{
						TokenAmount: amountOfAnotherToken,
						Price:       anotherTokenPriceInUSD,
					},
				},
			}

			newToken.Token.DefiUSDPrice = anotherTokenPriceInUSD
			tokensMap[newToken.Token.Address] = newToken
		}
	}
}

func getPrice(amount0 *big.Int, amount1 *big.Int, dec0 int64, dec1 int64) *big.Float {
	power0 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(dec0), nil))
	power1 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(dec1), nil))

	realAmount0 := new(big.Float).Quo(new(big.Float).SetInt(amount0), power0)
	realAmount1 := new(big.Float).Quo(new(big.Float).SetInt(amount1), power1)

	price := new(big.Float).Quo(realAmount1, realAmount0)
	return price
}

func getCoinAmountInPair(pair *models.UniswapV2Pair, tokenAddress string, decimals int64) (*big.Int, *big.Float, error) {
	tokenAmountInt := new(big.Int)
	if pair.Token0 == tokenAddress {
		tokenAmountInt.Set(pair.Amount0)
	} else if pair.Token1 == tokenAddress {
		tokenAmountInt.Set(pair.Amount1)
	} else {
		return nil, nil, errors.New("no token in pair")
	}
	tokenAmount := new(big.Float).SetInt(tokenAmountInt)

	bDecimals := big.NewInt(decimals)
	pow := new(big.Int).Exp(bTen, bDecimals, nil)
	powF := new(big.Float).SetInt(pow)

	return tokenAmountInt, tokenAmount.Quo(tokenAmount, powF), nil
}
