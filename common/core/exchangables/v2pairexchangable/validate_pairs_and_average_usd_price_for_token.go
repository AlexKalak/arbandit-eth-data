package v2pairexchangable

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
)

func ValidateV2PairsAndGetAverageUSDPriceForTokens(
	chainID uint,
	allTokens []*models.Token,
	pairs []models.UniswapV2Pair,
	stableCoins []*models.Token,
) ([]*models.Token, map[string]*models.UniswapV2Pair, error) {
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
	notDustyPools := map[string]*models.UniswapV2Pair{}

	defineStableTokens(stableCoinsMap, definedTokensMap)

	// How much away from stable coin can pairs checking token be
	checkingDeepness := 4
	for i := range checkingDeepness {
		amountErrs := 0
		fmt.Println("step: ", i)
		avoidTokensNew := map[string]any{}

		for _, pair := range pairs {

			if pair.Amount0.Cmp(big.NewInt(0)) == 0 || pair.Amount1.Cmp(big.NewInt(0)) == 0 {
				continue
			}

			err := checkPairForDefinedTokens(avoidTokensNew, tokenDataMap, definedTokensMap, checkedPools, &pair)
			if err != nil {
				amountErrs++
			}
			if !pair.IsDusty {
				if _, ok := notDustyPools[pair.Address]; !ok {
					notDustyPools[pair.Address] = &pair
				}
			}
		}

		fmt.Println("Amount err: ", amountErrs)
	}

	fmt.Println("not dusty pools before checking: ", len(notDustyPools))

	//Check imitate swap to tell if pool is inactive
	for key, pair := range notDustyPools {
		token0, ok1 := definedTokensMap[pair.Token0]
		token1, ok2 := definedTokensMap[pair.Token1]

		if !ok1 || !ok2 {
			fmt.Println("Not ok")
			delete(notDustyPools, key)
			continue
		}

		err := UpdateRateFor10USD(pair, token0, token1)
		if err != nil {
			fmt.Println(pair.Address, err)
			delete(notDustyPools, key)
			continue
		}
	}
	fmt.Println("not dusty pools after checking: ", len(notDustyPools))

	updatedTokens := make([]*models.Token, 0, len(definedTokensMap))
	for _, v := range definedTokensMap {
		updatedTokens = append(updatedTokens, v)
	}

	return updatedTokens, notDustyPools, nil
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
	checkedPairs map[string]any,
	pair *models.UniswapV2Pair,
) error {
	if _, ok := checkedPairs[pair.Address]; ok {
		return nil
	}

	dt := &models.Token{}
	dt0, hasDefinedToken0 := definedTokensMap[pair.Token0]
	dt1, hasDefinedToken1 := definedTokensMap[pair.Token1]
	atAddress := ""

	_, avoidDt0 := avoidTokensNew[pair.Token0]
	_, avoidDt1 := avoidTokensNew[pair.Token1]

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
		atAddress = pair.Token1
	} else if hasDefinedToken1 {
		if avoidDt1 {
			return nil
		}

		dt = dt1
		atAddress = pair.Token0
	} else {
		return nil
	}

	isDTZero := pair.Token0 == dt.Address

	// Mark that checked this pair for liquidity
	atData, ok := tokenDataMap[atAddress]
	if !ok {
		fmt.Println("Another token not found ============================================", atAddress, helpers.GetJSONString(pair))
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

	exchangable := ExchangableUniswapV2PairRaw{
		Pair: pair,
	}

	pair.IsDusty = true

	checkedPairs[pair.Address] = new(any)

	pair.Zfo10USDRate = big.NewFloat(0)
	pair.NonZfo10USDRate = big.NewFloat(0)

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

	tokenRate, err := GetRateForPairReal(pair, at.Address, dt.Decimals, at.Decimals)
	if err != nil {
		return err
	}

	atUSDPrice := new(big.Float).Mul(tokenRate, dt.USDPrice)
	if atUSDPrice.Cmp(big.NewFloat(0)) == 0 {
		return nil
	}

	// If token already added check if new amount of usd provided by pair is bigger than previous
	pair.IsDusty = false
	if !isBothDefined {
		avoidTokensNew[at.Address] = new(any)

	}

	fmt.Printf(
		"(%s) %s$ -> (%s) %s$ %s \n",
		dt.Symbol,
		dt.USDPrice.String(),
		at.Symbol,
		atUSDPrice.String(),
		pair.Address,
	)

	atAmount := pair.Amount0
	if isDTZero {
		atAmount = pair.Amount1
	}

	if token, ok := definedTokensMap[at.Address]; ok {
		impacts := token.GetImpacts()
		impactInt := new(big.Int)
		impactInt = atAmount

		impacts = append(
			impacts,
			&models.TokenPriceImpact{
				ChainID:            at.ChainID,
				USDPrice:           atUSDPrice,
				TokenAddress:       at.Address,
				ExchangeIdentifier: models.GetExchangeIdentifierForV2Pair(pair.ChainID, pair.Address),
				Impact:             impactInt, //in usd impact
			},
		)

		token.SetImpacts(impacts)
		definedTokensMap[token.Address] = token
	} else {
		impactInt := new(big.Int)
		impactInt = atAmount

		impact := models.TokenPriceImpact{
			ChainID:            at.ChainID,
			USDPrice:           atUSDPrice,
			TokenAddress:       at.Address,
			ExchangeIdentifier: models.GetExchangeIdentifierForV2Pair(pair.ChainID, pair.Address),
			Impact:             impactInt, //in usd impact
		}
		at.SetImpacts([]*models.TokenPriceImpact{&impact})
		definedTokensMap[at.Address] = &at
	}

	return nil
}

var bTen = big.NewInt(10)

func GetRateForPairReal(pair *models.UniswapV2Pair, tokenAddress string, tokenOutDecimals int, tokenInDecimals int) (*big.Float, error) {
	ex := ExchangableUniswapV2PairRaw{
		Pair: pair,
	}

	// always token1/token0
	rate := ex.GetRate(true)

	if pair.Token0 == tokenAddress {
		dtDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenOutDecimals)), nil)
		tokenDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenInDecimals)), nil)

		rate.Mul(rate, new(big.Float).SetInt(tokenDecimalsPow))
		rate.Quo(rate, new(big.Float).SetInt(dtDecimalsPow))

		return rate, nil
	} else if pair.Token1 == tokenAddress {
		dtDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenOutDecimals)), nil)
		tokenDecimalsPow := new(big.Int).Exp(bTen, big.NewInt(int64(tokenInDecimals)), nil)

		rate.Mul(rate, new(big.Float).SetInt(dtDecimalsPow))
		rate.Quo(rate, new(big.Float).SetInt(tokenDecimalsPow))
		rate.Quo(big.NewFloat(1), rate)
		return rate, nil
	}
	return nil, errors.New("cannot get price, token not found in pool")
}
