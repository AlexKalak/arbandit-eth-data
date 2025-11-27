package models

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
)

const TOKENS_TABLE = "tokens"
const TOKEN_NAME = "name"
const TOKEN_SYMBOL = "symbol"
const TOKEN_ADDRESS = "address"
const TOKEN_CHAINID = "chain_id"
const TOKEN_LOGOURI = "logo_uri"
const TOKEN_DECIMALS = "decimals"
const TOKEN_USD_PRICE = "usd_price"

type TokenIdentificator struct {
	Address string
	ChainID uint
}

func (t *TokenIdentificator) String() string {
	return fmt.Sprintf("%d_%s", t.ChainID, t.Address)
}

type Token struct {
	mu       sync.Mutex
	Name     string
	Symbol   string
	Address  string
	ChainID  uint
	LogoURI  string
	Decimals int
	USDPrice *big.Float

	impacts []*TokenPriceImpact
}

func (t *Token) GetImpacts() []*TokenPriceImpact {
	return t.impacts
}

func (t *Token) SetImpacts(impacts []*TokenPriceImpact) {
	t.mu.Lock()
	t.impacts = impacts
	t.mu.Unlock()
	var err error
	t.USDPrice, err = t.AveragePrice()
	if err != nil {
		t.USDPrice = big.NewFloat(0)
	}
}

func (t *Token) GetRealAmount(amount *big.Int) *big.Float {
	exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Decimals)), nil)
	return new(big.Float).Quo(new(big.Float).SetInt(amount), new(big.Float).SetInt(exp))

}

func (t *Token) FromUSD(amountUSD *big.Int) *big.Int {
	amountRealForOneUSD := new(big.Float).Quo(big.NewFloat(1), t.USDPrice)
	amountRealNeeded := new(big.Float).Mul(amountRealForOneUSD, new(big.Float).SetInt(amountUSD))

	amount := new(big.Float).Mul(new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Decimals)), nil)), amountRealNeeded)
	amountInt, _ := amount.Int(nil)

	return amountInt
}

func (t *Token) GetIdentificator() TokenIdentificator {
	return TokenIdentificator{
		Address: t.Address,
		ChainID: t.ChainID,
	}
}

func (t *Token) GetTotalImpact() *big.Int {
	total := big.NewInt(0)
	t.mu.Lock()
	for _, impact := range t.impacts {
		total.Add(total, impact.Impact)
	}
	t.mu.Unlock()

	return total
}

func (t *Token) GetTotalImpactInUSD() *big.Int {
	totalImpactInUSD := big.NewFloat(0)

	t.mu.Lock()
	for _, impact := range t.impacts {
		totalImpactInUSD.Add(totalImpactInUSD, new(big.Float).Mul(new(big.Float).SetInt(impact.Impact), impact.USDPrice))
	}
	t.mu.Unlock()

	res := new(big.Int)
	totalImpactInUSD.Int(res)

	return res
}

func (t *Token) GetTotalImpactInUSDReal() *big.Float {
	totalImpactInUSD := big.NewFloat(0)

	t.mu.Lock()
	for _, impact := range t.impacts {
		totalImpactInUSD.Add(totalImpactInUSD, new(big.Float).Mul(new(big.Float).SetInt(impact.Impact), impact.USDPrice))
	}
	t.mu.Unlock()

	decimalsPower := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Decimals)), nil)

	realAmountInUSD := new(big.Float).Quo(totalImpactInUSD, new(big.Float).SetInt(decimalsPower))
	return realAmountInUSD
}

func (t *Token) AveragePrice() (*big.Float, error) {
	totalTokenAmount := new(big.Float).SetInt(t.GetTotalImpact())
	totalImpactInUSD := t.GetTotalImpactInUSD()
	if totalImpactInUSD.Cmp(big.NewInt(0)) == 0 ||
		totalTokenAmount.Cmp(big.NewFloat(0)) == 0 {
		return big.NewFloat(0), errors.New("cannot estimate token average price, because there is no impact")
	}

	return new(big.Float).Quo(new(big.Float).SetInt(totalImpactInUSD), totalTokenAmount), nil
}

const (
	TOKEN_PRICE_IMPACT_TABLE = "token_price_impacts"

	TOKEN_PRICE_IMPACT_CHAIN_ID            = "chain_id"
	TOKEN_PRICE_IMPACT_TOKEN_ADDRESS       = "token_address"
	TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER = "exchange_identifier"
	TOKEN_PRICE_IMPACT_IMPACT              = "impact" //in usd impact
	TOKEN_PRICE_IMPACT_USD_PRICE           = "usd_price"
)

type TokenPriceImpact struct {
	ChainID            uint
	TokenAddress       string
	ExchangeIdentifier string //eg v3.chainID.poolAddress or cex.bybit.bybit_spot_identifier
	Impact             *big.Int
	USDPrice           *big.Float
}

func GetExchangeIdentifierForV3Pool(chainID uint, poolAddress string) string {
	return fmt.Sprintf("v3pool_%d_%s", chainID, poolAddress)
}

func GetExchangeIdentifierForV2Pair(chainID uint, pairAddress string) string {
	return fmt.Sprintf("v2pair_%d_%s", chainID, pairAddress)
}
