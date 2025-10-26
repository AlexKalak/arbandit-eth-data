package v2pairexchangable

import (
	"math/big"

	"github.com/alexkalak/go_market_analyze/src/errors/exchangeerrors"
	"github.com/alexkalak/go_market_analyze/src/models"
)

type ExchangableUniswapV2Pair struct {
	Pair   *models.UniswapV2Pair
	Token0 *models.Token
	Token1 *models.Token
}

func New(pair *models.UniswapV2Pair, token0 *models.Token, token1 *models.Token) (ExchangableUniswapV2Pair, error) {
	if pair == nil || token1 == nil || token0 == nil {
		return ExchangableUniswapV2Pair{}, exchangeerrors.ErrInvalidArgsOnExchangablePair
	}

	return ExchangableUniswapV2Pair{
		Pair:   pair,
		Token0: token0,
		Token1: token1,
	}, nil
}

func (e *ExchangableUniswapV2Pair) ImitateSwap(amountIn *big.Int, zfo bool) (*big.Int, error) {
	// feeNumerator := big.NewInt(int64(100*10000 - e.Pair.FeeTier))
	feeNumerator := big.NewInt(100*10000 - int64(e.Pair.FeeTier))
	feeDenominator := big.NewInt(100 * 10000)

	amountInWithFee := new(big.Int).Mul(amountIn, feeNumerator)

	amountOut := new(big.Int)

	if zfo {
		numerator := new(big.Int).Mul(amountInWithFee, e.Pair.Amount1)
		denominator := new(big.Int).Add(
			new(big.Int).Mul(e.Pair.Amount0, feeDenominator),
			amountInWithFee,
		)

		amountOut.Div(numerator, denominator)
	} else {
		numerator := new(big.Int).Mul(amountInWithFee, e.Pair.Amount0)
		denominator := new(big.Int).Add(
			new(big.Int).Mul(e.Pair.Amount1, feeDenominator),
			amountInWithFee,
		)

		amountOut.Div(numerator, denominator)
	}

	return amountOut, nil
}

func (e *ExchangableUniswapV2Pair) GetToken0() *models.Token {
	return e.Token0
}

func (e *ExchangableUniswapV2Pair) GetToken1() *models.Token {
	return e.Token1
}

func (e *ExchangableUniswapV2Pair) GetRate(zfo bool) *big.Float {
	if zfo {
		return new(big.Float).Quo(new(big.Float).SetInt(e.Pair.Amount1), new(big.Float).SetInt(e.Pair.Amount0))
	}
	return new(big.Float).Quo(new(big.Float).SetInt(e.Pair.Amount0), new(big.Float).SetInt(e.Pair.Amount1))
}

func (e *ExchangableUniswapV2Pair) Address() string {
	return e.Pair.Address
}

func (e *ExchangableUniswapV2Pair) GetIdentifier() string {
	return e.Pair.GetIdentificator().String()
}
