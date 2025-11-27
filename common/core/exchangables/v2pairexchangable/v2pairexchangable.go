package v2pairexchangable

import (
	"errors"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/exchangableerrors"
	"github.com/alexkalak/go_market_analyze/common/models"
)

type ExchangableUniswapV2Pair struct {
	exchangableRaw ExchangableUniswapV2PairRaw
	Token0         *models.Token
	Token1         *models.Token
}

func NewV2ExchangablePair(pair *models.UniswapV2Pair, token0 *models.Token, token1 *models.Token) (ExchangableUniswapV2Pair, error) {
	if pair == nil || token1 == nil || token0 == nil {
		return ExchangableUniswapV2Pair{}, exchangableerrors.ErrInvalidArgsOnExchangablePair
	}

	return ExchangableUniswapV2Pair{
		exchangableRaw: ExchangableUniswapV2PairRaw{
			Pair: pair,
		},
		Token0: token0,
		Token1: token1,
	}, nil
}

func UpdateRateFor10USD(pair *models.UniswapV2Pair, token0 *models.Token, token1 *models.Token) error {
	if token0.USDPrice.Cmp(big.NewFloat(0)) == 0 ||
		token1.USDPrice.Cmp(big.NewFloat(0)) == 0 {
		return errors.New("invalid usd price")
	}

	exchangablePair, err := NewV2ExchangablePair(pair, token0, token1)
	if err != nil {
		return errors.New("invalid usd price")
	}

	usdAmount := big.NewInt(5)

	amount0Init := token0.FromUSD(usdAmount)
	amount1Init := token1.FromUSD(usdAmount)

	amount1Out, err := exchangablePair.ImitateSwap(amount0Init, true)
	if err != nil {
		return err
	}
	reverted0, err := exchangablePair.ImitateSwap(amount1Out, false)
	if err != nil {
		return err
	}

	amount0Out, err := exchangablePair.ImitateSwap(amount1Init, false)
	if err != nil {
		return err
	}
	reverted1, err := exchangablePair.ImitateSwap(amount0Out, true)
	if err != nil {
		return err
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

	pair.Zfo10USDRate = new(big.Float).Quo(token1.GetRealAmount(amount1Out), token0.GetRealAmount(amount0Init))
	pair.NonZfo10USDRate = new(big.Float).Quo(token0.GetRealAmount(amount0Out), token1.GetRealAmount(amount1Init))

	return nil
}

func (e *ExchangableUniswapV2Pair) ImitateSwap(amountIn *big.Int, zfo bool) (*big.Int, error) {
	return e.exchangableRaw.ImitateSwap(amountIn, zfo)
}

func (e *ExchangableUniswapV2Pair) GetToken0() *models.Token {
	return e.Token0
}

func (e *ExchangableUniswapV2Pair) GetToken1() *models.Token {
	return e.Token1
}

func (e *ExchangableUniswapV2Pair) GetRate(zfo bool) *big.Float {
	return e.exchangableRaw.GetRate(zfo)
}

func (e *ExchangableUniswapV2Pair) Address() string {
	return e.exchangableRaw.Pair.Address
}

func (e *ExchangableUniswapV2Pair) GetIdentifier() string {
	return e.exchangableRaw.GetIdentifier()
}

type ExchangableUniswapV2PairRaw struct {
	Pair *models.UniswapV2Pair
}

func (e *ExchangableUniswapV2PairRaw) ImitateSwap(amountIn *big.Int, zfo bool) (*big.Int, error) {
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

func (e *ExchangableUniswapV2PairRaw) GetRate(zfo bool) *big.Float {
	if zfo {
		return new(big.Float).Quo(new(big.Float).SetInt(e.Pair.Amount1), new(big.Float).SetInt(e.Pair.Amount0))
	}
	return new(big.Float).Quo(new(big.Float).SetInt(e.Pair.Amount0), new(big.Float).SetInt(e.Pair.Amount1))
}

func (e *ExchangableUniswapV2PairRaw) Address() string {
	return e.Pair.Address
}

func (e *ExchangableUniswapV2PairRaw) GetIdentifier() string {
	return e.Pair.GetIdentificator().String()
}
