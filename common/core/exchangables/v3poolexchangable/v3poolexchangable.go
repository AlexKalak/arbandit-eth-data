package v3poolexchangable

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ALTree/bigfloat"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/exchangableerrors"
	"github.com/alexkalak/go_market_analyze/common/models"
)

var Q96 = new(big.Int).Lsh(big.NewInt(1), 96)
var Q96Float = new(big.Float).SetInt(Q96)

// func tickToSqrtPriceX96(tick int) *big.Int {
// 	sqrtP := big.Int math.Pow(1.0001, float64(tick)/2)
// 	r := new(*big.Float).Mul(big.NewFloat(sqrtP), new(big.Float).SetInt(Q96))
// }

type ExchangableUniswapV3Pool struct {
	rawExchangable ExchangableUniswapV3PoolRaw
	Token0         *models.Token
	Token1         *models.Token
}

func NewV3ExchangablePool(pool *models.UniswapV3Pool, token0 *models.Token, token1 *models.Token) (ExchangableUniswapV3Pool, error) {
	if pool == nil || token1 == nil || token0 == nil {
		return ExchangableUniswapV3Pool{}, exchangableerrors.ErrInvalidArgsOnExchangablePool
	}

	rawExchangable := ExchangableUniswapV3PoolRaw{
		Pool: pool,
	}

	return ExchangableUniswapV3Pool{
		rawExchangable: rawExchangable,
		Token0:         token0,
		Token1:         token1,
	}, nil
}

func (e *ExchangableUniswapV3Pool) ImitateSwap(amountIn *big.Int, zfo bool) (*big.Int, error) {
	return e.rawExchangable.ImitateSwap(amountIn, zfo)
}
func (e *ExchangableUniswapV3Pool) ImitateSwapWithLog(amountIn *big.Int, zfo bool) (*big.Int, error) {
	return e.rawExchangable.ImitateSwapWithLog(amountIn, zfo)
}
func (e *ExchangableUniswapV3Pool) GetRate(zfo bool) *big.Float {
	return e.rawExchangable.GetRate(zfo)

}
func (e *ExchangableUniswapV3Pool) GetToken0() *models.Token {
	return e.Token0

}
func (e *ExchangableUniswapV3Pool) GetToken1() *models.Token {
	return e.Token1
}
func (e *ExchangableUniswapV3Pool) Address() string {
	return e.rawExchangable.Address()
}
func (e *ExchangableUniswapV3Pool) GetIdentifier() string {
	return e.rawExchangable.GetIdentifier()
}

// Raw exchangable
type ExchangableUniswapV3PoolRaw struct {
	Pool *models.UniswapV3Pool
}

func tickToSqrtPriceX96(tick int) *big.Int {
	if tick < -887272 || tick > 887272 {
		panic("tick out of range")
	}

	absTick := tick
	if tick < 0 {
		absTick = -tick
	}

	ratio := new(big.Int)
	if (absTick & 0x1) != 0 {
		ratio.SetString("0xfffcb933bd6fad37aa2d162d1a594001", 0)
	} else {
		ratio.SetString("0x100000000000000000000000000000000", 0)
	}

	mul := func(hex string) {
		x := new(big.Int)
		x.SetString(hex, 0)
		ratio.Mul(ratio, x)
		ratio.Rsh(ratio, 128)
	}

	if (absTick & 0x2) != 0 {
		mul("0xfff97272373d413259a46990580e213a")
	}
	if (absTick & 0x4) != 0 {
		mul("0xfff2e50f5f656932ef12357cf3c7fdcc")
	}
	if (absTick & 0x8) != 0 {
		mul("0xffe5caca7e10e4e61c3624eaa0941cd0")
	}
	if (absTick & 0x10) != 0 {
		mul("0xffcb9843d60f6159c9db58835c926644")
	}
	if (absTick & 0x20) != 0 {
		mul("0xff973b41fa98c081472e6896dfb254c0")
	}
	if (absTick & 0x40) != 0 {
		mul("0xff2ea16466c96a3843ec78b326b52861")
	}
	if (absTick & 0x80) != 0 {
		mul("0xfe5dee046a99a2a811c461f1969c3053")
	}
	if (absTick & 0x100) != 0 {
		mul("0xfcbe86c7900a88aedcffc83b479aa3a4")
	}
	if (absTick & 0x200) != 0 {
		mul("0xf987a7253ac413176f2b074cf7815e54")
	}
	if (absTick & 0x400) != 0 {
		mul("0xf3392b0822b70005940c7a398e4b70f3")
	}
	if (absTick & 0x800) != 0 {
		mul("0xe7159475a2c29b7443b29c7fa6e889d9")
	}
	if (absTick & 0x1000) != 0 {
		mul("0xd097f3bdfd2022b8845ad8f792aa5825")
	}
	if (absTick & 0x2000) != 0 {
		mul("0xa9f746462d870fdf8a65dc1f90e061e5")
	}
	if (absTick & 0x4000) != 0 {
		mul("0x70d869a156d2a1b890bb3df62baf32f7")
	}
	if (absTick & 0x8000) != 0 {
		mul("0x31be135f97d08fd981231505542fcfa6")
	}
	if (absTick & 0x10000) != 0 {
		mul("0x9aa508b5b7a84e1c677de54f3e99bc9")
	}
	if (absTick & 0x20000) != 0 {
		mul("0x5d6af8dedb81196699c329225ee604")
	}
	if (absTick & 0x40000) != 0 {
		mul("0x2216e584f5fa1ea926041bedfe98")
	}
	if (absTick & 0x80000) != 0 {
		mul("0x48a170391f7dc42444e8fa2")
	}

	if tick > 0 {
		ratio = new(big.Int).Div(new(big.Int).Lsh(big.NewInt(1), 256), ratio)
	}

	// Downscale to Q96 (right shift 32 bits, rounding up)
	ratio.Rsh(ratio, 32)
	return ratio
}

func sqrtPriceX96ToFloat(sqrtPX96 *big.Int) float64 {
	f := new(big.Float).Quo(new(big.Float).SetInt(sqrtPX96), new(big.Float).SetInt(Q96))
	v, _ := f.Float64()
	return v * v
}

func amount0Delta(sqrtP, sqrtPNext, L *big.Int) *big.Int {
	diff := new(big.Int).Sub(sqrtPNext, sqrtP)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}

	num := new(big.Int)
	num.Mul(diff, Q96)
	num.Mul(num, L)
	den := new(big.Int).Mul(sqrtPNext, sqrtP)
	return new(big.Int).Div(num, den)
}

func amount1Delta(sqrtP, sqrtPNext, L *big.Int) *big.Int {
	diff := new(big.Int).Sub(sqrtPNext, sqrtP)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}

	num := new(big.Int)
	num.Mul(diff, L)
	return new(big.Int).Div(num, Q96)
}

var feeDenominator = 100 * 10_000
var feeDenominatorB = big.NewInt(int64(feeDenominator))

func (e *ExchangableUniswapV3PoolRaw) ImitateSwap(amountIn *big.Int, zfo bool) (*big.Int, error) {

	amountInAfterFee := new(big.Int).Mul(amountIn, big.NewInt(int64(feeDenominator)-int64(e.Pool.FeeTier)))
	amountInAfterFee.Div(amountInAfterFee, feeDenominatorB)

	// amountInAfterFee := amountIn

	if e.Pool.Liquidity.Cmp(big.NewInt(0)) <= 0 {
		return nil, errors.New("zero liquidity in pool")
	}

	if e.Pool == nil {
		return nil, exchangableerrors.ErrInvalidPool
	}

	if e.Pool.TickLower == 0 || e.Pool.TickUpper == 0 {
		return nil, errors.New("no lower upper tick info")
	}

	remaining := new(big.Int).Set(amountInAfterFee)
	amountOutTotal := new(big.Int)

	currentTick := e.Pool.Tick
	currentSqrtPX96 := e.Pool.SqrtPriceX96

	for remaining.Sign() > 0 {
		nextTick := 0
		if zfo {
			nextTick = currentTick - e.Pool.TickSpacing

		} else {
			nextTick = currentTick + e.Pool.TickSpacing
		}

		if nextTick < e.Pool.TickLower || nextTick > e.Pool.TickUpper {
			return nil, errors.New("tick out of space")

		}

		sqrtPTarget := tickToSqrtPriceX96(nextTick)

		// fmt.Printf("Tick=%d price=%.10f -> target=%d price=%.10f\n",
		// 	currentTick, sqrtPriceX96ToFloat(currentSqrtPX96), nextTick, sqrtPriceX96ToFloat(sqrtPTarget))

		if zfo {
			amtNeeded := amount0Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
			// fmt.Println("remaining: ", remaining.String())
			// fmt.Println("amtNeeded: ", amtNeeded.String())

			if remaining.Cmp(amtNeeded) >= 0 {
				amtOut := amount1Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
				remaining.Sub(remaining, amtNeeded)
				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPTarget
				currentTick = nextTick
			} else {
				invOld := new(big.Int).Div(new(big.Int).Mul(Q96, Q96), currentSqrtPX96)
				term := new(big.Int).Mul(remaining, Q96)
				term.Div(term, e.Pool.Liquidity)
				invNew := new(big.Int).Sub(invOld, term)
				sqrtPNew := new(big.Int).Div(new(big.Int).Mul(Q96, Q96), invNew)

				amtOut := amount1Delta(currentSqrtPX96, sqrtPNew, e.Pool.Liquidity)
				// fmt.Printf("partial move: used0=%s got1=%s\n", remaining, amtOut)

				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPNew
				remaining.SetInt64(0)
				break
			}
		} else {
			amtNeeded := amount1Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
			// fmt.Println("remaining: ", remaining.String())
			// fmt.Println("amtNeeded: ", amtNeeded.String())

			if remaining.Cmp(amtNeeded) >= 0 {
				amtOut := amount0Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
				remaining.Sub(remaining, amtNeeded)
				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPTarget
				currentTick = nextTick
			} else {
				add := new(big.Int).Mul(remaining, Q96)
				add.Div(add, e.Pool.Liquidity)
				sqrtPNew := new(big.Int).Add(currentSqrtPX96, add)

				amtOut := amount0Delta(currentSqrtPX96, sqrtPNew, e.Pool.Liquidity)
				// fmt.Printf("partial move: used0=%s got1=%s\n", remaining, amtOut)

				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPNew
				remaining.SetInt64(0)
				break
			}
		}

	}

	return amountOutTotal, nil
}

func (e *ExchangableUniswapV3PoolRaw) HowMuchWillPriceMoveForConstantLiquidity(amountIn *big.Int, zfo bool) {

}

func (e *ExchangableUniswapV3PoolRaw) ImitateSwapWithLog(amountIn *big.Int, zfo bool) (*big.Int, error) {

	amountInAfterFee := new(big.Int).Mul(amountIn, big.NewInt(int64(feeDenominator)-int64(e.Pool.FeeTier)))
	amountInAfterFee.Div(amountInAfterFee, feeDenominatorB)

	// amountInAfterFee := amountIn

	if e.Pool.Liquidity.Cmp(big.NewInt(0)) <= 0 {
		return nil, errors.New("zero liquidity in pool")
	}

	if e.Pool == nil {
		return nil, exchangableerrors.ErrInvalidPool
	}

	if e.Pool.TickLower == 0 || e.Pool.TickUpper == 0 {
		return nil, errors.New("no lower upper tick info")
	}

	remaining := new(big.Int).Set(amountInAfterFee)
	amountOutTotal := new(big.Int)

	currentTick := e.Pool.Tick
	currentSqrtPX96 := e.Pool.SqrtPriceX96

	for remaining.Sign() > 0 {

		nextTick := 0
		if zfo {
			nextTick = currentTick - 1

		} else {
			nextTick = currentTick + 1
		}

		if nextTick < e.Pool.TickLower || nextTick > e.Pool.TickUpper {
			return nil, errors.New("tick out of space")

		}

		sqrtPTarget := tickToSqrtPriceX96(nextTick)

		fmt.Printf("Tick=%d price=%.10f -> target=%d price=%.10f\n",
			currentTick, sqrtPriceX96ToFloat(currentSqrtPX96), nextTick, sqrtPriceX96ToFloat(sqrtPTarget))

		if zfo {
			amtNeeded := amount0Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
			fmt.Println("remaining: ", remaining.String())
			fmt.Println("amtNeeded: ", amtNeeded.String())

			if remaining.Cmp(amtNeeded) >= 0 {
				amtOut := amount1Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
				remaining.Sub(remaining, amtNeeded)
				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPTarget
				currentTick = nextTick
			} else {
				invOld := new(big.Int).Div(new(big.Int).Mul(Q96, Q96), currentSqrtPX96)
				term := new(big.Int).Mul(remaining, Q96)
				term.Div(term, e.Pool.Liquidity)
				invNew := new(big.Int).Sub(invOld, term)
				sqrtPNew := new(big.Int).Div(new(big.Int).Mul(Q96, Q96), invNew)

				amtOut := amount1Delta(currentSqrtPX96, sqrtPNew, e.Pool.Liquidity)
				fmt.Printf("partial move: used0=%s got1=%s\n", remaining, amtOut)

				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPNew
				remaining.SetInt64(0)
				break
			}
		} else {
			amtNeeded := amount1Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
			fmt.Println("remaining: ", remaining.String())
			fmt.Println("amtNeeded: ", amtNeeded.String())

			if remaining.Cmp(amtNeeded) >= 0 {
				amtOut := amount0Delta(currentSqrtPX96, sqrtPTarget, e.Pool.Liquidity)
				remaining.Sub(remaining, amtNeeded)
				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPTarget
				currentTick = nextTick
			} else {
				add := new(big.Int).Mul(remaining, Q96)
				add.Div(add, e.Pool.Liquidity)
				sqrtPNew := new(big.Int).Add(currentSqrtPX96, add)

				amtOut := amount0Delta(currentSqrtPX96, sqrtPNew, e.Pool.Liquidity)
				fmt.Printf("partial move: used0=%s got1=%s\n", remaining, amtOut)

				amountOutTotal.Add(amountOutTotal, amtOut)
				currentSqrtPX96 = sqrtPNew
				remaining.SetInt64(0)
				break
			}
		}

	}

	return amountOutTotal, nil
}

func (e *ExchangableUniswapV3PoolRaw) GetRate(zfo bool) *big.Float {
	price := bigfloat.Pow(divideSqrtPriceX96(e.Pool.SqrtPriceX96), big.NewFloat(2))
	if zfo {
		return price
	}

	res := price
	res.Quo(big.NewFloat(1), res)
	return res
}

func (e *ExchangableUniswapV3PoolRaw) Address() string {
	return e.Pool.Address
}

func (e *ExchangableUniswapV3PoolRaw) GetIdentifier() string {
	return e.Pool.GetIdentificator().String()
}
