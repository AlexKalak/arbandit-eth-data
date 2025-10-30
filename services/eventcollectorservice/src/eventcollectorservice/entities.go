package eventcollectorservice

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type pancakeswapV3SwapEvent struct {
	Sender             common.Address `json:"sender"`
	Recipient          common.Address `json:"repcipient"`
	Amount0            *big.Int       `json:"amount0"`
	Amount1            *big.Int       `json:"amount1"`
	SqrtPriceX96       *big.Int       `json:"sqrt_price_x96"`
	Liquidity          *big.Int       `json:"liquidity"`
	Tick               *big.Int       `json:"tick"`
	ProtocolFeesToken0 *big.Int       `json:""`
	ProtocolFeesToken1 *big.Int       `json:""`
	BlockNumber        *big.Int       `json:"block_number"`
}

type uniswapV3SwapEvent struct {
	Sender       common.Address `json:"sender"`
	Recipient    common.Address `json:"recipient"`
	Amount0      *big.Int       `json:"amount0"`
	Amount1      *big.Int       `json:"amount1"`
	SqrtPriceX96 *big.Int       `json:"sqrt_price_x96"`
	Liquidity    *big.Int       `json:"liquidity"`
	Tick         *big.Int       `json:"tick"`
}

type uniswapV3MintEvent struct {
	Sender    common.Address `json:"sender"`
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type uniswapV3BurnEvent struct {
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}
