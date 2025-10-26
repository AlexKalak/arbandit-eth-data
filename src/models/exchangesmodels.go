package models

import (
	"encoding/json"
	"fmt"
	"math/big"
)

const UNISWAP_V3_POOL_TABLE = "uniswap_v3_pools"

const UNISWAP_V3_POOL_ADDRESS = "address"
const UNISWAP_V3_POOL_EXCHANGE_NAME = "exchange_name"
const UNISWAP_V3_POOL_CHAINID = "chain_id"
const UNISWAP_V3_POOL_TOKEN0 = "token0"
const UNISWAP_V3_POOL_TOKEN1 = "token1"
const UNISWAP_V3_POOL_SQRTPRICEX96 = "sqrt_price_x96"
const UNISWAP_V3_POOL_LIQUIDITY = "liquidity"

const UNISWAP_V3_POOL_TICK = "tick"
const UNISWAP_V3_POOL_TICK_SPACING = "tick_spacing"
const UNISWAP_V3_POOL_TICK_LOWER = "tick_lower"
const UNISWAP_V3_POOL_TICK_UPPER = "tick_upper"
const UNISWAP_V3_POOL_NEAR_TICKS = "near_ticks"

const UNISWAP_V3_POOL_FEE_TIER = "fee_tier"
const UNISWAP_V3_POOL_IS_DUSTY = "is_dusty"
const UNISWAP_V3_POOL_BLOCK_NUMBER = "block_number"

const UNISWAP_V3_POOL_TOKEN0_HOLDING = "token0_holding"
const UNISWAP_V3_POOL_TOKEN1_HOLDING = "token1_holding"

type UniswapV3Pool struct {
	Address       string
	ExchangeName  string
	ChainID       uint
	Token0        string
	Token1        string
	SqrtPriceX96  *big.Int
	Liquidity     *big.Int
	Tick          int
	TickSpacing   int
	TickLower     int
	TickUpper     int
	NearTicksJSON string
	FeeTier       int
	IsDusty       bool
	BlockNumber   int

	Token0Holding *big.Int
	Token1Holding *big.Int
}

func (u *UniswapV3Pool) NearTicks() []int {
	nearTicks := []int{}
	err := json.Unmarshal([]byte(u.NearTicksJSON), &nearTicks)
	if err != nil {
		return []int{}
	}

	return nearTicks
}

func (p *UniswapV3Pool) GetIdentificator() V3PoolIdentificator {
	return V3PoolIdentificator{
		Address: p.Address,
		ChainID: p.ChainID,
	}
}

type V3PoolIdentificator struct {
	Address string
	ChainID uint
}

func (p V3PoolIdentificator) String() string {
	return fmt.Sprintf("%d.%s", p.ChainID, p.Address)
}

const UNISWAP_V2_PAIR_TABLE = "uniswap_v2_pairs"

const UNISWAP_V2_PAIR_ADDRESS = "address"
const UNISWAP_V2_PAIR_EXCHANGE_NAME = "exchange_name"
const UNISWAP_V2_PAIR_CHAINID = "chain_id"
const UNISWAP_V2_PAIR_TOKEN0 = "token0"
const UNISWAP_V2_PAIR_TOKEN1 = "token1"
const UNISWAP_V2_PAIR_AMOUNT0 = "amount0"
const UNISWAP_V2_PAIR_AMOUNT1 = "amount1"
const UNISWAP_V2_PAIR_BLOCK_NUMBER = "block_number"

const UNISWAP_V2_PAIR_FEE_TIER = "fee_tier"
const UNISWAP_V2_PAIR_IS_DUSTY = "is_dusty"

type UniswapV2Pair struct {
	Address      string
	ExchangeName string
	ChainID      uint
	Token0       string
	Token1       string
	Amount0      *big.Int
	Amount1      *big.Int
	FeeTier      int //format: 10000 = 1%
	IsDusty      bool
	BlockNumber  int
}

type V2PairIdentificator struct {
	Address string
	ChainID uint
}

func (p *UniswapV2Pair) GetIdentificator() V2PairIdentificator {
	return V2PairIdentificator{
		Address: p.Address,
		ChainID: p.ChainID,
	}
}
func (p V2PairIdentificator) String() string {
	return fmt.Sprintf("%d.%s", p.ChainID, p.Address)
}
