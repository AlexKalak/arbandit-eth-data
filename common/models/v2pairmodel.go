package models

import (
	"encoding/json"
	"fmt"
	"math/big"
)

const UNISWAP_V2_PAIR_TABLE = "uniswap_v2_pairs"

const UNISWAP_V2_PAIR_ADDRESS = "address"
const UNISWAP_V2_PAIR_EXCHANGE_NAME = "exchange_name"
const UNISWAP_V2_PAIR_CHAINID = "chain_id"
const UNISWAP_V2_PAIR_TOKEN0_ADDRESS = "token0_address"
const UNISWAP_V2_PAIR_TOKEN1_ADDRESS = "token1_address"
const UNISWAP_V2_PAIR_AMOUNT0 = "amount0"
const UNISWAP_V2_PAIR_AMOUNT1 = "amount1"
const UNISWAP_V2_PAIR_BLOCK_NUMBER = "block_number"

const UNISWAP_V2_PAIR_FEE_TIER = "fee_tier"
const UNISWAP_V2_PAIR_IS_DUSTY = "is_dusty"

const UNISWAP_V2_ZFO_10USD_RATE = "zfo_10usd_rate"
const UNISWAP_V2_NON_ZFO_10USD_RATE = "non_zfo_10usd_rate"

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

	Zfo10USDRate    *big.Float
	NonZfo10USDRate *big.Float
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

func (p *UniswapV2Pair) GetLiquidity() *big.Int {
	return new(big.Int).Mul(p.Amount0, p.Amount1)

}

func (p *UniswapV2Pair) GetJSON() ([]byte, error) {
	return json.Marshal(p)
}

func (p *UniswapV2Pair) FillFromJSON(jsonBytes []byte) error {
	err := json.Unmarshal(jsonBytes, &p)
	return err
}

func (p V2PairIdentificator) String() string {
	return fmt.Sprintf("%d.%s", p.ChainID, p.Address)
}
