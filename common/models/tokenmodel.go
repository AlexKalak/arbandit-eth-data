package models

import "math/big"

const TOKENS_TABLE = "tokens"
const TOKEN_NAME = "name"
const TOKEN_SYMBOL = "symbol"
const TOKEN_ADDRESS = "address"
const TOKEN_CHAINID = "chain_id"
const TOKEN_LOGOURI = "logo_uri"
const TOKEN_DECIMALS = "decimals"
const TOKEN_DEFI_USD_PRICE = "defi_scaled_usd_price"

type TokenIdentificator struct {
	Address string
	ChainID uint
}

type Token struct {
	Name         string
	Symbol       string
	Address      string
	ChainID      uint
	LogoURI      string
	Decimals     int
	DefiUSDPrice *big.Float

	//Not in db
	HasLiquidity bool
}

func (t *Token) GetRealAmount(amount *big.Int) *big.Float {
	exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Decimals)), nil)
	return new(big.Float).Quo(new(big.Float).SetInt(amount), new(big.Float).SetInt(exp))

}

func (t *Token) GetIdentificator() TokenIdentificator {
	return TokenIdentificator{
		Address: t.Address,
		ChainID: t.ChainID,
	}
}
