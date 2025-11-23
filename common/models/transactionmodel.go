package models

import "math/big"

const (
	V3_SWAP_TABLE                    = "v3_pool_swaps"
	V3_SWAP_ID                       = "id"
	V3_SWAP_CHAIN_ID                 = "chain_id"
	V3_SWAP_POOL_ADDRESS             = "pool_address"
	V3_SWAP_TX_HASH                  = "tx_hash"
	V3_SWAP_TX_TIMESTAMP             = "tx_timestamp"
	V3_SWAP_BLOCK_NUMBER             = "block_number"
	V3_SWAP_AMOUNT0                  = "amount0"
	V3_SWAP_AMOUNT1                  = "amount1"
	V3_SWAP_ARCHIVE_TOKEN0_USD_PRICE = "archive_token0_usd_price"
	V3_SWAP_ARCHIVE_TOKEN1_USD_PRICE = "archive_token1_usd_price"
)

type V3Swap struct {
	ID                    int        `json:"id"`
	PoolAddress           string     `json:"pool_address"`
	ChainID               uint       `json:"chain_id"`
	TxHash                string     `json:"tx_hash"`
	BlockNumber           uint64     `json:"block_number"`
	Amount0               *big.Int   `json:"amount0"`
	Amount1               *big.Int   `json:"amount1"`
	ArchiveToken0USDPrice *big.Float `json:"archive_token0_usd_price"`
	ArchiveToken1USDPrice *big.Float `json:"archive_token1_usd_price"`
	TxTimestamp           uint64     `json:"tx_timestamp"`
}

const (
	V2_SWAP_TABLE                    = "v2_pair_swaps"
	V2_SWAP_ID                       = "id"
	V2_SWAP_CHAIN_ID                 = "chain_id"
	V2_SWAP_PAIR_ADDRESS             = "pair_address"
	V2_SWAP_TX_HASH                  = "tx_hash"
	V2_SWAP_TX_TIMESTAMP             = "tx_timestamp"
	V2_SWAP_BLOCK_NUMBER             = "block_number"
	V2_SWAP_AMOUNT0                  = "amount0"
	V2_SWAP_AMOUNT1                  = "amount1"
	V2_SWAP_ARCHIVE_TOKEN0_USD_PRICE = "archive_token0_usd_price"
	V2_SWAP_ARCHIVE_TOKEN1_USD_PRICE = "archive_token1_usd_price"
)

type V2Swap struct {
	ID                    int        `json:"id"`
	PairAddress           string     `json:"pair_address"`
	ChainID               uint       `json:"chain_id"`
	TxHash                string     `json:"tx_hash"`
	BlockNumber           uint64     `json:"block_number"`
	Amount0               *big.Int   `json:"amount0"`
	Amount1               *big.Int   `json:"amount1"`
	ArchiveToken0USDPrice *big.Float `json:"archive_token0_usd_price"`
	ArchiveToken1USDPrice *big.Float `json:"archive_token1_usd_price"`
	TxTimestamp           uint64     `json:"tx_timestamp"`
}
