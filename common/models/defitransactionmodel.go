package models

import "math/big"

const (
	V3_TRANSACTION_TABLE                    = "v3_pool_transactions"
	V3_TRANSACTION_ID                       = "id"
	V3_TRANSACTION_CHAIN_ID                 = "chain_id"
	V3_TRANSACTION_POOL_ADDRESS             = "pool_address"
	V3_TRANSACTION_TX_HASH                  = "tx_hash"
	V3_TRANSACTION_TX_TIMESTAMP             = "tx_timestamp"
	V3_TRANSACTION_BLOCK_NUMBER             = "block_number"
	V3_TRANSACTION_AMOUNT0                  = "amount0"
	V3_TRANSACTION_AMOUNT1                  = "amount1"
	V3_TRANSACTION_ARCHIVE_TOKEN0_USD_PRICE = "archive_token0_usd_price"
	V3_TRANSACTION_ARCHIVE_TOKEN1_USD_PRICE = "archive_token1_usd_price"
)

type V3Transaction struct {
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
