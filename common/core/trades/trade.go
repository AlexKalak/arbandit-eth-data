package trades

import (
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/models"
)

type Trade interface {
	GetUniqueID() string
	GetHash() string
	GetExchangableAddress() string
	GetAmount0() *big.Int
	GetAmount1() *big.Int
	GetArchiveToken0USDPrice() *big.Float
	GetArchiveToken1USDPrice() *big.Float
	GetTimestamp() uint64
}

func NewTradeFromV3Swap(swap *models.V3Swap) Trade {
	return &V3SwapTradeWrap{
		uniqueID: fmt.Sprint(swap.ID, swap.TxHash),
		swap:     swap,
	}

}

type V3SwapTradeWrap struct {
	uniqueID string
	swap     *models.V3Swap
}

func (w *V3SwapTradeWrap) GetUniqueID() string {
	return w.uniqueID
}

func (w *V3SwapTradeWrap) GetHash() string {
	return w.swap.TxHash
}

func (w *V3SwapTradeWrap) GetExchangableAddress() string {
	return w.swap.PoolAddress
}
func (w *V3SwapTradeWrap) GetAmount0() *big.Int {
	return w.swap.Amount0
}
func (w *V3SwapTradeWrap) GetAmount1() *big.Int {
	return w.swap.Amount1
}

func (w *V3SwapTradeWrap) GetArchiveToken0USDPrice() *big.Float {
	return w.swap.ArchiveToken0USDPrice

}
func (w *V3SwapTradeWrap) GetArchiveToken1USDPrice() *big.Float {
	return w.swap.ArchiveToken1USDPrice

}
func (w *V3SwapTradeWrap) GetTimestamp() uint64 {
	return w.swap.TxTimestamp
}

func NewTradeFromV2Swap(swap *models.V2Swap) Trade {
	return &V2SwapTradeWrap{
		uniqueID: fmt.Sprint(swap.ID, swap.TxHash),
		swap:     swap,
	}

}

type V2SwapTradeWrap struct {
	uniqueID string
	swap     *models.V2Swap
}

func (w *V2SwapTradeWrap) GetUniqueID() string {
	return w.uniqueID
}

func (w *V2SwapTradeWrap) GetHash() string {
	return w.swap.TxHash
}

func (w *V2SwapTradeWrap) GetExchangableAddress() string {
	return w.swap.PairAddress
}
func (w *V2SwapTradeWrap) GetAmount0() *big.Int {
	return w.swap.Amount0
}
func (w *V2SwapTradeWrap) GetAmount1() *big.Int {
	return w.swap.Amount1
}

func (w *V2SwapTradeWrap) GetArchiveToken0USDPrice() *big.Float {
	return w.swap.ArchiveToken0USDPrice
}

func (w *V2SwapTradeWrap) GetArchiveToken1USDPrice() *big.Float {
	return w.swap.ArchiveToken1USDPrice
}

func (w *V2SwapTradeWrap) GetTimestamp() uint64 {
	return w.swap.TxTimestamp
}
