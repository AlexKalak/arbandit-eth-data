package eventhandles

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	abiassets "github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorassets"
	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorerrors"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type V2ExchangeABI struct {
	ABI       abi.ABI
	SyncV2Sig common.Hash
	SwapV2Sig common.Hash
}

func NewV2EventHandler() (V2EventHandler, error) {
	uniswapV2EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABIUniswapV2String))
	if err != nil {
		return V2EventHandler{}, err
	}

	uniswapABI := V2ExchangeABI{
		ABI:       uniswapV2EventsABI,
		SwapV2Sig: uniswapV2EventsABI.Events["Swap"].ID,
		SyncV2Sig: uniswapV2EventsABI.Events["Sync"].ID,
	}

	allEventSigs := []common.Hash{
		uniswapABI.SwapV2Sig,
		uniswapABI.SyncV2Sig,
	}

	return V2EventHandler{
		UniswapHandler: uniswapV2ExchangeEventHandler{
			ABI: uniswapABI,
		},
		AllEventSigs: allEventSigs,
	}, nil
}

type V2EventHandler struct {
	UniswapHandler uniswapV2ExchangeEventHandler
	AllEventSigs   []common.Hash
}

func (h *V2EventHandler) Handle(lg types.Log, timestamp int) (V3PoolEvent, error) {
	switch lg.Topics[0] {
	case
		h.UniswapHandler.ABI.SyncV2Sig,
		h.UniswapHandler.ABI.SwapV2Sig:
		return h.UniswapHandler.handle(timestamp, lg)
	default:
		return nil, errors.New("event signature not found")
	}
}
func (h *V2EventHandler) HasSig(sig common.Hash) bool {
	return slices.Contains(h.AllEventSigs, sig)
}

type uniswapV2ExchangeEventHandler struct {
	ABI V2ExchangeABI
}

func (h *uniswapV2ExchangeEventHandler) handle(timestamp int, lg types.Log) (V3PoolEvent, error) {
	switch lg.Topics[0] {
	case h.ABI.SwapV2Sig:
		ev := UniswapV2SwapEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.SyncV2Sig:
		ev := UniswapV2SyncEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	default:
		return nil, errors.New("Uniswap event signature not found")
	}
}

type UniswapV2SwapEvent struct {
	MetaData   EventMetaData
	Sender     common.Address `json:"sender"`
	Amount0In  *big.Int       `json:"amount0_in"`
	Amount1In  *big.Int       `json:"amount1_in"`
	Amount0Out *big.Int       `json:"amount0_out"`
	Amount1Out *big.Int       `json:"amount1_out"`
	To         common.Address `json:"to"`
}

func (e *UniswapV2SwapEvent) ParseFrom(abiForEvent V2ExchangeABI, lg types.Log, timestamp int) error {
	metaData := metaDataFromLog(SWAPV2_EVENT, timestamp, lg)
	e.MetaData = metaData

	out, err := abiForEvent.ABI.Unpack("Swap", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking V2 Swap event", err)
		return err
	}

	amount0In, ok := out[0].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	amount1In, ok := out[1].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	amount0Out, ok := out[2].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	amount1Out, ok := out[3].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}

	sender := common.HexToAddress(lg.Topics[1].Hex())
	to := common.HexToAddress(lg.Topics[2].Hex())

	e.Sender = sender
	e.To = to
	e.Amount0In = amount0In
	e.Amount1In = amount1In
	e.Amount0Out = amount0Out
	e.Amount1Out = amount1Out

	return nil
}

func (e *UniswapV2SwapEvent) GetData() any {
	return e
}

func (e *UniswapV2SwapEvent) GetMetaData() EventMetaData {
	return e.MetaData
}

type UniswapV2SyncEvent struct {
	MetaData EventMetaData
	Reserve0 *big.Int `json:"reserve1"`
	Reserve1 *big.Int `json:"reserve0"`
}

func (e *UniswapV2SyncEvent) ParseFrom(abiForEvent V2ExchangeABI, lg types.Log, timestamp int) error {
	metaData := metaDataFromLog(SYNCV2_EVENT, timestamp, lg)
	e.MetaData = metaData

	out, err := abiForEvent.ABI.Unpack("Sync", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking V2 Sync event", err)
		return err
	}

	reserve0, ok := out[0].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	reserve1, ok := out[0].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}

	e.Reserve0 = reserve0
	e.Reserve1 = reserve1

	return nil
}

func (e *UniswapV2SyncEvent) GetData() any {
	return e
}

func (e *UniswapV2SyncEvent) GetMetaData() EventMetaData {
	return e.MetaData
}
