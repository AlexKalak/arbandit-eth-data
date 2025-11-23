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

type V3ExchangeABI struct {
	ABI        abi.ABI
	SwapV3Sig  common.Hash
	MintV3Sig  common.Hash
	BurnV3Sig  common.Hash
	FlashV3Sig common.Hash
}

func NewV3EventHandler() (V3EventHandler, error) {
	uniswapV3EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABIUniswapV3String))
	if err != nil {
		return V3EventHandler{}, err
	}
	pancakeswapV3EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABIPancakeswapV3String))
	if err != nil {
		return V3EventHandler{}, err
	}
	sushiswapV3EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABISushiswapV3String))
	if err != nil {
		return V3EventHandler{}, err
	}

	uniswapABI := V3ExchangeABI{
		ABI:        uniswapV3EventsABI,
		SwapV3Sig:  uniswapV3EventsABI.Events["Swap"].ID,
		MintV3Sig:  uniswapV3EventsABI.Events["Mint"].ID,
		BurnV3Sig:  uniswapV3EventsABI.Events["Burn"].ID,
		FlashV3Sig: uniswapV3EventsABI.Events["Flash"].ID,
	}
	pancakeswapABI := V3ExchangeABI{
		ABI:       pancakeswapV3EventsABI,
		SwapV3Sig: pancakeswapV3EventsABI.Events["Swap"].ID,
		MintV3Sig: pancakeswapV3EventsABI.Events["Mint"].ID,
		BurnV3Sig: pancakeswapV3EventsABI.Events["Burn"].ID,
	}
	sushiswapABI := V3ExchangeABI{
		ABI:       sushiswapV3EventsABI,
		SwapV3Sig: sushiswapV3EventsABI.Events["Swap"].ID,
		MintV3Sig: sushiswapV3EventsABI.Events["Mint"].ID,
		BurnV3Sig: sushiswapV3EventsABI.Events["Burn"].ID,
	}

	allEventSigs := []common.Hash{
		uniswapABI.SwapV3Sig,
		uniswapABI.MintV3Sig,
		uniswapABI.BurnV3Sig,
		uniswapABI.FlashV3Sig,
		pancakeswapABI.SwapV3Sig,
		pancakeswapABI.MintV3Sig,
		pancakeswapABI.BurnV3Sig,
		sushiswapABI.SwapV3Sig,
		sushiswapABI.MintV3Sig,
		sushiswapABI.BurnV3Sig,
	}

	return V3EventHandler{
		UniswapHandler: uniswapV3ExchangeEventHandler{
			ABI: uniswapABI,
		},
		SushiswapHandler: sushiswapV3ExchangeEventHandler{
			ABI: sushiswapABI,
		},
		PancakeswapHandler: pancakeswapV3ExchangeEventHandler{
			ABI: pancakeswapABI,
		},
		AllEventSigs: allEventSigs,
	}, nil
}

type V3EventHandler struct {
	UniswapHandler     uniswapV3ExchangeEventHandler
	SushiswapHandler   sushiswapV3ExchangeEventHandler
	PancakeswapHandler pancakeswapV3ExchangeEventHandler
	AllEventSigs       []common.Hash
}

func (h *V3EventHandler) Handle(lg types.Log, timestamp int) (V3PoolEvent, error) {
	switch lg.Topics[0] {
	case
		h.UniswapHandler.ABI.SwapV3Sig,
		h.UniswapHandler.ABI.BurnV3Sig,
		h.UniswapHandler.ABI.MintV3Sig,
		h.UniswapHandler.ABI.FlashV3Sig:
		return h.UniswapHandler.handle(timestamp, lg)
	case
		h.PancakeswapHandler.ABI.SwapV3Sig,
		h.PancakeswapHandler.ABI.BurnV3Sig,
		h.PancakeswapHandler.ABI.MintV3Sig:
		return h.UniswapHandler.handle(timestamp, lg)
	case
		h.SushiswapHandler.ABI.SwapV3Sig,
		h.SushiswapHandler.ABI.BurnV3Sig,
		h.SushiswapHandler.ABI.MintV3Sig:
		return h.UniswapHandler.handle(timestamp, lg)
	default:
		return nil, errors.New("event signature not found")
	}
}

func (h *V3EventHandler) HasSig(sig common.Hash) bool {
	return slices.Contains(h.AllEventSigs, sig)
}

type uniswapV3ExchangeEventHandler struct {
	ABI V3ExchangeABI
}

func (h *uniswapV3ExchangeEventHandler) handle(timestamp int, lg types.Log) (V3PoolEvent, error) {
	switch lg.Topics[0] {
	case h.ABI.SwapV3Sig:
		ev := UniswapV3SwapEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.MintV3Sig:
		ev := UniswapV3MintEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.BurnV3Sig:
		ev := UniswapV3BurnEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	default:
		return nil, errors.New("Uniswap event signature not found")
	}
}

type sushiswapV3ExchangeEventHandler struct {
	ABI V3ExchangeABI
}

func (h *sushiswapV3ExchangeEventHandler) handle(timestamp int, lg types.Log) (V3PoolEvent, error) {
	switch lg.Topics[0] {
	case h.ABI.SwapV3Sig:
		ev := UniswapV3SwapEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.MintV3Sig:
		ev := UniswapV3MintEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.BurnV3Sig:
		ev := UniswapV3BurnEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	default:
		return nil, errors.New("Uniswap event signature not found")
	}
}

type pancakeswapV3ExchangeEventHandler struct {
	ABI V3ExchangeABI
}

func (h *pancakeswapV3ExchangeEventHandler) handle(timestamp int, lg types.Log) (V3PoolEvent, error) {
	switch lg.Topics[0] {
	case h.ABI.SwapV3Sig:
		ev := PancakeswapV3SwapEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.MintV3Sig:
		ev := UniswapV3MintEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	case h.ABI.BurnV3Sig:
		ev := UniswapV3BurnEvent{}
		err := ev.ParseFrom(h.ABI, lg, timestamp)
		if err != nil {
			return nil, err
		}
		return &ev, nil
	default:
		return nil, errors.New("Uniswap event signature not found")
	}
}

// Events
type UniswapV3SwapEvent struct {
	MetaData     EventMetaData
	Sender       common.Address `json:"sender"`
	Recipient    common.Address `json:"recipient"`
	Amount0      *big.Int       `json:"amount0"`
	Amount1      *big.Int       `json:"amount1"`
	SqrtPriceX96 *big.Int       `json:"sqrt_price_x96"`
	Liquidity    *big.Int       `json:"liquidity"`
	Tick         *big.Int       `json:"tick"`
}

func (e *UniswapV3SwapEvent) ParseFrom(abiForEvent V3ExchangeABI, lg types.Log, timestamp int) error {
	metaData := metaDataFromLog(SWAPV3_EVENT, timestamp, lg)
	e.MetaData = metaData

	out, err := abiForEvent.ABI.Unpack("Swap", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Swap event", err)
		return err
	}

	Amount0, ok := out[0].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[1].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	SqrtPriceX96, ok := out[2].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Liquidity, ok := out[3].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Tick, ok := out[4].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Sender := common.HexToAddress(lg.Topics[1].Hex())
	Recipient := common.HexToAddress(lg.Topics[2].Hex())

	e.Sender = Sender
	e.Recipient = Recipient
	e.Amount0 = Amount0
	e.Amount1 = Amount1
	e.SqrtPriceX96 = SqrtPriceX96
	e.Liquidity = Liquidity
	e.Tick = Tick

	return nil
}

func (e *UniswapV3SwapEvent) GetData() any {
	return e
}

func (e *UniswapV3SwapEvent) GetMetaData() EventMetaData {
	return e.MetaData
}

type UniswapV3MintEvent struct {
	MetaData  EventMetaData
	Sender    common.Address `json:"sender"`
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

func (e *UniswapV3MintEvent) ParseFrom(abiForEvent V3ExchangeABI, lg types.Log, timestamp int) error {
	metaData := metaDataFromLog(MINTV3_EVENT, timestamp, lg)
	e.MetaData = metaData

	out, err := abiForEvent.ABI.Unpack("Mint", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Mint event", err)
		return err
	}

	Sender, ok := out[0].(common.Address) // int256
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount, ok := out[1].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount0, ok := out[2].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[3].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}

	Owner := common.HexToAddress(lg.Topics[1].Hex())
	TickLower := parseTickFromTopic(lg.Topics[2])
	TickUpper := parseTickFromTopic(lg.Topics[3])

	e.Sender = Sender
	e.Owner = Owner
	e.TickLower = TickLower
	e.TickUpper = TickUpper
	e.Amount = Amount
	e.Amount0 = Amount0
	e.Amount1 = Amount1

	return nil
}

func (e *UniswapV3MintEvent) GetData() any {
	return e
}

func (e *UniswapV3MintEvent) GetMetaData() EventMetaData {
	return e.MetaData
}

type UniswapV3BurnEvent struct {
	MetaData  EventMetaData
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

func (e *UniswapV3BurnEvent) ParseFrom(abiForEvent V3ExchangeABI, lg types.Log, timestamp int) error {
	metaData := metaDataFromLog(BURNV3_EVENT, timestamp, lg)
	e.MetaData = metaData

	out, err := abiForEvent.ABI.Unpack("Burn", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Burn event", err)
		return err
	}

	Amount, ok := out[0].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount0 := out[1].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1 := out[2].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}

	Owner := common.HexToAddress(lg.Topics[1].Hex())
	TickLower := parseTickFromTopic(lg.Topics[2])
	TickUpper := parseTickFromTopic(lg.Topics[3])

	e.Owner = Owner
	e.TickLower = TickLower
	e.TickUpper = TickUpper
	e.Amount = Amount
	e.Amount0 = Amount0
	e.Amount1 = Amount1

	return nil
}

func (e *UniswapV3BurnEvent) GetData() any {
	return e
}

func (e *UniswapV3BurnEvent) GetMetaData() EventMetaData {
	return e.MetaData
}

type PancakeswapV3SwapEvent struct {
	MetaData           EventMetaData
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

func (e *PancakeswapV3SwapEvent) ParseFrom(abiForEvent V3ExchangeABI, lg types.Log, timestamp int) error {
	metaData := metaDataFromLog(SWAPV3_EVENT, timestamp, lg)
	e.MetaData = metaData

	out, err := abiForEvent.ABI.Unpack("Swap", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Swap event", err)
		return err
	}

	Amount0, ok := out[0].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[1].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	SqrtPriceX96, ok := out[2].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Liquidity, ok := out[3].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	Tick, ok := out[4].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	ProtocolFeesToken0, ok := out[4].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}
	ProtocolFeesToken1, ok := out[4].(*big.Int)
	if !ok {
		return eventcollectorerrors.ErrUnableToParseLog
	}

	Sender := common.HexToAddress(lg.Topics[1].Hex())
	Recipient := common.HexToAddress(lg.Topics[2].Hex())

	e.Sender = Sender
	e.Recipient = Recipient
	e.Amount0 = Amount0
	e.Amount1 = Amount1
	e.SqrtPriceX96 = SqrtPriceX96
	e.Liquidity = Liquidity
	e.Tick = Tick
	e.ProtocolFeesToken0 = ProtocolFeesToken0
	e.ProtocolFeesToken1 = ProtocolFeesToken1

	return nil
}

func (e *PancakeswapV3SwapEvent) GetData() any {
	return e
}

func (e *PancakeswapV3SwapEvent) GetMetaData() EventMetaData {
	return e.MetaData
}

func parseTickFromTopic(topic common.Hash) int32 {
	// Convert bytes to big.Int
	b := new(big.Int).SetBytes(topic.Bytes())

	// Mask only the lowest 24 bits
	value := b.Int64() & 0xFFFFFF // 24 bits

	// If sign bit (bit 23) is set, convert to negative
	if value&0x800000 != 0 {
		value = value - 0x1000000
	}

	return int32(value)
}
