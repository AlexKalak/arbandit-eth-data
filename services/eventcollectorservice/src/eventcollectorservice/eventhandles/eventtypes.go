package eventhandles

import (
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
)

const (
	SWAPV3_EVENT = "SwapV3"
	MINTV3_EVENT = "MintV3"
	BURNV3_EVENT = "BurnV3"

	SWAPV2_EVENT = "SwapV2"
	SYNCV2_EVENT = "SyncV2"
)

type EventMetaData struct {
	Type        string
	BlockNumber uint64
	Address     string
	TxHash      string
	TxTimestamp int
}

type V3PoolEvent interface {
	GetMetaData() EventMetaData
	GetData() any
}

type V2PoolEvent interface {
	GetMetaData() EventMetaData
	GetData() any
}

func metaDataFromLog(eventType string, timestamp int, lg types.Log) EventMetaData {
	return EventMetaData{
		Type:        eventType,
		BlockNumber: lg.BlockNumber,
		Address:     strings.ToLower(lg.Address.Hex()),
		TxHash:      lg.TxHash.Hex(),
		TxTimestamp: timestamp,
	}
}
