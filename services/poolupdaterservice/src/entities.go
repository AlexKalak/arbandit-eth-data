package poolupdaterservice

type eventMetaData struct {
	Type        string `json:"type"`
	BlockNumber uint64 `json:"block_number"`
	Address     string `json:"address"`
	TxHash      string `json:"tx_hash"`
	TxTimestamp uint64 `json:"tx_timestamp"`
}
