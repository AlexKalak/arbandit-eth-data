package eventcollectorservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorservice/eventhandles"
	"github.com/segmentio/kafka-go"
)

const (
	BLOCK_OVER   = "BlockOver"
	SWAPV3_EVENT = "SwapV3"
	MINTV3_EVENT = "MintV3"
	BURNV3_EVENT = "BurnV3"

	SWAPV2_EVENT = "SwapV2"
	SYNCV2_EVENT = "SyncV2"
)

func PoolEventType2Kafka(ev string) (string, error) {
	switch ev {
	case eventhandles.SWAPV3_EVENT:
		return SWAPV3_EVENT, nil
	case eventhandles.MINTV3_EVENT:
		return MINTV3_EVENT, nil
	case eventhandles.BURNV3_EVENT:
		return BURNV3_EVENT, nil

	case eventhandles.SWAPV2_EVENT:
		return SWAPV2_EVENT, nil
	case eventhandles.SYNCV2_EVENT:
		return SYNCV2_EVENT, nil
	default:
		return "", errors.New("unable to convert v3poolevent to kafka event")
	}
}

type poolEvent struct {
	Type        string      `json:"type"`
	BlockNumber uint64      `json:"block_number"`
	Address     string      `json:"address"`
	TxHash      string      `json:"tx_hash"`
	TxTimestamp uint64      `json:"tx_timestamp"`
	Data        interface{} `json:"data"`
}

func (p *poolEvent) FromV3PoolEvent(ev eventhandles.V3PoolEvent) error {
	metaData := ev.GetMetaData()

	poolEventType, err := PoolEventType2Kafka(metaData.Type)
	if err != nil {
		return err
	}

	p.Type = poolEventType
	p.BlockNumber = metaData.BlockNumber
	p.Address = metaData.Address
	p.TxHash = metaData.TxHash
	p.TxTimestamp = uint64(metaData.TxTimestamp)
	p.Data = ev.GetData()

	return nil
}

func (p *poolEvent) FromV2PoolEvent(ev eventhandles.V2PoolEvent) error {
	metaData := ev.GetMetaData()

	poolEventType, err := PoolEventType2Kafka(metaData.Type)
	if err != nil {
		return err
	}

	p.Type = poolEventType
	p.BlockNumber = metaData.BlockNumber
	p.Address = metaData.Address
	p.TxHash = metaData.TxHash
	p.TxTimestamp = uint64(metaData.TxTimestamp)
	p.Data = ev.GetData()

	return nil
}

type kafkaClientConfig struct {
	ChainID     uint
	KafkaTopic  string
	KafkaServer string
}

type kafkaClient struct {
	currentBlockNumber   *big.Int
	updateV3PricesWriter *kafka.Writer
}

func newKafkaClient(config kafkaClientConfig) (kafkaClient, error) {
	writer := kafka.Writer{
		Addr:  kafka.TCP(config.KafkaServer),
		Topic: config.KafkaTopic,
		// Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1,
		Async:        false,
	}

	client := kafkaClient{
		updateV3PricesWriter: &writer,
	}
	return client, nil
}

func (c *kafkaClient) sendUpdateV3PricesEvents(events []poolEvent) error {
	messages := make([]kafka.Message, len(events))
	for i, event := range events {
		fmt.Println(event.Address, event.BlockNumber)
		eventJSON, err := json.Marshal(&event)
		if err != nil {
			return err
		}
		messages[i] = kafka.Message{
			Value: eventJSON,
		}
	}

	err := c.updateV3PricesWriter.WriteMessages(context.Background(), messages...)

	return err
}

func (c *kafkaClient) sendUpdateV3PricesEvent(event poolEvent) error {
	eventJSON, err := json.Marshal(&event)
	if err != nil {
		return err
	}

	fmt.Println(event.Address, event.BlockNumber)
	err = c.updateV3PricesWriter.WriteMessages(context.Background(), kafka.Message{
		Value: eventJSON,
	})

	return err
}
