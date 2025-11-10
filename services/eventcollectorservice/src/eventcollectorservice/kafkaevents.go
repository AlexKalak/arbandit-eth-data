package eventcollectorservice

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	BLOCK_OVER       = "BlockOver"
	SWAP_KAFKA_EVENT = "Swap"
	MINT_KAFKA_EVENT = "Mint"
	BURN_KAFKA_EVENT = "Burn"
)

type poolEvent struct {
	Type        string      `json:"type"`
	Data        interface{} `json:"data"`
	BlockNumber uint64      `json:"block_number"`
	Address     string      `json:"address"`
	TxHash      string      `json:"tx_hash"`
	TxTimestamp uint64      `json:"tx_timestamp"`
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
		BatchTimeout: 1 * time.Millisecond,
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
	err = c.updateV3PricesWriter.WriteMessages(context.Background(), kafka.Message{
		Value: eventJSON,
	})

	return err
}
