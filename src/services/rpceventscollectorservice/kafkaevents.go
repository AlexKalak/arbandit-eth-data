package rpceventscollectorservice

import (
	"context"
	"encoding/json"
	"time"

	"github.com/alexkalak/go_market_analyze/src/errors/rpceventcollectorerrors"
	"github.com/alexkalak/go_market_analyze/src/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/src/helpers/kafkahelpers"
	"github.com/segmentio/kafka-go"
)

const (
	BLOCK_OVER = "BlockOver"
	SWAP_KAFKA_EVENT = "Swap"
	MINT_KAFKA_EVENT = "Mint"
	BURN_KAFKA_EVENT = "Burn"
)
type poolEvent struct {
	Type string `json:"type"`
	Data interface{} `json:"data"`
	BlockNumber uint64`json:"block_number"`
	Address string `json:"address"`
}


type kafkaClient struct {
	updateV3PricesWriter *kafka.Writer
}

func newKafkaClient (chainID uint, env *envhelper.Environment) (kafkaClient, error){
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{env.KAFKA_SERVER},
		Topic: kafkahelpers.GetUpdateV3PoolTopicByChainID(chainID, env.KAFKA_UPDATE_V3_POOLS_TOPIC),
		Balancer: &kafka.LeastBytes{},
		BatchTimeout:     10 * time.Millisecond,
	})
	if writer == nil {
		return kafkaClient{}, rpceventcollectorerrors.ErrUnableToInitKafka
	}
	client := kafkaClient{
		updateV3PricesWriter : writer,
	}
	return client, nil
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
