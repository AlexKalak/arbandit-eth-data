package kafkahelpers

import (
	"fmt"
)

func GetUpdateV3PoolTopicByChainID(chainID uint, kafkaUpdateV3PoolTopicHeader string) string {
	return fmt.Sprintf("%s.%d", kafkaUpdateV3PoolTopicHeader, chainID)
}
