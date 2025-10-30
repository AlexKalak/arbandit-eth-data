package poolupdaterservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

type poolEventMetaData struct {
	Type        string `json:"type"`
	BlockNumber uint64 `json:"block_number"`
	Address     string `json:"address"`
}

type swapEventData struct {
	Sender       common.Address `json:"sender"`
	Recipient    common.Address `json:"repcipient"`
	Amount0      *big.Int       `json:"amount0"`
	Amount1      *big.Int       `json:"amount1"`
	SqrtPriceX96 *big.Int       `json:"sqrt_price_x96"`
	Liquidity    *big.Int       `json:"liquidity"`
	Tick         *big.Int       `json:"tick"`
}

type mintEventData struct {
	Sender    common.Address `json:"sender"`
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type burnEventData struct {
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type poolEventData[T swapEventData | mintEventData | burnEventData] struct {
	Data T `json:"data"`
}

func (s *poolUpdaterService) Start(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{s.config.KafkaServer},
		Topic:   s.config.KafkaUpdateV3PoolsTopic,
		GroupID: "ssanina",
	})
	defer reader.Close()

	fmt.Println("🚀 Listening for messages...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("failed to read message:", err)
			continue
		}

		err = s.handlePoolEventMessage(ctx, &m)
		if err != nil {
			fmt.Println("Error parsing message: ", err)
			continue
		}

	}
}

func (s *poolUpdaterService) handlePoolEventMessage(ctx context.Context, m *kafka.Message) error {
	fmt.Println("handling message")
	if m == nil {
		return errors.New("nil message")
	}

	metaData := poolEventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	if metaData.Type == "BlockOver" {
		fmt.Println("BLOCK OVER: ", metaData.BlockNumber)
		fmt.Println(s.currentCheckingBlock, metaData.BlockNumber)

		if s.currentCheckingBlock == metaData.BlockNumber {
			pools := make([]models.UniswapV3Pool, 0, len(s.currentBlockPoolChanges))

			for _, pool := range s.currentBlockPoolChanges {
				pool.BlockNumber = int(metaData.BlockNumber)
				pools = append(pools, pool)
			}

			for vpi := range s.currentBlockPoolChanges {
				delete(s.currentBlockPoolChanges, vpi)
			}

			fmt.Println(helpers.GetJSONString(pools))

			s.v3PoolCacheRepo.SetPools(s.config.ChainID, pools)
		}

		return nil
	}

	s.currentCheckingBlock = metaData.BlockNumber

	switch metaData.Type {
	case "Swap":
		data := poolEventData[swapEventData]{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			return err
		}

		poolIdentificator := models.V3PoolIdentificator{
			Address: metaData.Address,
			ChainID: s.config.ChainID,
		}

		fmt.Println(s.config.ChainID, "Swap event: ", helpers.GetJSONString(data))
		fmt.Println(helpers.GetJSONString(poolIdentificator))

		pool := models.UniswapV3Pool{}
		if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
			pool = existingPool
		} else {
			var err error
			pool, err = s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
			if err != nil {
				return err
			}
		}

		pool.Liquidity = data.Data.Liquidity
		pool.SqrtPriceX96 = data.Data.SqrtPriceX96
		pool.Tick = int(data.Data.Tick.Int64())

		s.currentBlockPoolChanges[poolIdentificator] = pool
	case "Mint":
		data := poolEventData[mintEventData]{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			return err
		}

		poolIdentificator := models.V3PoolIdentificator{
			Address: metaData.Address,
			ChainID: s.config.ChainID,
		}

		fmt.Println(s.config.ChainID, "Mint event: ", helpers.GetJSONString(data))
		fmt.Println(helpers.GetJSONString(poolIdentificator))

		pool := models.UniswapV3Pool{}
		if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
			pool = existingPool
		} else {
			var err error
			pool, err = s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
			if err != nil {
				return err
			}
		}

		if data.Data.TickLower < int32(pool.Tick) && int32(pool.Tick) < data.Data.TickUpper {
			pool.Liquidity.Add(pool.Liquidity, data.Data.Amount)
		}

		s.currentBlockPoolChanges[poolIdentificator] = pool
	case "Burn":
		data := poolEventData[burnEventData]{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			return err
		}

		poolIdentificator := models.V3PoolIdentificator{
			Address: metaData.Address,
			ChainID: s.config.ChainID,
		}

		fmt.Println(s.config.ChainID, "Burn event: ", helpers.GetJSONString(data))
		fmt.Println(helpers.GetJSONString(poolIdentificator))

		pool := models.UniswapV3Pool{}
		if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
			pool = existingPool
		} else {
			var err error
			pool, err = s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
			if err != nil {
				return err
			}
		}

		if data.Data.TickLower < int32(pool.Tick) && int32(pool.Tick) < data.Data.TickUpper {
			pool.Liquidity.Sub(pool.Liquidity, data.Data.Amount)
		}

		s.currentBlockPoolChanges[poolIdentificator] = pool

	default:
		return errors.New("message type not found")

	}

	return nil
}
