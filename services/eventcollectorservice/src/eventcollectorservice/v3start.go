package eventcollectorservice

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func (s *rpcEventsCollector) StartFromBlock(ctx context.Context, addresses []common.Address) error {
	fmt.Println("Configuring RpcSyncService...")
	err := s.configure(ctx, addresses)
	if err != nil {
		fmt.Println("Got error ,err: ", err)
		return err
	}
	defer s.wsLogsClient.Close()

	query := ethereum.FilterQuery{
		Addresses: addresses,
		Topics: [][]common.Hash{
			s.eventHandler.sigs,
		},
	}

	headCh := make(chan *types.Header, 1024)
	logsCh := make(chan types.Log, 1024)

	fmt.Println("Subscribing to head...")
	subHead, err := s.wsLogsClient.SubscribeNewHead(ctx, headCh)
	if err != nil {
		return err
	}

	fmt.Println("Subscribing to logs...")
	sub, err := s.wsLogsClient.SubscribeFilterLogs(ctx, query, logsCh)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	fmt.Println("Query header by number")
	currentHead, err := s.wsLogsClient.HeaderByNumber(ctx, nil)
	if err != nil {
		fmt.Println("Error getting last block header: ", err)
		time.Sleep(3 * time.Second)
		return s.StartFromBlock(ctx, addresses)
	}

	if currentHead.Number.Uint64()-s.lastCheckedBlock > 30 {
		err := s.mergeFromBlock(ctx, s.config.ChainID, currentHead.Number)
		if err != nil {
			return err
		}
		s.setLastCheckedBlock(currentHead.Number.Uint64())
	} else {
		lastSentBlock, err := s.produceHistoryEventsFromBlock(ctx, big.NewInt(int64(s.lastCheckedBlock+1)), currentHead.Number)
		if err != nil {
			if strings.Contains(err.Error(), "invalid params") {
				time.Sleep(3 * time.Second)
				s.StartFromBlock(ctx, addresses)
			}
			return err
		}
		s.setLastCheckedBlock(lastSentBlock)
	}

	fmt.Println("Got head: ", currentHead)
	fmt.Println("END PRELOADING, PRODUCING NEW MESSAGES")
	err = s.ListenNewLogs(ctx, sub, subHead, s.lastCheckedBlock, logsCh, headCh)
	if err != nil {
		s.StartFromBlock(ctx, addresses)
	}

	return nil
}

func (s *rpcEventsCollector) mergeFromBlock(ctx context.Context, chainID uint, blockNumber *big.Int) error {
	var err error
	fmt.Println("Merging pools...")
	err = s.merger.MergePools(chainID)
	if err != nil {
		fmt.Println("Merging pools error: ", err)
		return err
	}
	fmt.Println("Merging pools done.")

	// fmt.Println("Merging pools ticks...")
	// err = s.merger.MergePoolsTicks(ctx, chainID)
	// if err != nil {
	// 	fmt.Println("Merging pools ticks error: ", err)
	// 	return err
	// }
	// fmt.Println("Merging pools ticks done.")

	fmt.Println("Merging pools data...")
	err = s.merger.MergePoolsData(ctx, chainID, blockNumber)
	if err != nil {
		fmt.Println("Merging pools data error: ", err)
		return err
	}
	fmt.Println("Merging pools data done.")

	fmt.Println("Merging pairs...")
	err = s.merger.MergePairs(chainID)
	if err != nil {
		fmt.Println("Merging pairs error: ", err)
		return err
	}
	fmt.Println("Merging pairs done.")

	fmt.Println("Merging pairs data...")
	err = s.merger.MergePairsData(ctx, chainID, blockNumber)
	if err != nil {
		fmt.Println("Merging pairs data error: ", err)
		return err
	}
	fmt.Println("Merging pairs data done.")

	fmt.Println("Validating pairs ...")
	err = s.merger.ValidateV2PairsAndComputeAverageUSDPrice(chainID)
	if err != nil {
		fmt.Println("Validating pairs error: ", err)
		return err
	}
	fmt.Println("Validating pairs done.")

	fmt.Println("Validating pools ...")
	err = s.merger.ValidateV3PoolsAndComputeAverageUSDPrice(chainID)
	if err != nil {
		fmt.Println("Validating pools error: ", err)
		return err
	}
	fmt.Println("Validating pools done.")

	return nil
}

func (s *rpcEventsCollector) ListenNewLogs(ctx context.Context, sub ethereum.Subscription, headSub ethereum.Subscription, fromBlock uint64, logsCh <-chan types.Log, headsCh <-chan *types.Header) error {
	crashTicker := time.NewTicker(s.averageBlockTime * 2)
	logCount := 0
	for {
		select {
		case <-ctx.Done():
			return errors.New("rpc service stopped because of ctx done")
		case err := <-sub.Err():
			log.Println("logs subscription error:", err)
			return err
		case err := <-headSub.Err():
			log.Println("head subscription error:", err)
			return err
		case head := <-headsCh:
			if head == nil {
				continue
			}

			blockNum := head.Number.Uint64()

			s.headsAndLogsData.mu.Lock()

			s.headsAndLogsData.blockTimestamps[blockNum] = head.Time
			fmt.Println("blockNumber", blockNum, "timestamp", head.Time)

			if logs, ok := s.headsAndLogsData.pendingLogs[blockNum]; ok {
				fmt.Println("pending for block: ", blockNum)
				for _, log := range logs {
					s.processNewLog(log, head.Time)
				}
				delete(s.headsAndLogsData.pendingLogs, blockNum)
			}

			s.headsAndLogsData.mu.Unlock()

		case lg := <-logsCh:
			// process log (dedupe inside)
			if fromBlock > lg.BlockNumber {
				fmt.Println("skipping:", lg.BlockNumber)
				continue
			}

			s.headsAndLogsData.mu.Lock()

			ts, ok := s.headsAndLogsData.blockTimestamps[lg.BlockNumber]
			if ok {
				s.headsAndLogsData.mu.Unlock()
				s.processNewLog(lg, ts)
			} else {
				fmt.Println("Waiting for header...")
				s.headsAndLogsData.pendingLogs[lg.BlockNumber] = append(s.headsAndLogsData.pendingLogs[lg.BlockNumber], lg)
				s.headsAndLogsData.mu.Unlock()
			}

			logCount++

		case <-s.ticker.C:
			s.headsAndLogsData.mu.Lock()
			if len(s.headsAndLogsData.pendingLogs) > 0 {
				s.headsAndLogsData.mu.Unlock()
				continue
			}
			s.headsAndLogsData.mu.Unlock()

			if !s.lastLogTime.IsZero() && s.lastCheckedBlock < s.lastLogBlockNumber && time.Since(s.lastLogTime) > quietDelay {
				err := s.kafkaClient.sendUpdateV3PricesEvent(poolEvent{
					Type:        BLOCK_OVER,
					Data:        nil,
					BlockNumber: s.lastLogBlockNumber,
				})
				if err != nil {
					fmt.Println("KAFKA ERR: ", err)
				}
				s.setLastCheckedBlock(uint64(s.lastLogBlockNumber))
				fmt.Println("successful logs: ", logCount)

			}
		case <-crashTicker.C:
			fmt.Println("In crashTicker: ", time.Since(s.lastLogTime) > s.averageBlockTime*2)
			if time.Since(s.lastLogTime) > s.averageBlockTime*2 {
				fmt.Println("Not getting events properly, crashing listener.")
				return errors.New("Not getting events properly.")
			}

		}

	}
}

func (s *rpcEventsCollector) processNewLog(lg types.Log, blockTimestamp uint64) {
	s.lastLogTime = time.Now()
	s.lastLogBlockNumber = lg.BlockNumber

	if _, ok := s.addresses[lg.Address]; !ok {
		return
	}

	poolEvent, err := s.eventHandler.Handle(lg, blockTimestamp)
	if err != nil {
		log.Println("handleLog err:", err)
		return
	}

	if poolEvent.Address == "" {
		return
	}

	fmt.Println("a", lg.Address, poolEvent.Type, poolEvent.TxHash, poolEvent.TxTimestamp, lg.BlockNumber)

	err = s.kafkaClient.sendUpdateV3PricesEvent(poolEvent)
	if err != nil {
		fmt.Println("KAFKA ERR: ", err)
	}
}

func (s *rpcEventsCollector) produceHistoryEventsFromBlock(ctx context.Context, blockNumber *big.Int, headBlockNumber *big.Int) (uint64, error) {
	fmt.Println("Producing history events: ")
	fmt.Println("Requesting timestamps...")
	timestamps, err := s.getBlocksInfo(ctx, blockNumber.Uint64(), headBlockNumber.Uint64())
	if err != nil {
		return 0, err
	}
	fmt.Println("Got timestamps...")
	fmt.Println(helpers.GetJSONString(timestamps))

	fmt.Println("Requesting logs.")
	logs, err := s.getSwapEventsFromBlock(ctx, blockNumber, headBlockNumber, 1)
	if err != nil {
		return 0, err
	}
	fmt.Println("Got logs.")

	var currentBlock uint64 = 0

	eventsForBatch := make([]poolEvent, 10)
	batchIndex := -1

	for i, lg := range logs {
		batchIndex++

		timestamp, ok := timestamps[lg.BlockNumber]
		if !ok {
			fmt.Println("Not found timestamp: ", lg.BlockNumber)
			continue
		}

		if currentBlock == 0 {
			currentBlock = uint64(lg.BlockNumber)
		} else if currentBlock > (uint64(lg.BlockNumber)) {
			fmt.Println("blockNumber:", lg.BlockNumber, "currentBlock:", currentBlock)
			batchIndex--
			continue
		}

		currentEvent, err := s.eventHandler.Handle(lg, timestamp)
		if err != nil {
			log.Println("handleLog err:", err)
			batchIndex--
			continue
		}

		eventsForBatch[batchIndex] = currentEvent

		isLastEventInBlock := i < len(logs)-1 && logs[i+1].BlockNumber > uint64(currentBlock)

		if isLastEventInBlock || i == len(logs)-1 {
			events := eventsForBatch[:batchIndex+1]
			batchIndex = 0

			currentBlock = uint64(lg.BlockNumber)
			err = s.kafkaClient.sendUpdateV3PricesEvents(events)
			if err != nil {
				fmt.Println("KAFKA ERR: ", err)
			}
			batchIndex = 0

			fmt.Println("sending block over", currentBlock)
			err = s.kafkaClient.sendUpdateV3PricesEvent(poolEvent{
				Type:        BLOCK_OVER,
				Data:        nil,
				BlockNumber: currentBlock,
				Address:     lg.Address.Hex(),
			})
			if err != nil {
				fmt.Println("KAFKA ERR: ", err)
			}

			err = s.setLastCheckedBlock(uint64(blockNumber.Int64()))
			if err != nil {
				fmt.Println("error setting lastCheckedBlock: ", err)
			}

			batchIndex = -1
		} else if batchIndex == 9 {
			err = s.kafkaClient.sendUpdateV3PricesEvents(eventsForBatch)
			fmt.Println("sendingBatch")
			if err != nil {
				fmt.Println("KAFKA ERR: ", err)
			}
			batchIndex = -1
		}

	}

	return currentBlock, nil
}

func (s *rpcEventsCollector) getBlocksInfo(ctx context.Context, fromBlock uint64, toBlock uint64) (map[uint64]uint64, error) {
	timestamps := map[uint64]uint64{}

	for blockNumber := fromBlock; blockNumber <= toBlock; blockNumber++ {
		fmt.Println("Requesting header for block: ", blockNumber)
		header, err := s.wsLogsClient.HeaderByNumber(ctx, big.NewInt(int64(blockNumber)))
		if err != nil {
			return nil, err
		}

		timestamps[blockNumber] = header.Time
	}

	return timestamps, nil
}

func (s *rpcEventsCollector) getSwapEventsFromBlock(ctx context.Context, blockNumber *big.Int, currentHeadBlockNumber *big.Int, chunksCount int) ([]types.Log, error) {
	fmt.Println("requiring old logs chunks:", chunksCount)
	if chunksCount <= 0 {
		chunksCount = 1
	}

	validLogs := make([]types.Log, 0)
	blocksByChunk := new(big.Int).Sub(currentHeadBlockNumber, blockNumber)
	blocksByChunk.Div(blocksByChunk, big.NewInt(int64(chunksCount)))

	for i := range chunksCount {
		query := ethereum.FilterQuery{
			FromBlock: blockNumber,
			ToBlock: new(big.Int).Add(
				blockNumber,
				new(big.Int).Mul(
					blocksByChunk,
					big.NewInt(int64(i+1)),
				)),
			Topics: [][]common.Hash{s.eventHandler.sigs},
		}

		ctxWithTime, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		logs, err := s.wsLogsClient.FilterLogs(ctxWithTime, query)

		fmt.Println("Len logs: ", len(logs))

		if err != nil {
			fmt.Println("Error quering logs: ", err)
			time.Sleep(1)
			if strings.Contains(err.Error(), "invalid params") {
				return nil, err
			}
			return s.getSwapEventsFromBlock(ctx, blockNumber, currentHeadBlockNumber, chunksCount+20)
		}

		for _, log := range logs {
			if _, ok := s.addresses[log.Address]; ok {
				validLogs = append(validLogs, log)
			}
		}

	}

	return validLogs, nil
}
