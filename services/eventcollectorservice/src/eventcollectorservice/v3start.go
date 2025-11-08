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
	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorerrors"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func (s *rpcEventsCollector) StartFromBlockV3(ctx context.Context, addresses []common.Address) error {
	fmt.Println("Configuring RpcSyncService...")
	err := s.configure(ctx, addresses)
	if err != nil {
		fmt.Println("Got error ,err: ", err)
		return err
	}
	defer s.wsLogsClient.Close()

	uniswapABI, ok := s.abis[_UNISWAP_V3_ABI_NAME]
	if !ok {
		return errors.New("abi not found")
	}

	query := ethereum.FilterQuery{
		FromBlock: nil,
		Topics:    [][]common.Hash{{uniswapABI.SwapV3Sig}},
	}

	logsCh := make(chan types.Log, 1024)

	sub, err := s.wsLogsClient.SubscribeFilterLogs(ctx, query, logsCh)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	lastSentBlock, err := s.produceHistoryEventsFromBlock(ctx, big.NewInt(int64(s.lastCheckedBlock+1)))
	if err != nil {
		return err
	}

	fmt.Println("END PRELOADING, PRODUCING NEW MESSAGES")
	err = s.ListenNewLogs(ctx, sub, lastSentBlock, logsCh)
	if err != nil {
		s.StartFromBlockV3(ctx, addresses)
	}

	return nil
}

func (s *rpcEventsCollector) ListenNewLogs(ctx context.Context, sub ethereum.Subscription, fromBlock uint64, logsCh <-chan types.Log) error {
	logCount := 0
	for {
		select {
		case <-ctx.Done():
			return errors.New("rpc service stopped because of ctx done")
		case err := <-sub.Err():
			log.Println("subscription error:", err)
			return err
		case lg := <-logsCh:
			// process log (dedupe inside)
			if fromBlock > lg.BlockNumber {
				fmt.Println("skipping:", lg.BlockNumber)
				continue
			}

			s.lastLogTime = time.Now()
			s.lastLogBlockNumber = lg.BlockNumber

			if _, ok := s.addresses[lg.Address]; !ok {
				continue
			}

			poolEvent, err := s.handleLog(lg)
			if err != nil {
				log.Println("handleLog err:", err)
				continue
			}

			fmt.Println("a", lg.Address, lg.BlockNumber, poolEvent.Type, poolEvent.TxHash)
			fmt.Println("h", lg.TxHash.Hex())
			err = s.kafkaClient.sendUpdateV3PricesEvent(poolEvent)
			if err != nil {
				fmt.Println("KAFKA ERR: ", err)
			}

			logCount++

		case <-s.ticker.C:
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
		}
	}

}

func (s *rpcEventsCollector) produceHistoryEventsFromBlock(ctx context.Context, blockNumber *big.Int) (uint64, error) {
	logs, err := s.requireSwapEventsFromBlock(ctx, blockNumber)
	if err != nil {
		return 0, err
	}

	var currentBlock uint64 = 0

	eventsForBatch := make([]poolEvent, 10)
	batchIndex := -1

	for i, lg := range logs {
		batchIndex++

		if currentBlock == 0 {
			currentBlock = uint64(lg.BlockNumber)
		} else if currentBlock > (uint64(lg.BlockNumber)) {
			fmt.Println(lg.BlockNumber, lg.Address)
			panic("history events are not sorted")
		}

		currentEvent, err := s.handleLog(lg)
		if err != nil {
			log.Println("handleLog err:", err)
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

			s.setLastCheckedBlock(uint64(blockNumber.Int64()))

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

func (s *rpcEventsCollector) requireSwapEventsFromBlock(ctx context.Context, blockNumber *big.Int) ([]types.Log, error) {
	uniswapABI, ok := s.abis[_UNISWAP_V3_ABI_NAME]
	if !ok {
		return nil, errors.New("abi not found")
	}
	// pancakeswapABI, ok := s.abis[_PANCAKESWAP_V3_ABI_NAME]
	// if !ok {
	// 	return nil, errors.New("abi not found")
	// }
	// sushiswapABI, ok := s.abis[_SUSHISWAP_V3_ABI_NAME]
	// if !ok {
	// 	return nil, errors.New("abi not found")
	// }

	query := ethereum.FilterQuery{
		FromBlock: blockNumber,
		Topics:    [][]common.Hash{{uniswapABI.SwapV3Sig}},
	}

	logs, err := s.httpLogsClient.FilterLogs(ctx, query)
	fmt.Println("Len logs: ", len(logs))
	if err != nil {
		fmt.Println("Error quering logs: ", err)
		return nil, err
	}

	validLogs := make([]types.Log, 0, len(logs))
	for _, log := range logs {
		if _, ok := s.addresses[log.Address]; ok {
			validLogs = append(validLogs, log)
		}
	}

	return validLogs, nil
}

func (s *rpcEventsCollector) handleLog(lg types.Log) (poolEvent, error) {
	uniswapAbi, ok := s.abis[_UNISWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, eventcollectorerrors.ErrAbiError
	}
	pancakeSwapAbi, ok := s.abis[_PANCAKESWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, eventcollectorerrors.ErrAbiError
	}
	sushiswapAbi, ok := s.abis[_SUSHISWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, eventcollectorerrors.ErrAbiError
	}

	if len(lg.Topics) == 0 {
		return poolEvent{}, eventcollectorerrors.ErrNotTopicInLogs
	}

	switch lg.Topics[0] {
	case pancakeSwapAbi.SwapV3Sig,
		pancakeSwapAbi.MintV3Sig,
		pancakeSwapAbi.BurnV3Sig:
		return s.handlePancakeswapV3Log(lg)
	case uniswapAbi.SwapV3Sig,
		uniswapAbi.SwapV3Sig,
		uniswapAbi.SwapV3Sig:
		return s.handleUniswapV3Log(lg)
	case sushiswapAbi.SwapV3Sig,
		sushiswapAbi.MintV3Sig,
		sushiswapAbi.BurnV3Sig:
		return s.handleSushiswapV3Log(lg)
	}

	return poolEvent{}, eventcollectorerrors.ErrLogTypeNotFound
}

func (s *rpcEventsCollector) handlePancakeswapV3Log(lg types.Log) (poolEvent, error) {
	fmt.Println("Pancakeswap log: ", time.Unix(int64(lg.BlockTimestamp), 0))
	abiForEvent, ok := s.abis[_PANCAKESWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, fmt.Errorf("abi with name %s not found", _UNISWAP_V3_ABI_NAME)
	}

	switch lg.Topics[0] {
	case abiForEvent.SwapV3Sig:
		// Has own swap event abi
		ev, err := parsePancakeSwapV3SwapEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        SWAP_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil

	case abiForEvent.MintV3Sig:
		// Uses standard uniswap mint abi
		ev, err := parseUniswapV3MintEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        MINT_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil
	case abiForEvent.BurnV3Sig:
		// Uses standard uniswap mint abi
		ev, err := parseUniswapV3BurnEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        BURN_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil

	default:
		fmt.Println("Event topic not found")
	}
	return poolEvent{}, eventcollectorerrors.ErrLogTypeNotFound
}

func (s *rpcEventsCollector) handleSushiswapV3Log(lg types.Log) (poolEvent, error) {
	fmt.Println("Sushi swap log: ", time.Unix(int64(lg.BlockTimestamp), 0))
	abiForEvent, ok := s.abis[_SUSHISWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, fmt.Errorf("abi with name %s not found", _UNISWAP_V3_ABI_NAME)
	}

	switch lg.Topics[0] {
	case abiForEvent.SwapV3Sig:
		// Uses standard uniswap swap event abi
		ev, err := parseUniswapV3SwapEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        SWAP_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil

	case abiForEvent.MintV3Sig:
		// Uses standard uniswap mint event abi
		ev, err := parseUniswapV3MintEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        MINT_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil

	case abiForEvent.BurnV3Sig:
		// Uses standard uniswap burn event abi
		ev, err := parseUniswapV3BurnEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        BURN_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil

	default:
		fmt.Println("Event topic not found")
	}

	return poolEvent{}, eventcollectorerrors.ErrLogTypeNotFound
}

func (s *rpcEventsCollector) handleUniswapV3Log(lg types.Log) (poolEvent, error) {
	abiForEvent, ok := s.abis[_UNISWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, fmt.Errorf("abi with name %s not found", _UNISWAP_V3_ABI_NAME)
	}

	switch lg.Topics[0] {
	case abiForEvent.SwapV3Sig:
		ev, err := parseUniswapV3SwapEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        SWAP_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil
	case abiForEvent.MintV3Sig:
		ev, err := parseUniswapV3MintEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        MINT_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil
	case abiForEvent.BurnV3Sig:
		ev, err := parseUniswapV3BurnEvent(abiForEvent, lg)
		if err != nil {
			return poolEvent{}, err
		}

		return poolEvent{
			Type:        BURN_KAFKA_EVENT,
			Data:        ev,
			BlockNumber: lg.BlockNumber,
			Address:     strings.ToLower(lg.Address.Hex()),
			TxHash:      lg.TxHash.Hex(),
		}, nil

	default:
		fmt.Println("Event topic not found")
	}

	return poolEvent{}, eventcollectorerrors.ErrLogTypeNotFound
}

func parsePancakeSwapV3SwapEvent(abiForEvent v3ExchangeABI, lg types.Log) (pancakeswapV3SwapEvent, error) {
	var ev pancakeswapV3SwapEvent

	out, err := abiForEvent.ABI.Unpack("Swap", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Swap event", err)
		return ev, err
	}

	Amount0, ok := out[0].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[1].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	SqrtPriceX96, ok := out[2].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Liquidity, ok := out[3].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Tick, ok := out[4].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	ProtocolFeesToken0, ok := out[4].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	ProtocolFeesToken1, ok := out[4].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}

	Sender := common.HexToAddress(lg.Topics[1].Hex())
	Recipient := common.HexToAddress(lg.Topics[2].Hex())

	ev.Sender = Sender
	ev.Recipient = Recipient
	ev.Amount0 = Amount0
	ev.Amount1 = Amount1
	ev.SqrtPriceX96 = SqrtPriceX96
	ev.Liquidity = Liquidity
	ev.Tick = Tick
	ev.ProtocolFeesToken0 = ProtocolFeesToken0
	ev.ProtocolFeesToken1 = ProtocolFeesToken1

	return ev, nil
}

func parseUniswapV3SwapEvent(abiForEvent v3ExchangeABI, lg types.Log) (uniswapV3SwapEvent, error) {
	var ev uniswapV3SwapEvent
	out, err := abiForEvent.ABI.Unpack("Swap", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Swap event", err)
		return ev, err
	}

	Amount0, ok := out[0].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[1].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	SqrtPriceX96, ok := out[2].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Liquidity, ok := out[3].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Tick, ok := out[4].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Sender := common.HexToAddress(lg.Topics[1].Hex())
	Recipient := common.HexToAddress(lg.Topics[2].Hex())

	ev.Sender = Sender
	ev.Recipient = Recipient
	ev.Amount0 = Amount0
	ev.Amount1 = Amount1
	ev.SqrtPriceX96 = SqrtPriceX96
	ev.Liquidity = Liquidity
	ev.Tick = Tick

	return ev, nil
}

func parseUniswapV3MintEvent(abiForEvent v3ExchangeABI, lg types.Log) (uniswapV3MintEvent, error) {
	var ev uniswapV3MintEvent

	out, err := abiForEvent.ABI.Unpack("Mint", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Mint event", err)
		return ev, err
	}

	Sender, ok := out[0].(common.Address) // int256
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount, ok := out[1].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount0, ok := out[2].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[3].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}

	Owner := common.HexToAddress(lg.Topics[1].Hex())
	TickLower := parseTickFromTopic(lg.Topics[2])
	TickUpper := parseTickFromTopic(lg.Topics[3])

	ev.Sender = Sender
	ev.Owner = Owner
	ev.TickLower = TickLower
	ev.TickUpper = TickUpper
	ev.Amount = Amount
	ev.Amount0 = Amount0
	ev.Amount1 = Amount1
	return ev, nil
}

func parseUniswapV3BurnEvent(abiForEvent v3ExchangeABI, lg types.Log) (uniswapV3BurnEvent, error) {
	var ev uniswapV3BurnEvent

	out, err := abiForEvent.ABI.Unpack("Burn", lg.Data)
	if err != nil {
		fmt.Println("Error unpacking Burn event", err)
		return ev, err
	}

	Amount, ok := out[0].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount0 := out[1].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}
	Amount1 := out[2].(*big.Int)
	if !ok {
		return ev, eventcollectorerrors.ErrUnableToParseLog
	}

	Owner := common.HexToAddress(lg.Topics[1].Hex())
	TickLower := parseTickFromTopic(lg.Topics[2])
	TickUpper := parseTickFromTopic(lg.Topics[3])

	ev.Owner = Owner
	ev.TickLower = TickLower
	ev.TickUpper = TickUpper
	ev.Amount = Amount
	ev.Amount0 = Amount0
	ev.Amount1 = Amount1

	return ev, nil
}

func printPancakeswapSwapV3Event(blockNumber uint64, txHash common.Hash, ev pancakeswapV3SwapEvent) {
	fmt.Printf("Swap block number: %d, txHash: %s \n\t%s \n", blockNumber, txHash.String(), helpers.GetJSONString(ev))
}

func printSwapV3Event(blockNumber uint64, txHash common.Hash, ev uniswapV3SwapEvent) {
	fmt.Printf("Swap block number: %d, txHash: %s \n\t%s \n", blockNumber, txHash.String(), helpers.GetJSONString(ev))
}

func printMintV3Event(blockNumber uint64, txHash common.Hash, ev uniswapV3MintEvent) {
	fmt.Printf("Mint block number: %d, txHash: %s \n\t%s \n", blockNumber, txHash.String(), helpers.GetJSONString(ev))
}

func printBurnV3Event(blockNumber uint64, txHash common.Hash, ev uniswapV3BurnEvent) {
	fmt.Printf("Burn block number: %d, txHash: %s \n\t%s \n", blockNumber, txHash.String(), helpers.GetJSONString(ev))
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
