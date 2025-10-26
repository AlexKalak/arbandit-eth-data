package rpceventscollectorservice

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/alexkalak/go_market_analyze/src/assets/abiassets"
	"github.com/alexkalak/go_market_analyze/src/errors/rpcerrors"
	"github.com/alexkalak/go_market_analyze/src/errors/rpceventcollectorerrors"
	"github.com/alexkalak/go_market_analyze/src/helpers"
	"github.com/alexkalak/go_market_analyze/src/helpers/envhelper"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v3poolsrepo"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const quietDelay = 150 * time.Millisecond

type v3ExchangeABI struct {
	ABI        abi.ABI
	SwapV3Sig  common.Hash
	MintV3Sig  common.Hash
	BurnV3Sig  common.Hash
	FlashV3Sig common.Hash
}

type RPCEventsCollectorService interface {
	Start(ctx context.Context) error
}

type RPCEventCollectorServiceDependencies struct {
	Env        *envhelper.Environment
	V3PoolRepo v3poolsrepo.V3PoolDBRepo
}

type abiName string

const (
	_UNISWAP_V3_ABI_NAME     abiName = "uniswap_v3_events"
	_PANCAKESWAP_V3_ABI_NAME abiName = "pancakeswap_v3_events"
	_SUSHISWAP_V3_ABI_NAME   abiName = "sushiswap_v3_events"
)

type rpcEventsCollector struct {
	chainID uint

	abis map[abiName]v3ExchangeABI

	wsLogsClient   *ethclient.Client
	httpLogsClient *ethclient.Client
	kafkaClient    kafkaClient
	addresses      []common.Address

	lastLogTime           time.Time
	lastLogBlockNumber    uint64
	lastOveredBlockNumber uint64
	ticker                *time.Ticker

	env        *envhelper.Environment
	v3PoolRepo v3poolsrepo.V3PoolDBRepo
}

func New(chainID uint, dependencies RPCEventCollectorServiceDependencies) (RPCEventsCollectorService, error) {
	if dependencies.Env == nil ||
		dependencies.Env.ETH_MAINNET_RPC_WS == "" ||
		dependencies.V3PoolRepo == nil {
		return nil, rpceventcollectorerrors.ErrInvalidDependencies
	}

	uniswapV3EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABIUniswapV3String))
	if err != nil {
		return nil, err
	}
	pancakeswapV3EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABIPancakeswapV3String))
	if err != nil {
		return nil, err
	}
	sushiswapV3EventsABI, err := abi.JSON(strings.NewReader(abiassets.EventsABISushiswapV3String))
	if err != nil {
		return nil, err
	}

	uniswapABI := v3ExchangeABI{
		ABI:        uniswapV3EventsABI,
		SwapV3Sig:  uniswapV3EventsABI.Events["Swap"].ID,
		MintV3Sig:  uniswapV3EventsABI.Events["Mint"].ID,
		BurnV3Sig:  uniswapV3EventsABI.Events["Burn"].ID,
		FlashV3Sig: uniswapV3EventsABI.Events["Flash"].ID,
	}
	pancakeswapABI := v3ExchangeABI{
		ABI:       pancakeswapV3EventsABI,
		SwapV3Sig: pancakeswapV3EventsABI.Events["Swap"].ID,
		MintV3Sig: pancakeswapV3EventsABI.Events["Mint"].ID,
		BurnV3Sig: pancakeswapV3EventsABI.Events["Burn"].ID,
	}
	sushiswapABI := v3ExchangeABI{
		ABI:       sushiswapV3EventsABI,
		SwapV3Sig: sushiswapV3EventsABI.Events["Swap"].ID,
		MintV3Sig: sushiswapV3EventsABI.Events["Mint"].ID,
		BurnV3Sig: sushiswapV3EventsABI.Events["Burn"].ID,
	}

	fmt.Println(uniswapABI.SwapV3Sig)
	fmt.Println(uniswapABI.MintV3Sig)
	fmt.Println(uniswapABI.BurnV3Sig)
	fmt.Println(uniswapABI.FlashV3Sig)

	return &rpcEventsCollector{
		chainID: chainID,
		abis: map[abiName]v3ExchangeABI{
			_UNISWAP_V3_ABI_NAME:     uniswapABI,
			_PANCAKESWAP_V3_ABI_NAME: pancakeswapABI,
			_SUSHISWAP_V3_ABI_NAME:   sushiswapABI,
		},
		env:         dependencies.Env,
		v3PoolRepo:  dependencies.V3PoolRepo,
		lastLogTime: time.Time{},
		ticker:      time.NewTicker(quietDelay),
	}, nil
}

func (s *rpcEventsCollector) Start(ctx context.Context) error {
	fmt.Println("Configuring RpcSyncService...")
	err := s.configure(ctx)
	if err != nil {
		fmt.Println("Got error ,err: ", err)
		return err
	}
	defer s.wsLogsClient.Close()
	fmt.Println("RpcSyncService configured.")

	query := ethereum.FilterQuery{
		Addresses: s.addresses,
	}

	logsCh := make(chan types.Log, 1024)
	sub, err := s.wsLogsClient.SubscribeFilterLogs(ctx, query, logsCh)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	logCount := 0
	for {
		select {
		case <-ctx.Done():
			return errors.New("rpc service stopped because of ctx done")
			// case err := <-sub.Err():
			// 	log.Println("subscription error:", err)
			// return err
		case lg := <-logsCh:
			// process log (dedupe inside)
			// fmt.Println("got log: ", lg.BlockNumber, lg.BlockTimestamp, time.Unix(int64(lg.BlockTimestamp), 0))

			s.lastLogTime = time.Now()
			s.lastLogBlockNumber = lg.BlockNumber

			poolEvent, err := s.handleLog(lg)
			if err != nil {
				log.Println("handleLog err:", err)
				continue
			}

			err = s.kafkaClient.sendUpdateV3PricesEvent(poolEvent)
			if err != nil {
				fmt.Println("KAFKA ERR: ", err)
			}

			logCount++
			// fmt.Println("successful logs: ", logCount)

		case <-s.ticker.C:
			if !s.lastLogTime.IsZero() && s.lastOveredBlockNumber < s.lastLogBlockNumber && time.Since(s.lastLogTime) > quietDelay {
				s.lastOveredBlockNumber = s.lastLogBlockNumber

				err = s.kafkaClient.sendUpdateV3PricesEvent(poolEvent{
					Type:        BLOCK_OVER,
					Data:        nil,
					BlockNumber: s.lastLogBlockNumber,
				})
				if err != nil {
					fmt.Println("KAFKA ERR: ", err)
				}
				logCount++
				fmt.Println("successful logs: ", logCount)

			}
		}
	}
}

func (s *rpcEventsCollector) configure(ctx context.Context) error {
	logsWsClient, err := ethclient.DialContext(ctx, s.env.ETH_MAINNET_RPC_WS)
	if err != nil {
		fmt.Println("Unable to init ws logs clinet error", err)
		return rpceventcollectorerrors.ErrUnableToInitWsLogsClient
	}
	logsHTTPClient, err := ethclient.DialContext(ctx, s.env.ETH_MAINNET_RPC_HTTP)
	if err != nil {
		fmt.Println("Unable to init http logs clinet error", err)
		logsWsClient.Close()
		return rpceventcollectorerrors.ErrUnableToInitWsLogsClient
	}

	kafkaClient, err := newKafkaClient(s.chainID, s.env)
	if err != nil {
		logsWsClient.Close()
		logsHTTPClient.Close()
	}
	s.kafkaClient = kafkaClient

	pools, err := s.v3PoolRepo.GetPoolsByChainID(s.chainID)
	if err != nil {
		logsWsClient.Close()
		logsHTTPClient.Close()
		return err
	}
	poolAddresses := []common.Address{}

	for _, pool := range pools {
		poolAddresses = append(poolAddresses, common.HexToAddress(pool.Address))
	}

	s.wsLogsClient = logsWsClient
	s.httpLogsClient = logsHTTPClient
	s.addresses = poolAddresses
	return nil
}

func (s *rpcEventsCollector) imitateEventsForBlock(ctx context.Context, blockNumber *big.Int, lgChan chan<- types.Log) error {
	abiForEvent, ok := s.abis[_PANCAKESWAP_V3_ABI_NAME]
	if !ok {
		return errors.New("abi not found")
	}

	fmt.Println(abiForEvent.SwapV3Sig)
	query := ethereum.FilterQuery{
		FromBlock: blockNumber,
		Topics:    [][]common.Hash{{abiForEvent.SwapV3Sig}},
		Addresses: s.addresses,
	}

	logs, err := s.httpLogsClient.FilterLogs(ctx, query)
	fmt.Println("Len logs: ", len(logs))
	if err != nil {
		fmt.Println("Error quering logs: ", err)
		return err
	}

	for _, log := range logs {
		lgChan <- log
	}

	return nil
}

func (s *rpcEventsCollector) handleLog(lg types.Log) (poolEvent, error) {
	uniswapAbi, ok := s.abis[_UNISWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, rpceventcollectorerrors.ErrAbiError
	}
	pancakeSwapAbi, ok := s.abis[_PANCAKESWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, rpceventcollectorerrors.ErrAbiError
	}
	sushiswapAbi, ok := s.abis[_SUSHISWAP_V3_ABI_NAME]
	if !ok {
		return poolEvent{}, rpceventcollectorerrors.ErrAbiError
	}

	if len(lg.Topics) == 0 {
		return poolEvent{}, rpcerrors.ErrNoTopicsInLog
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

	return poolEvent{}, rpceventcollectorerrors.ErrLogTypeNotFound
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
		}, nil

	default:
		fmt.Println("Event topic not found")
	}
	return poolEvent{}, rpceventcollectorerrors.ErrLogTypeNotFound
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
		}, nil

	default:
		fmt.Println("Event topic not found")
	}

	return poolEvent{}, rpceventcollectorerrors.ErrLogTypeNotFound
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
		}, nil

	default:
		fmt.Println("Event topic not found")
	}

	return poolEvent{}, rpceventcollectorerrors.ErrLogTypeNotFound
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
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[1].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	SqrtPriceX96, ok := out[2].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Liquidity, ok := out[3].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Tick, ok := out[4].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	ProtocolFeesToken0, ok := out[4].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	ProtocolFeesToken1, ok := out[4].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
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
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[1].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	SqrtPriceX96, ok := out[2].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Liquidity, ok := out[3].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Tick, ok := out[4].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
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
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount, ok := out[1].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount0, ok := out[2].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount1, ok := out[3].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
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
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount0 := out[1].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
	}
	Amount1 := out[2].(*big.Int)
	if !ok {
		return ev, rpceventcollectorerrors.ErrUnableToParseLog
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
