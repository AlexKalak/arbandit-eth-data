package eventcollectorservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	abiassets "github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorassets"
	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorerrors"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
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
	StartFromBlockV3(ctx context.Context, addresses []common.Address) error
}

type RPCEventsCollectorServiceConfig struct {
	ChainID                 uint
	MainnetRPCWS            string
	MainnetRPCHTTP          string
	KafkaServer             string
	KafkaUpdateV3PoolsTopic string
}

func (d *RPCEventsCollectorServiceConfig) validate() error {
	if d.ChainID == 0 {
		return errors.New("RPCEventsCollectorServiceConfig.ChainID cannot be empty")
	}
	if d.MainnetRPCWS == "" {
		return errors.New("RPCEventsCollectorServiceConfig.MainnetRPCWS cannot be empty")
	}
	if d.MainnetRPCHTTP == "" {
		return errors.New("RPCEventsCollectorServiceConfig.MainnetRPCWS cannot be empty")
	}
	if d.KafkaServer == "" {
		return errors.New("RPCEventsCollectorServiceConfig.KafkaServer cannot be empty")
	}
	if d.KafkaUpdateV3PoolsTopic == "" {
		return errors.New("RPCEventsCollectorServiceConfig.KafkaUpdateV3Poolstopic cannot be empty")
	}

	return nil
}

type RPCEventCollectorServiceDependencies struct {
}

func (d *RPCEventCollectorServiceDependencies) validate() error {
	return nil
}

type abiName string

const (
	_UNISWAP_V3_ABI_NAME     abiName = "uniswap_v3_events"
	_PANCAKESWAP_V3_ABI_NAME abiName = "pancakeswap_v3_events"
	_SUSHISWAP_V3_ABI_NAME   abiName = "sushiswap_v3_events"

	_UNISWAP_V2_ABI_Name abiName = "uniswap_v2_events"
)

type rpcEventsCollector struct {
	lastCheckedBlock     uint64
	lastCheckedBlockFile *os.File

	config RPCEventsCollectorServiceConfig

	abis map[abiName]v3ExchangeABI

	wsLogsClient   *ethclient.Client
	httpLogsClient *ethclient.Client
	kafkaClient    kafkaClient
	addresses      map[common.Address]any

	lastLogTime        time.Time
	lastLogBlockNumber uint64
	ticker             *time.Ticker

	v3PoolRepo v3poolsrepo.V3PoolDBRepo
}

func New(config RPCEventsCollectorServiceConfig, dependencies RPCEventCollectorServiceDependencies) (RPCEventsCollectorService, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	os.MkdirAll("blocks", 0777)
	var lastCheckedBlock uint64
	path := fmt.Sprintf("./blocks/%d.txt", config.ChainID)
	lastCheckedBlockFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	fmt.Println(absPath)

	blockBytes, err := io.ReadAll(lastCheckedBlockFile)
	if err != nil {
		return nil, err
	}
	lastCheckedBlockInt, err := strconv.Atoi(strings.TrimSpace(string(blockBytes)))
	if err != nil {
		return nil, err
	}
	lastCheckedBlock = uint64(lastCheckedBlockInt)

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
	fmt.Println("lastBlockNumber: ", lastCheckedBlock)

	return &rpcEventsCollector{
		lastCheckedBlock:     lastCheckedBlock,
		lastCheckedBlockFile: lastCheckedBlockFile,
		config:               config,
		abis: map[abiName]v3ExchangeABI{
			_UNISWAP_V3_ABI_NAME:     uniswapABI,
			_PANCAKESWAP_V3_ABI_NAME: pancakeswapABI,
			_SUSHISWAP_V3_ABI_NAME:   sushiswapABI,
		},
		lastLogTime: time.Time{},
		ticker:      time.NewTicker(quietDelay),
	}, nil
}

func (s *rpcEventsCollector) configure(ctx context.Context, addresses []common.Address) error {
	logsWsClient, err := ethclient.DialContext(ctx, s.config.MainnetRPCWS)
	if err != nil {
		fmt.Println("Unable to init ws logs clinet error", err)
		return eventcollectorerrors.ErrUnableToInitWsLogsClient
	}
	logsHTTPClient, err := ethclient.DialContext(ctx, s.config.MainnetRPCHTTP)
	if err != nil {
		fmt.Println("Unable to init http logs clinet error", err)
		logsWsClient.Close()
		return eventcollectorerrors.ErrUnableToInitWsLogsClient
	}

	kafkaClient, err := newKafkaClient(kafkaClientConfig{
		ChainID:     s.config.ChainID,
		KafkaServer: s.config.KafkaServer,
		KafkaTopic:  s.config.KafkaUpdateV3PoolsTopic,
	})

	if err != nil {
		logsWsClient.Close()
		logsHTTPClient.Close()
		return err
	}

	s.kafkaClient = kafkaClient

	s.wsLogsClient = logsWsClient
	s.httpLogsClient = logsHTTPClient

	s.addresses = map[common.Address]any{}
	for _, address := range addresses {
		s.addresses[address] = new(any)
	}
	return nil
}

func (s *rpcEventsCollector) setLastCheckedBlock(blockNumber uint64) error {
	err := s.lastCheckedBlockFile.Truncate(0)
	if err != nil {
		return err
	}

	// Move cursor to beginning (important!)
	_, err = s.lastCheckedBlockFile.Seek(0, 0)
	if err != nil {
		return err
	}

	_, err = s.lastCheckedBlockFile.Write([]byte(fmt.Sprint(blockNumber)))
	if err != nil {
		return err
	}
	s.lastCheckedBlock = blockNumber
	return nil
}
