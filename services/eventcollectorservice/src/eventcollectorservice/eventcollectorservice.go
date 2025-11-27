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
	"sync"
	"time"

	"github.com/alexkalak/go_market_analyze/services/eventcollectorservice/src/eventcollectorservice/eventhandles"
	"github.com/alexkalak/go_market_analyze/services/merging/src/merger"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const quietDelay = 150 * time.Millisecond

type RPCEventsCollectorService interface {
	StartFromBlock(ctx context.Context, addresses []common.Address) error
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
	Merger merger.Merger
}

func (d *RPCEventCollectorServiceDependencies) validate() error {
	if d.Merger == nil {
		return errors.New("RPCEventsCollectorServiceDependencies.Merger cannot be nil")
	}
	return nil
}

type headAndLogsSync struct {
	mu sync.Mutex
	//blocknumber -> blocktimestamp
	blockTimestamps map[uint64]uint64
	//blocknumber -> logs
	pendingLogs map[uint64][]types.Log
}

func newEventHandler(v3EventHandler eventhandles.V3EventHandler, v2EventHandler eventhandles.V2EventHandler) eventHandler {
	return eventHandler{
		v3EventHandler: v3EventHandler,
		v2EventHandler: v2EventHandler,
		sigs:           append(v3EventHandler.AllEventSigs, v2EventHandler.AllEventSigs...),
		// sigs:           v2EventHandler.AllEventSigs,
	}

}

type eventHandler struct {
	v3EventHandler eventhandles.V3EventHandler
	v2EventHandler eventhandles.V2EventHandler
	sigs           []common.Hash
}

func (h *eventHandler) Handle(lg types.Log, timestamp uint64) (poolEvent, error) {
	sig := lg.Topics[0]

	poolEv := poolEvent{}
	if h.v3EventHandler.HasSig(sig) {
		ev, err := h.v3EventHandler.Handle(lg, int(timestamp))
		if err != nil {
			return poolEvent{}, err

		}
		poolEv.FromV3PoolEvent(ev)
		return poolEv, nil
	}

	if h.v2EventHandler.HasSig(sig) {
		ev, err := h.v2EventHandler.Handle(lg, int(timestamp))
		if err != nil {
			return poolEvent{}, err

		}
		poolEv.FromV2PoolEvent(ev)
		return poolEv, nil
	}

	return poolEvent{}, errors.New("Invalid log signature")
}

type rpcEventsCollector struct {
	lastCheckedBlock     uint64
	lastCheckedBlockFile *os.File

	config RPCEventsCollectorServiceConfig

	wsLogsClient   *ethclient.Client
	httpLogsClient *ethclient.Client
	kafkaClient    kafkaClient
	addresses      map[common.Address]any

	averageBlockTime   time.Duration
	lastLogTime        time.Time
	lastLogBlockNumber uint64
	ticker             *time.Ticker

	eventHandler eventHandler

	headsAndLogsData *headAndLogsSync

	merger merger.Merger
}

func New(config RPCEventsCollectorServiceConfig, dependencies RPCEventCollectorServiceDependencies) (RPCEventsCollectorService, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	lastCheckedBlock, lastCheckedBlockFile, err := loadLastCheckedBlock(config.ChainID)
	if err != nil {
		return nil, err
	}

	v3EventHandler, err := eventhandles.NewV3EventHandler()
	if err != nil {
		return nil, err
	}
	v2EventHandler, err := eventhandles.NewV2EventHandler()
	if err != nil {
		return nil, err
	}

	fmt.Println("lastBlockNumber: ", lastCheckedBlock)

	return &rpcEventsCollector{
		lastCheckedBlock:     lastCheckedBlock,
		lastCheckedBlockFile: lastCheckedBlockFile,

		config:           config,
		averageBlockTime: 14 * time.Second,
		lastLogTime:      time.Time{},
		ticker:           time.NewTicker(quietDelay),
		headsAndLogsData: nil,

		eventHandler: newEventHandler(v3EventHandler, v2EventHandler),
		merger:       dependencies.Merger,
	}, nil
}

func loadLastCheckedBlock(chainID uint) (uint64, *os.File, error) {
	os.MkdirAll("blocks", 0777)
	var lastCheckedBlock uint64

	path := fmt.Sprintf("./blocks/%d.txt", chainID)
	lastCheckedBlockFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return 0, nil, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return 0, nil, err
	}
	fmt.Println(absPath)

	blockBytes, err := io.ReadAll(lastCheckedBlockFile)
	if err != nil {
		return 0, nil, err
	}

	lastCheckedBlockInt, err := strconv.Atoi(strings.TrimSpace(string(blockBytes)))
	if err != nil {
		return 0, nil, err
	}
	lastCheckedBlock = uint64(lastCheckedBlockInt)

	return lastCheckedBlock, lastCheckedBlockFile, nil
}

func (s *rpcEventsCollector) configure(ctx context.Context, addresses []common.Address) error {
	fmt.Println("Requesting ws client...")
	logsWsClient, err := ethclient.DialContext(ctx, s.config.MainnetRPCWS)
	if err != nil {
		fmt.Println("Unable to init ws logs clinet error, waiting for 3 secs...", err)
		time.Sleep(3 * time.Second)
		return s.configure(ctx, addresses)
	}

	fmt.Println("Requesting http client...")
	logsHTTPClient, err := ethclient.DialContext(ctx, s.config.MainnetRPCHTTP)
	if err != nil {
		fmt.Println("Unable to init http logs clinet error, waiting for 3 secs...", err)
		logsWsClient.Close()
		time.Sleep(3 * time.Second)
		return s.configure(ctx, addresses)
	}

	fmt.Println("Initializing kafka client...")
	kafkaClient, err := newKafkaClient(kafkaClientConfig{
		ChainID:     s.config.ChainID,
		KafkaServer: s.config.KafkaServer,
		KafkaTopic:  s.config.KafkaUpdateV3PoolsTopic,
	})

	if err != nil {
		logsWsClient.Close()
		logsHTTPClient.Close()
		fmt.Println("Unable to init kafka client error, waiting for 3 secs...", err)
		time.Sleep(3 * time.Second)
		return s.configure(ctx, addresses)
	}

	s.kafkaClient = kafkaClient

	s.wsLogsClient = logsWsClient
	s.httpLogsClient = logsHTTPClient

	s.addresses = map[common.Address]any{}

	fmt.Println("Creating addresses map...")
	for _, address := range addresses {
		s.addresses[address] = new(any)
	}

	s.headsAndLogsData = &headAndLogsSync{
		blockTimestamps: map[uint64]uint64{},
		pendingLogs:     map[uint64][]types.Log{},
	}

	fmt.Println("Configuration done.")
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
