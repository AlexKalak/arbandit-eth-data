package poolupdaterservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v2transactionrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
	"github.com/segmentio/kafka-go"
)

type StateUpdaterService interface {
	Start(ctx context.Context) error
}

type StateUpdaterServiceDependencies struct {
	TokenDBRepo    tokenrepo.TokenRepo
	TokenCacheRepo tokenrepo.TokenCacheRepo

	V2PairCacheRepo        v2pairsrepo.V2PairCacheRepo
	V2PairDBRepo           v2pairsrepo.V2PairDBRepo
	V2TransactionDBRepo    v2transactionrepo.V2TransactionDBRepo
	V2TransactionCacheRepo v2transactionrepo.V2TransactionCacheRepo

	V3PoolDBRepo           v3poolsrepo.V3PoolDBRepo
	V3PoolCacheRepo        v3poolsrepo.V3PoolCacheRepo
	V3TransactionDBRepo    v3transactionrepo.V3TransactionDBRepo
	V3TransactionCacheRepo v3transactionrepo.V3TransactionCacheRepo
}

func (d *StateUpdaterServiceDependencies) validate() error {
	if d.TokenDBRepo == nil {
		return errors.New("pool updater service dependencies TokenDBRepo cannot be nil")
	}
	if d.TokenCacheRepo == nil {
		return errors.New("pool updater service dependencies TokenCacheRepo cannot be nil")
	}

	if d.V2PairDBRepo == nil {
		return errors.New("pool updater service dependencies V2PairDBRepo cannot be nil")
	}
	if d.V2PairCacheRepo == nil {
		return errors.New("pool updater service dependencies V2PairCacheRepo cannot be nil")
	}
	if d.V2TransactionCacheRepo == nil {
		return errors.New("pool updater service dependencies V2TransactionCacheRepo cannot be nil")
	}
	if d.V2TransactionDBRepo == nil {
		return errors.New("pool updater service dependencies V2TransactionDBRepo cannot be nil")
	}

	if d.V3PoolDBRepo == nil {
		return errors.New("pool updater service dependencies V3PoolDBRepo cannot be nil")
	}
	if d.V3PoolCacheRepo == nil {
		return errors.New("pool updater service dependencies V3PoolCacheRepo cannot be nil")
	}
	if d.V3TransactionDBRepo == nil {
		return errors.New("pool updater service dependencies V3TransactionDBRepo cannot be nil")
	}
	if d.V3TransactionCacheRepo == nil {
		return errors.New("pool updater service dependencies V3TransactionCacheRepo cannot be nil")
	}

	return nil
}

type StateUpdaterServiceConfig struct {
	ChainID                 uint
	KafkaServer             string
	KafkaUpdateV3PoolsTopic string
}

func (d *StateUpdaterServiceConfig) validate() error {
	if d.ChainID == 0 {
		return errors.New("pool updater service config ChainID not set")
	}

	if d.KafkaServer == "" {
		return errors.New("pool updater service config KafkaServer not set")
	}

	if d.KafkaUpdateV3PoolsTopic == "" {
		return errors.New("pool updater service config KafkaUpdateV3PoolsTopic not set")
	}

	return nil
}

type stateUpdater struct {
	currentBlockUpdatedTokens map[string]any
	tokensMapForCache         map[models.TokenIdentificator]*models.Token
	tokensMapForDB            map[models.TokenIdentificator]*models.Token

	currentCheckingBlock uint64

	config StateUpdaterServiceConfig

	tokenDBRepo    tokenrepo.TokenRepo
	tokenCacheRepo tokenrepo.TokenCacheRepo

	messageCount   int
	lastTimeLogged time.Time

	//updaters
	poolUpdater poolUpdater
	pairUpdater pairUpdater
}

func New(config StateUpdaterServiceConfig, dependencies StateUpdaterServiceDependencies) (StateUpdaterService, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	service := stateUpdater{
		config: config,

		currentBlockUpdatedTokens: map[string]any{},
		tokenDBRepo:               dependencies.TokenDBRepo,
		tokenCacheRepo:            dependencies.TokenCacheRepo,

		currentCheckingBlock: 0,
	}

	tokens, err := service.tokenDBRepo.GetTokensByChainID(config.ChainID)
	if err != nil {
		return nil, err
	}

	service.tokensMapForDB = map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		service.tokensMapForDB[token.GetIdentificator()] = token
	}
	tokens, err = service.tokenDBRepo.GetTokensByChainID(config.ChainID)
	if err != nil {
		return nil, err
	}

	service.tokensMapForCache = map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		service.tokensMapForCache[token.GetIdentificator()] = token
	}

	poolUpdater, err := newPoolUpdater(
		config.ChainID,
		service.tokensMapForCache,
		service.tokensMapForDB,
		service.currentBlockUpdatedTokens,
		poolUpdaterDependencies{
			tokenDBRepo:            dependencies.TokenDBRepo,
			V3PoolDBRepo:           dependencies.V3PoolDBRepo,
			V3PoolCacheRepo:        dependencies.V3PoolCacheRepo,
			V3TransactionDBRepo:    dependencies.V3TransactionDBRepo,
			V3TransactionCacheRepo: dependencies.V3TransactionCacheRepo,
		},
	)
	if err != nil {
		return nil, err
	}

	pairUpdater, err := newPairUpdater(
		config.ChainID,
		service.tokensMapForCache,
		service.tokensMapForDB,
		service.currentBlockUpdatedTokens,
		pairUpdaterDependencies{
			tokenDBRepo:            dependencies.TokenDBRepo,
			V2PairDBRepo:           dependencies.V2PairDBRepo,
			V2PairCacheRepo:        dependencies.V2PairCacheRepo,
			V2TransactionDBRepo:    dependencies.V2TransactionDBRepo,
			V2TransactionCacheRepo: dependencies.V2TransactionCacheRepo,
		},
	)
	if err != nil {
		return nil, err
	}

	service.poolUpdater = poolUpdater
	service.pairUpdater = pairUpdater

	return &service, nil
}

func (s *stateUpdater) Start(ctx context.Context) error {
	poolDBChannel := make(chan *kafka.Message, 1024)
	go s.poolUpdater.startPostgresUpdater(ctx, poolDBChannel)

	pairDBChannel := make(chan *kafka.Message, 1024)
	go s.pairUpdater.startPostgresUpdater(ctx, pairDBChannel)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{s.config.KafkaServer},
		Topic:   s.config.KafkaUpdateV3PoolsTopic,
		GroupID: "ssanina228133",
	})

	defer reader.Close()

	fmt.Println("🚀 Listening for messages...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("failed to read message:", err)
			continue
		}

		s.handleMessage(&m, poolDBChannel, pairDBChannel)
	}
}

func (s *stateUpdater) handleMessage(m *kafka.Message, poolDBChannel, pairDBChannel chan<- *kafka.Message) error {
	metaData := eventMetaData{}

	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	if metaData.Type == "BlockOver" && s.currentCheckingBlock == metaData.BlockNumber {
		fmt.Println("BLOCK OVER: ", metaData.BlockNumber)
		fmt.Println(s.currentCheckingBlock, metaData.BlockNumber)

		s.pairUpdater.onBlockOver(m, metaData.BlockNumber)
		s.poolUpdater.onBlockOver(m, metaData.BlockNumber)
		s.handleBlockOver()

		return nil
	}

	s.currentCheckingBlock = metaData.BlockNumber

	if time.Since(s.lastTimeLogged) > time.Second {
		fmt.Println(s.lastTimeLogged, "messages")
		s.messageCount = 0
		s.lastTimeLogged = time.Now()
	}

	s.poolUpdater.handlePoolEventMessageForCache(m)
	s.pairUpdater.handlePairEventMessageForCache(m)
	poolDBChannel <- m
	pairDBChannel <- m

	return nil
}

func (s *stateUpdater) handleBlockOver() {
	for tokenAddress := range s.currentBlockUpdatedTokens {
		tokenIdentificator := models.TokenIdentificator{
			ChainID: s.config.ChainID,
			Address: tokenAddress,
		}
		token, ok := s.tokensMapForCache[tokenIdentificator]

		if !ok {
			continue
		}

		delete(s.tokensMapForCache, tokenIdentificator)

		s.tokenCacheRepo.SetToken(token)
	}
}
