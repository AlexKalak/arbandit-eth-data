package poolupdaterservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v2transactionrepo"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

type swapV2EventData struct {
	Sender     common.Address `json:"sender"`
	To         common.Address `json:"to"`
	Amount0In  *big.Int       `json:"amount0_in"`
	Amount1In  *big.Int       `json:"amount1_in"`
	Amount0Out *big.Int       `json:"amount0_out"`
	Amount1Out *big.Int       `json:"amount1_out"`
}

type syncV2EventData struct {
	Reserve0 *big.Int `json:"reserve0"`
	Reserve1 *big.Int `json:"reserve1"`
}

type pairEventData[T swapV2EventData | syncV2EventData] struct {
	Data T `json:"data"`
}

type pairUpdaterDependencies struct {
	tokenDBRepo tokenrepo.TokenRepo

	V2PairDBRepo           v2pairsrepo.V2PairDBRepo
	V2PairCacheRepo        v2pairsrepo.V2PairCacheRepo
	V2TransactionDBRepo    v2transactionrepo.V2TransactionDBRepo
	V2TransactionCacheRepo v2transactionrepo.V2TransactionCacheRepo
}

type pairUpdater struct {
	updatedTokensSet  map[string]any
	tokensMapForCache map[models.TokenIdentificator]*models.Token
	tokensMapForDB    map[models.TokenIdentificator]*models.Token
	tokenDBRepo       tokenrepo.TokenRepo

	v2PairDBRepo           v2pairsrepo.V2PairDBRepo
	v2PairCacheRepo        v2pairsrepo.V2PairCacheRepo
	v2TransactionDBRepo    v2transactionrepo.V2TransactionDBRepo
	v2TransactionCacheRepo v2transactionrepo.V2TransactionCacheRepo

	chainID                 uint
	currentBlockPairChanges map[models.V2PairIdentificator]models.UniswapV2Pair
}

func newPairUpdater(chainID uint, tokensMapForCache, tokensMapForDB map[models.TokenIdentificator]*models.Token, updatedTokensSet map[string]any, dependencies pairUpdaterDependencies) (pairUpdater, error) {
	pairs, err := dependencies.V2PairDBRepo.GetPairsByChainID(chainID)
	if err != nil {
		return pairUpdater{}, err
	}

	var maxBlockNumber int64 = math.MinInt64

	for _, pair := range pairs {
		if int64(pair.BlockNumber) > maxBlockNumber {
			maxBlockNumber = int64(pair.BlockNumber)
		}
	}

	if maxBlockNumber == math.MinInt64 {
		return pairUpdater{}, errors.New("unable to define block_number for pools")
	}

	err = dependencies.V2PairCacheRepo.ClearPairs(chainID)
	if err != nil {
		return pairUpdater{}, err
	}

	err = dependencies.V2PairCacheRepo.SetBlockNumber(chainID, uint64(maxBlockNumber))
	if err != nil {
		return pairUpdater{}, err
	}

	if len(pairs) > 0 {
		err = dependencies.V2PairCacheRepo.SetPairs(chainID, pairs)
		if err != nil {
			return pairUpdater{}, err
		}

	}

	return pairUpdater{
		updatedTokensSet: updatedTokensSet,

		tokensMapForDB:    tokensMapForDB,
		tokensMapForCache: tokensMapForCache,
		tokenDBRepo:       dependencies.tokenDBRepo,

		v2PairDBRepo:           dependencies.V2PairDBRepo,
		v2PairCacheRepo:        dependencies.V2PairCacheRepo,
		v2TransactionDBRepo:    dependencies.V2TransactionDBRepo,
		v2TransactionCacheRepo: dependencies.V2TransactionCacheRepo,

		chainID:                 chainID,
		currentBlockPairChanges: map[models.V2PairIdentificator]models.UniswapV2Pair{},
	}, nil
}

func (s *pairUpdater) onBlockOver(m *kafka.Message, blockNumber uint64) error {
	metaData := eventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	pairs := make([]models.UniswapV2Pair, 0, len(s.currentBlockPairChanges))
	for _, pair := range s.currentBlockPairChanges {
		pair.BlockNumber = int(metaData.BlockNumber)
		pairs = append(pairs, pair)
	}

	for identificator := range s.currentBlockPairChanges {
		delete(s.currentBlockPairChanges, identificator)
	}

	err := s.v2PairCacheRepo.SetPairs(s.chainID, pairs)
	if err != nil {
		return err
	}
	err = s.v2PairCacheRepo.SetBlockNumber(s.chainID, blockNumber)
	if err != nil {
		return err
	}

	return nil
}

func (s *pairUpdater) handlePairEventMessageForCache(m *kafka.Message) error {
	if m == nil {
		return errors.New("nil message")
	}

	metaData := eventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}
	fmt.Println(metaData.Type)

	switch metaData.Type {
	case "SwapV2":
		s.handleSwapEventForCache(metaData, m)
	case "SyncV2":
		s.handleSyncEventForCache(metaData, m)
	default:
	}

	return nil
}

func (s *pairUpdater) handleSwapEventForCache(metaData eventMetaData, m *kafka.Message) error {
	fmt.Println("SwapV2 for cache", helpers.GetJSONString(metaData))
	data := pairEventData[swapV2EventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	pairIdentificator := models.V2PairIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pair := models.UniswapV2Pair{}

	if existingPair, ok := s.currentBlockPairChanges[pairIdentificator]; ok {
		pair = existingPair
	} else {
		var err error
		pair, err = s.v2PairCacheRepo.GetPairByIdentificator(pairIdentificator)
		if err != nil {
			return err
		}
	}

	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pair.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pair.Token1, ChainID: s.chainID}]

	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	amount0 := new(big.Int).Sub(data.Data.Amount0In, data.Data.Amount0Out)
	amount1 := new(big.Int).Sub(data.Data.Amount1In, data.Data.Amount1Out)

	v2Swap := models.V2Swap{
		TxHash:                metaData.TxHash,
		TxTimestamp:           metaData.TxTimestamp,
		PairAddress:           metaData.Address,
		ChainID:               s.chainID,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               amount0,
		Amount1:               amount1,
		ArchiveToken0USDPrice: token0.USDPrice,
		ArchiveToken1USDPrice: token1.USDPrice,
	}

	return s.v2TransactionCacheRepo.StreamSwap(v2Swap)

}

func (s *pairUpdater) handleSyncEventForCache(metaData eventMetaData, m *kafka.Message) error {
	fmt.Println("SyncV2 for cache", helpers.GetJSONString(metaData))
	data := pairEventData[syncV2EventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	pairIdentificator := models.V2PairIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pair := models.UniswapV2Pair{}

	if existingPair, ok := s.currentBlockPairChanges[pairIdentificator]; ok {
		pair = existingPair
	} else {
		var err error
		pair, err = s.v2PairCacheRepo.GetPairByIdentificator(pairIdentificator)
		if err != nil {
			return err
		}
	}

	token0, ok0 := s.tokensMapForCache[models.TokenIdentificator{Address: pair.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForCache[models.TokenIdentificator{Address: pair.Token1, ChainID: s.chainID}]
	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	err := s.updatePairForSyncEvent(&pair, data, token0, token1)
	if err != nil {
		return err
	}

	_, err = s.updateTokensImpactsForSync(&pair, token0, token1)
	if err != nil {
		return err
	}

	s.updatedTokensSet[token0.Address] = new(any)
	s.updatedTokensSet[token1.Address] = new(any)

	s.currentBlockPairChanges[pairIdentificator] = pair

	return nil
}

func (s *pairUpdater) updatePairForSyncEvent(pair *models.UniswapV2Pair, data pairEventData[syncV2EventData], token0 *models.Token, token1 *models.Token) error {
	oldZfo := new(big.Float)
	oldZfo.Set(pair.Zfo10USDRate)

	pair.Amount0 = data.Data.Reserve0
	pair.Amount1 = data.Data.Reserve1

	err := v2pairexchangable.UpdateRateFor10USD(pair, token0, token1)
	if err != nil {
		pair.Zfo10USDRate = big.NewFloat(0)
		pair.NonZfo10USDRate = big.NewFloat(0)
		pair.IsDusty = true
	}

	return nil
}

// //DATABASE HANDLING
func (s *pairUpdater) startPostgresUpdater(ctx context.Context, chanel <-chan *kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-chanel:
			err := s.handlePairEventMessageForPostgres(m)
			if err != nil {
				continue
			}

		}
	}

}

func (s *pairUpdater) handlePairEventMessageForPostgres(m *kafka.Message) error {
	fmt.Println("handling postgres message")
	if m == nil {
		return errors.New("nil message")
	}

	metaData := eventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	switch metaData.Type {
	case "SwapV2":
		return s.handleSwapEventDB(metaData, m)

	case "SyncV2":
		return s.handleSyncEventDB(metaData, m)

	default:
		return errors.New("message type not found")
	}
}

func (s *pairUpdater) handleSwapEventDB(metaData eventMetaData, m *kafka.Message) error {
	data := pairEventData[swapV2EventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	pairIdentificator := models.V2PairIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pair, err := s.v2PairDBRepo.GetPairByIdentificator(pairIdentificator)
	if err != nil {
		return err
	}

	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pair.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pair.Token1, ChainID: s.chainID}]

	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	amount0 := new(big.Int).Sub(data.Data.Amount0In, data.Data.Amount0Out)
	amount1 := new(big.Int).Sub(data.Data.Amount1In, data.Data.Amount1Out)

	v2Swap := models.V2Swap{
		TxHash:                metaData.TxHash,
		TxTimestamp:           metaData.TxTimestamp,
		PairAddress:           metaData.Address,
		ChainID:               s.chainID,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               amount0,
		Amount1:               amount1,
		ArchiveToken0USDPrice: token0.USDPrice,
		ArchiveToken1USDPrice: token1.USDPrice,
	}
	fmt.Println("New Swap Adding in db: ", helpers.GetJSONString(v2Swap))

	err = s.v2TransactionDBRepo.CreateV2Swap(&v2Swap)
	if err != nil {
		fmt.Println("error creating tx:", err)
		return err
	}

	return nil
}

func (s *pairUpdater) handleSyncEventDB(metaData eventMetaData, m *kafka.Message) error {
	data := pairEventData[syncV2EventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	pairIdentificator := models.V2PairIdentificator{
		Address: metaData.Address,
		ChainID: s.chainID,
	}

	pair, err := s.v2PairDBRepo.GetPairByIdentificator(pairIdentificator)
	if err != nil {
		return err
	}

	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pair.Token0, ChainID: s.chainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pair.Token1, ChainID: s.chainID}]

	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	err = s.updatePairForSyncEvent(&pair, data, token0, token1)
	if err != nil {
		return err
	}
	err = s.updateImpactsForDB(&pair, token0, token1)
	if err != nil {
		return err
	}

	err = s.v2PairDBRepo.UpdatePairColumns(pair, []string{
		models.UNISWAP_V2_PAIR_AMOUNT1,
		models.UNISWAP_V2_PAIR_AMOUNT0,
		models.UNISWAP_V2_NON_ZFO_10USD_RATE,
		models.UNISWAP_V2_ZFO_10USD_RATE,
		models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
		models.UNISWAP_V2_PAIR_IS_DUSTY,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *pairUpdater) updateImpactsForDB(pair *models.UniswapV2Pair, token0, token1 *models.Token) error {
	updatedImpact, err := s.updateTokensImpactsForSync(pair, token0, token1)
	if err != nil {
		return err
	}

	if updatedImpact != nil {
		if updatedImpact.TokenAddress == token0.Address {
			err = s.tokenDBRepo.UpdateTokenPriceImpactAndTokenPrice(token0, updatedImpact)
		} else {
			err = s.tokenDBRepo.UpdateTokenPriceImpactAndTokenPrice(token1, updatedImpact)
		}
		if err != nil {
			fmt.Println("Unable to write update token price impact and token price")
			return err
		} else {
			fmt.Println("Updated pool impact", helpers.GetJSONString(updatedImpact))
		}
	}

	return err
}

func (s *pairUpdater) updateTokensImpactsForSync(pair *models.UniswapV2Pair, token0, token1 *models.Token) (*models.TokenPriceImpact, error) {
	var token0CurrentPoolImpact *models.TokenPriceImpact = nil
	exchangeIdentifier := models.GetExchangeIdentifierForV3Pool(
		pair.ChainID,
		pair.Address,
	)
	for _, imp := range token0.GetImpacts() {
		if imp.ChainID == pair.ChainID &&
			imp.TokenAddress == pair.Token0 &&
			imp.ExchangeIdentifier == exchangeIdentifier {
			token0CurrentPoolImpact = imp
			break
		}
	}

	var token1CurrentPoolImpact *models.TokenPriceImpact = nil
	for _, imp := range token1.GetImpacts() {
		if imp.ChainID == pair.ChainID &&
			imp.TokenAddress == pair.Token1 &&
			imp.ExchangeIdentifier == exchangeIdentifier {
			token1CurrentPoolImpact = imp
			break
		}
	}

	var updatedImpact *models.TokenPriceImpact

	if token0CurrentPoolImpact != nil {
		tokenRate, err := v2pairexchangable.GetRateForPairReal(pair, pair.Token0, token1.Decimals, token0.Decimals)
		if err != nil {
			return nil, nil
		}

		token0USDPrice := new(big.Float).Mul(tokenRate, token1.USDPrice)
		if token0USDPrice.Cmp(big.NewFloat(0)) == 0 {
			return nil, nil
		}

		token0CurrentPoolImpact.USDPrice = token0USDPrice
		token0CurrentPoolImpact.Impact = pair.Amount0

		token0.USDPrice, err = token0.AveragePrice()
		if err != nil {
			return nil, err
		}
		updatedImpact = token0CurrentPoolImpact

	} else if token1CurrentPoolImpact != nil {
		tokenRate, err := v2pairexchangable.GetRateForPairReal(pair, pair.Token1, token0.Decimals, token1.Decimals)
		if err != nil {
			return nil, nil
		}

		token1USDPrice := new(big.Float).Mul(tokenRate, token0.USDPrice)
		if token1USDPrice.Cmp(big.NewFloat(0)) == 0 {
			return nil, nil
		}

		token1CurrentPoolImpact.USDPrice = token1USDPrice
		token1CurrentPoolImpact.Impact = pair.Amount1

		token1.USDPrice, err = token1.AveragePrice()
		if err != nil {
			return nil, err
		}

		updatedImpact = token1CurrentPoolImpact
	}

	return updatedImpact, nil
}
