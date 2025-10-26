package merging

import (
	"fmt"
	"math/big"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/src/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/src/external/subgraphs/v2subgraphs"
	"github.com/alexkalak/go_market_analyze/src/models"
)

func (m *Merger) MergeV2PairsNotFilled(chainID uint) error {
	//All tokens from database
	tokens, err := m.TokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		fmt.Println("Error getting tokens from db")
		return err
	}
	fmt.Println("Len tokens: ", len(tokens))

	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		if _, ok := tokensMap[token.Address]; ok {
			continue
		}
		tokensMap[token.Address] = &token
	}

	alreadyAddedPairAddresses := map[string]any{}

	for _, token := range tokens {
		prioPreload := 20
		preloadPairs := 400

		// if token.Address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" {
		// 	prioPreload = 10
		// 	preloadPairs = 300
		// }
		// if token.Address == "0xdac17f958d2ee523a2206206994597c13d831ec7" {
		// 	prioPreload = 10
		// 	preloadPairs = 30
		// }

		v2Streamer, err := subgraphs.NewV2Streamer(m.SubgraphClient, token.Address, chainID, prioPreload, preloadPairs)
		if err != nil {
			fmt.Println("Error getting v2 streamer for token: ", chainID, token.Symbol, token.Address)
			continue
		}

		for {
			v2Pairs, ok := v2Streamer.Next()
			if !ok {
				fmt.Println("Not ok")
				break
			}

			fmt.Println("Len pairs:", len(v2Pairs))

			m.addV2PairsToDB(v2Pairs, chainID, alreadyAddedPairAddresses, tokensMap)
		}

		if err != nil {
			fmt.Println("Error getting v2 pairs for chain: ", err)
		}
	}
	return nil
}

func (m *Merger) addV2PairsToDB(pairs []v2subgraphs.ExchangeV2, chainID uint, alreadyAddedPairAddresses map[string]any, tokensMap map[string]*models.Token) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	chunkSize := 1000
	for chunk := 0; chunk < len(pairs); chunk += chunkSize {
		slice := pairs[chunk:]

		if chunk+chunkSize < len(pairs) {
			slice = pairs[chunk:chunkSize]
		}

		query := psql.
			Insert(models.UNISWAP_V2_PAIR_TABLE).
			Columns(
				models.UNISWAP_V2_PAIR_ADDRESS,
				models.UNISWAP_V2_PAIR_EXCHANGE_NAME,
				models.UNISWAP_V2_PAIR_CHAINID,
				models.UNISWAP_V2_PAIR_TOKEN0,
				models.UNISWAP_V2_PAIR_TOKEN1,
				models.UNISWAP_V2_PAIR_AMOUNT0,
				models.UNISWAP_V2_PAIR_AMOUNT1,
				models.UNISWAP_V2_PAIR_FEE_TIER,
				models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
				models.UNISWAP_V2_PAIR_IS_DUSTY,
			)

		notFoundTokens := 0
		for _, pair := range slice {
			if _, ok := alreadyAddedPairAddresses[pair.Address]; ok {
				fmt.Println("Pair already added")
				continue
			}

			// check if both of pair's tokens exist in database, else skip
			_, ok1 := tokensMap[pair.Token0]
			_, ok2 := tokensMap[pair.Token1]

			if !ok1 || !ok2 {
				notFoundTokens++
				continue
			}

			fmt.Println("Addin pair: ", pair)
			query = query.Values(
				pair.Address,
				pair.ExchangeName,
				chainID,
				pair.Token0,
				pair.Token1,
				defaultAmount0,
				defaultAmount1,
				pair.FeeTier,
				0,
				defaultIsDusty,
			)

			alreadyAddedPairAddresses[pair.Address] = new(any)
		}
		fmt.Println("Not found tokens: ", notFoundTokens)

		res, err := query.RunWith(db).Exec()
		if err != nil {
			fmt.Println("Error running merge v2 query: ", err)
			continue
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			fmt.Println("error getting rows affected of v2 merging: ", err)
			continue
		}

		fmt.Println("Rows affecting while v2 merging: ", rowsAffected)
	}

	return nil
}

// Temp
func (m *Merger) MergeExchangeNamesForV2Pairs(chainID uint) error {
	//All tokens from database
	tokens, err := m.TokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		fmt.Println("Error getting tokens from db")
		return err
	}
	fmt.Println("Len tokens: ", len(tokens))

	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		if _, ok := tokensMap[token.Address]; ok {
			continue
		}
		tokensMap[token.Address] = &token
	}

	alreadyAddedPairAddresses := map[string]any{}

	for _, token := range tokens {
		prioPreload := 2
		preloadPairs := 2

		if token.Address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" {
			prioPreload = 10
			preloadPairs = 300
		}
		if token.Address == "0xdac17f958d2ee523a2206206994597c13d831ec7" {
			prioPreload = 10
			preloadPairs = 30
		}

		v2Streamer, err := subgraphs.NewV2Streamer(m.SubgraphClient, token.Address, chainID, prioPreload, preloadPairs)
		if err != nil {
			fmt.Println("Error getting v2 streamer for token: ", chainID, token.Symbol, token.Address)
			continue
		}

		for {
			v2Pairs, ok := v2Streamer.Next()
			if !ok {
				fmt.Println("Not ok")
				break
			}

			fmt.Println("Len pairs:", len(v2Pairs))

			m.updatePairsExchangeName(v2Pairs, chainID, alreadyAddedPairAddresses, tokensMap)
		}

		if err != nil {
			fmt.Println("Error getting v2 pairs for chain: ", err)
		}
	}
	return nil
}

func (m *Merger) updatePairsExchangeName(pairs []v2subgraphs.ExchangeV2, chainID uint, alreadyAddedPairAddresses map[string]any, tokensMap map[string]*models.Token) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	for _, pair := range pairs {
		if _, ok := alreadyAddedPairAddresses[pair.Address]; ok {
			fmt.Println("Pair already added")
			continue
		}
		_, ok1 := tokensMap[pair.Token0]
		_, ok2 := tokensMap[pair.Token1]

		if !ok1 || !ok2 {
			continue
		}

		query := psql.
			Update(models.UNISWAP_V2_PAIR_TABLE).
			Set(models.UNISWAP_V2_PAIR_EXCHANGE_NAME, pair.ExchangeName).
			Where(sq.Eq{models.UNISWAP_V2_PAIR_ADDRESS: pair.Address, models.UNISWAP_V2_PAIR_CHAINID: chainID})

		alreadyAddedPairAddresses[pair.Address] = new(any)

		_, err := query.RunWith(db).Exec()
		if err != nil {
			fmt.Println("Error running merge v2 query: ", err)
			continue
		}
	}

	return nil
}

func (m *Merger) MergeV2PairsDataAndUpdateUSDPriceForTokens(chainID uint, blockNumber *big.Int) error {
	pairsFromDB, err := m.V2PairsRepo.GetPairsByChainID(chainID)
	if err != nil {
		panic(err)
	}

	pairs, err := m.getAmountsForV2Pairs(pairsFromDB, chainID, blockNumber)
	for i := range pairsFromDB {
		pairs[i].IsDusty = true
		pairs[i].BlockNumber = int(blockNumber.Int64())
	}
	if err != nil {
		panic(err)
	}

	err = m.V2PairsRepo.UpdatePairsAmount0Amount1BlockNumber(pairs)
	if err != nil {
		panic(err)
	}

	return nil
}

func (m *Merger) getAmountsForV2Pairs(pairsFromDB []models.UniswapV2Pair, chainID uint, blockNumber *big.Int) ([]models.UniswapV2Pair, error) {
	chunkSize := 50
	resPairs := make([]models.UniswapV2Pair, 0, len(pairsFromDB))
	pairsMap := make(map[string]*models.UniswapV2Pair)

	for i := 0; i < len(pairsFromDB); i += chunkSize {
		fmt.Println("Chunk: ", i/chunkSize)
		pairsChunk := make([]models.UniswapV2Pair, 0, chunkSize)
		if i+chunkSize > len(pairsFromDB) {
			pairsChunk = append(pairsChunk, pairsFromDB[i:]...)
		} else {
			pairsChunk = append(pairsChunk, pairsFromDB[i:i+chunkSize]...)
		}

		pairAddresses := make([]string, 0, len(pairsChunk))
		for _, pair := range pairsChunk {
			pairAddresses = append(pairAddresses, pair.Address)
			pairsMap[pair.Address] = &pair
		}

		pairsData, err := m.RpcClient.GetV2ParisData(pairAddresses, chainID, blockNumber)
		if err != nil {
			fmt.Println("Sleeping for 3 second")
			time.Sleep(1000 * time.Millisecond)
			i -= chunkSize
			continue
		}

		for _, pairAmounts := range pairsData {
			pairFromDB, ok := pairsMap[pairAmounts.Address]
			if !ok {
				continue
			}

			pairFromDB.Amount0 = pairAmounts.Amount0
			pairFromDB.Amount1 = pairAmounts.Amount1

			pair := models.UniswapV2Pair{
				Address: pairFromDB.Address,
				ChainID: pairFromDB.ChainID,
				Token0:  pairFromDB.Token0,
				Token1:  pairFromDB.Token1,
				Amount0: pairAmounts.Amount0,
				Amount1: pairAmounts.Amount1,
			}

			resPairs = append(resPairs, pair)
		}
	}

	return resPairs, nil

}

func (m *Merger) ValidateV2PairsAndComputeAvergeUSDPrice(pairs []models.UniswapV2Pair, chainID uint) error {
	stableCoins, err := m.TokenRepo.GetTokensBySymbolsAndChainID(USD_STABLECOIN_SYMBOLS, chainID)
	if err != nil {
		return err
	}
	tokens, err := m.TokenRepo.GetTokens()
	if err != nil {
		return err
	}

	tokenPricesMap, notDustyPairs, err := v2pairexchangable.ValidateV2PairsAndGetAverageUSDPriceForTokens(pairs, stableCoins)
	if err != nil {
		return err
	}

	updatingTokens := make([]models.Token, 0, len(tokenPricesMap))
	for _, token := range tokens {
		if price, ok := tokenPricesMap[token.Address]; ok {
			token.DefiUSDPrice = price
			updatingTokens = append(updatingTokens, token)
		}
	}

	err = m.TokenRepo.UpdateTokens(updatingTokens)
	if err != nil {
		return err
	}

	for i, pair := range pairs {
		if _, ok := notDustyPairs[pair.Address]; ok {
			pairs[i].IsDusty = false
		} else {
			pairs[i].IsDusty = true
		}
	}

	err = m.V2PairsRepo.UpdatePairsIsDusty(pairs)
	if err != nil {
		return err
	}

	return nil
}
