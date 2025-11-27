package arbitrageservice

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/core/trades"
	"github.com/alexkalak/go_market_analyze/common/models"
)

func (s *arbitrageService) FindOldArbs() error {
	arbs := []oldArb{}

	tokensMap, tradesMap, exchangablesMap, err := s.getDataNeededForOldSwapsFinding()
	if err != nil {
		return err
	}

	i := 0
	for tradeHash, tradesArray := range tradesMap {
		i++
		fmt.Println("i: ", i)
		tradesMap := map[string]trades.Trade{}
		amountsOut := map[string]*big.Int{}

		for _, trade := range tradesArray {
			tradesMap[trade.GetUniqueID()] = trade
			amountOut := trade.GetAmount0()
			if trade.GetAmount1().Sign() < 0 {
				amountOut = trade.GetAmount1()
			}

			amountsOut[trade.GetUniqueID()] = amountOut
		}

		nodesMap := map[string]*TradesChainNode{}

		var randomTradeNode *TradesChainNode
		for _, trade := range tradesArray {
			amountIn := trade.GetAmount1()
			if trade.GetAmount0().Sign() > 0 {
				amountIn = trade.GetAmount0()
			}

			for outID, amountOut := range amountsOut {
				if amountIn.CmpAbs(amountOut) == 0 {
					if outID == trade.GetUniqueID() {
						continue
					}
					tradeNode, ok := nodesMap[outID]
					if !ok {
						tradeNode = &TradesChainNode{
							TradeID:       outID,
							NextTradeNode: nil,
							PrevTradeNode: nil,
						}
					}

					nextTradeNode, ok := nodesMap[trade.GetUniqueID()]
					if !ok {
						nextTradeNode = &TradesChainNode{
							TradeID:       trade.GetUniqueID(),
							NextTradeNode: nil,
						}
					}
					nextTradeNode.PrevTradeNode = tradeNode
					tradeNode.NextTradeNode = nextTradeNode
					nodesMap[outID] = tradeNode
					nodesMap[trade.GetUniqueID()] = nextTradeNode

					randomTradeNode = tradeNode
					break
				}
			}
		}

		arb, err := checkTradesChain(tradesMap, exchangablesMap, tradeHash, randomTradeNode)
		if err != nil {
			continue
		}

		arbs = append(arbs, arb)

	}

	slices.SortFunc(arbs, func(x oldArb, y oldArb) int {
		return x.t - y.t
	})

	for _, arb := range arbs {
		fmt.Println(time.Unix(int64(arb.t), 0))
		// fmt.Println(arb.pathString)
		for _, tokenAddress := range arb.tokens {
			token, ok := tokensMap[tokenAddress]
			if !ok {
				fmt.Println("======FAILED=====")
			}
			fmt.Printf("->%s\n", token.Symbol)
		}
		fmt.Println(arb.txHash)
		fmt.Println("=============")

	}

	return nil
}

func checkTradesChain(tradesMap map[string]trades.Trade, exchangablesMap map[string]exchangables.Exchangable, tradeHash string, randomTradeNode *TradesChainNode) (oldArb, error) {
	if randomTradeNode == nil || (randomTradeNode.PrevTradeNode == nil && randomTradeNode.NextTradeNode == nil) {
		return oldArb{}, errors.New("not enough trades in chain")
	}

	//move to left of trades chain
	firstTradeNode := randomTradeNode
	for firstTradeNode.PrevTradeNode != nil {
		firstTradeNode = firstTradeNode.PrevTradeNode
	}

	//count amount of trades in chain
	count := 1
	firstTradeNodeForCount := firstTradeNode
	for firstTradeNodeForCount.NextTradeNode != nil {
		firstTradeNodeForCount = firstTradeNodeForCount.NextTradeNode
		count++
	}

	//create arbitrage struct
	arb := oldArb{
		txHash:     tradeHash,
		pathString: "",
		tokens:     []string{},
		amountIn:   new(big.Int),
		amountOut:  new(big.Int),
	}
	if count < 2 {
		return oldArb{}, errors.New("not enough trades in chain")
	}

	i := 0
	var firstTradeTokenInAddress string
	var lastTradeTokenOutAddress string

	for firstTradeNode != nil {
		//Adding node to arbitrage struct

		trade, ok := tradesMap[firstTradeNode.TradeID]
		arb.t = int(trade.GetTimestamp())
		if !ok {
			return oldArb{}, errors.New("trade not found")
		}
		arb.pathString += fmt.Sprint(trade.GetExchangableAddress(), "->")

		exchangable, ok := exchangablesMap[trade.GetExchangableAddress()]
		if !ok {
			return oldArb{}, errors.New("exchangable not found")
		}

		zfo := trade.GetAmount0().Cmp(big.NewInt(0)) > 0

		tokenIn := ""
		tokenOut := ""
		if zfo {
			tokenIn = exchangable.GetToken0().Address
			tokenOut = exchangable.GetToken1().Address
			arb.amountIn.Abs(trade.GetAmount0())
			arb.amountOut.Abs(trade.GetAmount1())
		} else {
			tokenIn = exchangable.GetToken1().Address
			tokenOut = exchangable.GetToken0().Address
			arb.amountIn.Abs(trade.GetAmount1())
			arb.amountOut.Abs(trade.GetAmount0())
		}

		if i == 0 {
			firstTradeTokenInAddress = tokenIn
			arb.tokens = append(arb.tokens, tokenIn)
		}
		lastTradeTokenOutAddress = tokenOut

		arb.tokens = append(arb.tokens, tokenOut)

		i++
		firstTradeNode = firstTradeNode.NextTradeNode
	}

	if firstTradeTokenInAddress == "" || lastTradeTokenOutAddress == "" || firstTradeTokenInAddress != lastTradeTokenOutAddress {
		return oldArb{}, errors.New("not equeal in/out")

	}

	return arb, nil
}

func (s *arbitrageService) getDataNeededForOldSwapsFinding() (map[string]*models.Token, map[string][]trades.Trade, map[string]exchangables.Exchangable, error) {
	tokens, err := s.tokenDBRepo.GetTokensByChainID(s.chainID)
	if err != nil {
		return nil, nil, nil, err
	}

	tokensMap := map[string]*models.Token{}
	for _, token := range tokens {
		tokensMap[token.Address] = token
	}

	v3swaps, err := s.v3TransactionsDBRepo.GetV3SwapsByChainID(s.chainID)
	if err != nil {
		return nil, nil, nil, err
	}
	v2swaps, err := s.v2TransactionsDBRepo.GetV2SwapsByChainID(s.chainID)
	if err != nil {
		return nil, nil, nil, err
	}

	fmt.Println("Swapsv3: ", len(v3swaps))
	fmt.Println("Swapsv2: ", len(v2swaps))

	tradesMap := map[string][]trades.Trade{}
	for _, v3swap := range v3swaps {
		if arr, ok := tradesMap[v3swap.TxHash]; ok {
			trade := trades.NewTradeFromV3Swap(&v3swap)
			arr = append(arr, trade)
			tradesMap[trade.GetHash()] = arr
		} else {
			trade := trades.NewTradeFromV3Swap(&v3swap)
			tradesMap[trade.GetHash()] = []trades.Trade{
				trade,
			}
		}
	}

	for _, v2swap := range v2swaps {
		if arr, ok := tradesMap[v2swap.TxHash]; ok {
			trade := trades.NewTradeFromV2Swap(&v2swap)
			arr = append(arr, trade)
			tradesMap[trade.GetHash()] = arr
		} else {
			trade := trades.NewTradeFromV2Swap(&v2swap)
			tradesMap[trade.GetHash()] = []trades.Trade{
				trade,
			}
		}
	}

	pools, err := s.v3PoolDBRepo.GetPoolsByChainID(s.chainID)
	if err != nil {
		return nil, nil, nil, err
	}
	fmt.Println("Pools: ", len(pools))

	pairs, err := s.v2PairDBRepo.GetPairsByChainID(s.chainID)
	if err != nil {
		return nil, nil, nil, err
	}

	exchangablesMap := map[string]exchangables.Exchangable{}
	for _, pool := range pools {
		token0, ok0 := tokensMap[pool.Token0]
		token1, ok1 := tokensMap[pool.Token1]
		if !ok0 || !ok1 {
			continue
		}

		exchangable, err := v3poolexchangable.NewV3ExchangablePool(&pool, token0, token1)
		if err != nil {
			continue
		}

		exchangablesMap[pool.Address] = &exchangable
	}
	for _, pair := range pairs {
		token0, ok0 := tokensMap[pair.Token0]
		token1, ok1 := tokensMap[pair.Token1]
		if !ok0 || !ok1 {
			continue
		}

		exchangable, err := v2pairexchangable.NewV2ExchangablePair(&pair, token0, token1)
		if err != nil {
			continue
		}

		exchangablesMap[pair.Address] = &exchangable
	}

	return tokensMap, tradesMap, exchangablesMap, nil
}
