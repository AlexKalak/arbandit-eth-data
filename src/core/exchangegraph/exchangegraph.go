package exchangegraph

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/alexkalak/go_market_analyze/src/core/exchangables"
	"github.com/alexkalak/go_market_analyze/src/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/src/errors/exchangegrapherrors"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/repo/tokenrepo"
)

type ExchangesGraph interface {
	FindAllArbs(maxDepth int, initAmount *big.Int)
	FindArbs(startTokenIndex int, maxDepth int, initAmount *big.Int) ([]int, bool)
	GetTokenByIndex(index int) (*models.Token, error)
	GetTokenIndexByIdentificator(identificator models.TokenIdentificator) (int, error)
	UpdateExchangable(exchangableIdentifier string, exchangable exchangables.Exchangable) error
}

type edge struct {
	From, To    int
	Exchangable exchangables.Exchangable
	Zfo         bool
}

type Arbitrage struct {
	hops      []int
	usedEdges []*edge
	amounts   []*big.Int
}

type exchangesGraph struct {
	mu                 sync.Mutex
	arbitrages         []Arbitrage
	tokenIDs           map[models.TokenIdentificator]int
	tokens             []*models.Token
	exchangableIndexes map[string]int
	exchangablesArray  []exchangables.Exchangable
	edgesGraph         map[int][]edge
	TokenRepo          tokenrepo.TokenRepo
}

type ExchangeGraphDependencies struct {
	TokenRepo tokenrepo.TokenRepo
}

// New important all the exchangable to be on the same chain
func New(arrayOfExchangables []exchangables.Exchangable, dependencies ExchangeGraphDependencies) (ExchangesGraph, error) {
	res := exchangesGraph{}

	if dependencies.TokenRepo == nil {
		return nil, errors.New("invalid exchange graph dependencies, provide TokenRepo")
	}
	res.TokenRepo = dependencies.TokenRepo

	fillExchangesGraphWithData(&res, arrayOfExchangables)

	return &res, nil
}

func fillExchangesGraphWithData(graph *exchangesGraph, arrayOfExchangables []exchangables.Exchangable) {
	tokenIDs := map[models.TokenIdentificator]int{}
	tokens := make([]*models.Token, 0)
	currentTokenIndex := 0
	edgesGraph := map[int][]edge{}
	exchangableIndexes := map[string]int{}

	for i, exchangable := range arrayOfExchangables {
		exchangableIndexes[exchangable.GetIdentifier()] = i
		token0Index := 0
		token1Index := 0

		if tokenIndex, ok := tokenIDs[exchangable.GetToken0().GetIdentificator()]; ok {
			token0Index = tokenIndex
		} else {
			tokens = append(tokens, exchangable.GetToken0())
			tokenIDs[exchangable.GetToken0().GetIdentificator()] = currentTokenIndex
			token0Index = currentTokenIndex
			currentTokenIndex++
		}

		if tokenIndex, ok := tokenIDs[exchangable.GetToken1().GetIdentificator()]; ok {
			token1Index = tokenIndex
		} else {
			tokens = append(tokens, exchangable.GetToken1())
			tokenIDs[exchangable.GetToken1().GetIdentificator()] = currentTokenIndex
			token1Index = currentTokenIndex
			currentTokenIndex++
		}

		edge01 := edge{
			From:        token0Index,
			To:          token1Index,
			Exchangable: exchangable,
			Zfo:         true,
		}
		edge10 := edge{
			From:        token1Index,
			To:          token0Index,
			Exchangable: exchangable,
			Zfo:         false,
		}

		edgesGraph[token0Index] = append(edgesGraph[token0Index], edge01)
		edgesGraph[token1Index] = append(edgesGraph[token1Index], edge10)
	}

	graph.tokenIDs = tokenIDs
	graph.tokens = tokens
	graph.edgesGraph = edgesGraph
	graph.exchangablesArray = arrayOfExchangables
	graph.exchangableIndexes = exchangableIndexes
}

type Path struct {
	tokenIndex int
	amount     *big.Int
	hops       []int
	usedEdges  []*edge
	amounts    []*big.Int
}

func (g *exchangesGraph) FindAllArbs(maxDepth int, initAmount *big.Int) {
	tall := time.Now()
	tokensLen := len(g.tokens)
	fmt.Println("len tokens: ", tokensLen)
	chunks := 16
	wg := sync.WaitGroup{}
	for chunk := range chunks {
		start := tokensLen * chunk / chunks
		end := tokensLen * (chunk + 1) / chunks
		wg.Add(1)
		go func(start, end int) {
			for i := start; i < end; i++ {
				g.FindArbs(i, maxDepth, initAmount)
			}
			wg.Done()
		}(start, end)
	}
	wg.Wait()

	uniqueArbs := []struct {
		len   int
		elems map[int]int
	}{}

	for _, arb := range g.arbitrages {
		// fmt.Println("uniq", uniqueArbs)
		currentArb := struct {
			len   int
			elems map[int]int
		}{
			len:   len(arb.hops),
			elems: map[int]int{},
		}

		for _, hop := range arb.hops {
			currentArb.elems[hop] = 1
		}
		// fmt.Println("curr", currentArb)

		found := false

	uniqueArbsLoop:
		for _, existingArb := range uniqueArbs {
			if existingArb.len != currentArb.len {
				continue
			}

			for hop, _ := range currentArb.elems {
				_, ok := existingArb.elems[hop]
				if !ok {
					continue uniqueArbsLoop
				}
			}

			found = true
			break
		}
		//
		if found {
			continue
		}

		uniqueArbs = append(uniqueArbs, currentArb)
		// fmt.Printf("Init amount %s\n", arb.amountInit.String())
		// fmt.Printf("End amount %s\n", arb.amountEnd.String())

		for i, hop := range arb.hops {
			token, err := g.TokenRepo.GetTokenByIdentificator(g.tokens[hop].GetIdentificator())
			if err != nil {
				fmt.Println("unable to print hops")
				break
			}

			if i == 0 {
				fmt.Printf(" -> %s - %s \n", arb.amounts[i], token.Symbol)
			} else {
				edge := arb.usedEdges[i-1]
				v3Exchangable, ok := edge.Exchangable.(*v3poolexchangable.ExchangableUniswapV3Pool)
				if ok {
					v3Exchangable.ImitateSwapWithLog(arb.amounts[i-1], edge.Zfo)
				}
				fmt.Printf(" -> %s %s - %s \n", arb.amounts[i], token.Symbol, edge.Exchangable.Address())
			}
		}
		fmt.Println("")

	}
	fmt.Println("Total time elapsed: ", time.Since(tall).Milliseconds(), "ms")
}

func (g *exchangesGraph) FindArbs(startTokenIndex int, maxDepth int, initAmount *big.Int) ([]int, bool) {
	token := g.tokens[startTokenIndex]

	tokenAmountForOneUSD := new(big.Float).Quo(big.NewFloat(1), token.DefiUSDPrice)
	tokenAmountNeeded := new(big.Float).Mul(tokenAmountForOneUSD, new(big.Float).SetInt(initAmount))

	amount := new(big.Float).Mul(new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(token.Decimals)), nil)), tokenAmountNeeded)
	amountInt, _ := amount.Int(nil)
	if amountInt == nil {
		return nil, false
	}

	stack := []Path{{
		tokenIndex: startTokenIndex,
		amount:     amountInt,
		hops:       []int{startTokenIndex},
		usedEdges:  []*edge{},
		amounts:    []*big.Int{amountInt},
	}}

	totalCount := 0
	for len(stack) > 0 {
		n := len(stack) - 1
		path := stack[n]
		stack = stack[:n]

		if len(path.hops) > maxDepth {
			continue
		}
		if path.amount.Cmp(big.NewInt(0)) == 0 {
			continue
		}

		for _, e := range g.edgesGraph[path.tokenIndex] {
			next := e.To
			if next == path.tokenIndex {
				continue
			}

			if next != startTokenIndex && slices.Contains(path.hops, next) {
				continue
			}

			newAmount, err := e.Exchangable.ImitateSwap(path.amount, e.Zfo)
			if err != nil {
				continue
			}
			if newAmount.Cmp(big.NewInt(0)) == 0 {
				continue
			}

			updatedHops := append([]int(nil), path.hops...)
			updatedHops = append(updatedHops, next)

			updatedAmounts := append([]*big.Int(nil), path.amounts...)
			updatedAmounts = append(updatedAmounts, newAmount)

			usedEdgesUpdated := append([]*edge(nil), path.usedEdges...)
			usedEdgesUpdated = append(usedEdgesUpdated, &e)

			if next == startTokenIndex {
				if new(big.Int).Sub(newAmount, new(big.Int).Div(newAmount, big.NewInt(200))).Cmp(amountInt) > 0 {
					g.mu.Lock()
					totalCount++

					g.arbitrages = append(g.arbitrages, Arbitrage{
						hops:      updatedHops,
						usedEdges: usedEdgesUpdated,
						amounts:   updatedAmounts,
					})

					g.mu.Unlock()
				}

				continue
			}

			stack = append(stack, Path{
				tokenIndex: next,
				amount:     newAmount,
				hops:       updatedHops,
				usedEdges:  usedEdgesUpdated,
				amounts:    updatedAmounts,
			})
		}
	}

	return nil, false
}

func (g *exchangesGraph) GetTokenByIndex(index int) (*models.Token, error) {
	if index < 0 || index >= len(g.tokens) {
		return nil, exchangegrapherrors.ErrInvalidTokenIndexGraph
	}
	return g.tokens[index], nil
}

func (g *exchangesGraph) GetTokenIndexByIdentificator(identificator models.TokenIdentificator) (int, error) {
	if index, ok := g.tokenIDs[identificator]; ok {
		return index, nil
	}
	return 0, exchangegrapherrors.ErrTokenNotFoundInGraph
}

func (g *exchangesGraph) UpdateExchangable(exchangableIdentifier string, exchangable exchangables.Exchangable) error {
	index, ok := g.exchangableIndexes[exchangableIdentifier]
	if !ok {
		return nil
	}

	g.exchangablesArray[index] = exchangable

	return nil
}
