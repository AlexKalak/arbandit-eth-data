package exchangegraph

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/alexkalak/go_market_analyze/common/core/coreerrors/exchangegrapherrors"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables"
	"github.com/alexkalak/go_market_analyze/common/models"
)

type ExchangesGraph interface {
	FindAllArbs(maxDepth int, initAmount *big.Int) ([]Arbitrage, error)
	FindArbs(startTokenIndex int, maxDepth int, initAmount *big.Int) ([]Arbitrage, error)
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
	ResultUSD *big.Float
	Hops      []int
	UsedEdges []*edge
	Amounts   []*big.Int
}

func (a *Arbitrage) CountRealAmounts() error {
	currentAmount := a.Amounts[0]
	realAmounts := []*big.Int{new(big.Int).Set(currentAmount)}
	var err error
	for _, edge := range a.UsedEdges {
		currentAmount, err = edge.Exchangable.ImitateSwap(currentAmount, edge.Zfo)
		if err != nil {
			return err
		}
		realAmounts = append(realAmounts, new(big.Int).Set(currentAmount))
	}

	a.Amounts = realAmounts

	return nil
}

type exchangesGraph struct {
	mu                 sync.Mutex
	arbitrages         []Arbitrage
	tokenIDs           map[models.TokenIdentificator]int
	tokens             []*models.Token
	exchangableIndexes map[string]int
	exchangablesArray  []exchangables.Exchangable
	edgesGraph         map[int][]edge
}

type ExchangeGraphDependencies struct {
}

// New important all the exchangable to be on the same chain
func New(arrayOfExchangables []exchangables.Exchangable, dependencies ExchangeGraphDependencies) (ExchangesGraph, error) {
	res := exchangesGraph{}

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

func (g *exchangesGraph) FindAllArbs(maxDepth int, initAmount *big.Int) ([]Arbitrage, error) {
	tall := time.Now()

	tokensLen := len(g.tokens)
	fmt.Println("len tokens: ", tokensLen)

	chunks := 16

	arbsSynced := struct {
		mu   sync.Mutex
		arbs []Arbitrage
	}{}

	wg := sync.WaitGroup{}
	for chunk := range chunks {
		start := tokensLen * chunk / chunks
		end := tokensLen * (chunk + 1) / chunks
		wg.Add(1)
		go func(start, end int) {
			for i := start; i < end; i++ {
				arbitrages, err := g.FindArbs(i, maxDepth, initAmount)
				if err != nil {
					continue
				}
				if len(arbitrages) == 0 {
					continue
				}

				arbsSynced.mu.Lock()
				arbsSynced.arbs = append(arbsSynced.arbs, arbitrages...)
				arbsSynced.mu.Unlock()
			}
			wg.Done()
		}(start, end)
	}
	wg.Wait()

	fmt.Println("Total time elapsed: ", time.Since(tall).Milliseconds(), "ms")
	return arbsSynced.arbs, nil
}

func (g *exchangesGraph) FindArbs(startTokenIndex int, maxDepth int, initAmount *big.Int) ([]Arbitrage, error) {
	arbitrages := []Arbitrage{}

	token := g.tokens[startTokenIndex]

	tokenAmountForOneUSD := new(big.Float).Quo(big.NewFloat(1), token.USDPrice)
	tokenAmountNeeded := new(big.Float).Mul(tokenAmountForOneUSD, new(big.Float).SetInt(initAmount))

	amount := new(big.Float).Mul(new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(token.Decimals)), nil)), tokenAmountNeeded)
	amountInt, _ := amount.Int(nil)
	if amountInt == nil {
		return nil, errors.New("Unable to convert usd amount to tokenAmount")
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

	edgeLoop:
		for _, e := range g.edgesGraph[path.tokenIndex] {
			next := e.To
			if next == path.tokenIndex {
				continue
			}

			if next != startTokenIndex && (slices.Contains(path.hops, next)) {
				continue
			}

			for _, usedEdge := range path.usedEdges {
				if usedEdge.Exchangable.Address() == e.Exchangable.Address() {
					continue edgeLoop
				}
			}

			newAmountF := new(big.Float).Mul(new(big.Float).SetInt(path.amount), e.Exchangable.GetRate(e.Zfo))
			if newAmountF.Cmp(big.NewFloat(0)) == 0 {
				continue
			}

			newAmount, _ := newAmountF.Int(nil)

			updatedHops := append([]int(nil), path.hops...)
			updatedHops = append(updatedHops, next)

			updatedAmounts := append([]*big.Int(nil), path.amounts...)
			updatedAmounts = append(updatedAmounts, newAmount)

			usedEdgesUpdated := append([]*edge(nil), path.usedEdges...)
			usedEdgesUpdated = append(usedEdgesUpdated, &e)

			if next == startTokenIndex {
				if new(big.Int).Sub(newAmount, new(big.Int).Div(newAmount, big.NewInt(100))).Cmp(amountInt) > 0 {
					totalCount++

					arb := Arbitrage{
						Hops:      updatedHops,
						UsedEdges: usedEdgesUpdated,
						Amounts:   updatedAmounts,
					}

					err := arb.CountRealAmounts()
					if err != nil {
						continue
					}

					am := arb.Amounts[len(arb.Amounts)-1]

					if new(big.Int).Sub(am, new(big.Int).Div(am, big.NewInt(100))).Cmp(amountInt) <= 0 {
						continue
					}

					tokenAmount := new(big.Int).Sub(am, amountInt)
					tokenAmountReal := new(big.Float)

					usdPriceOfTokenOut := new(big.Float)
					if e.Zfo {
						tokenAmountReal.Set(e.Exchangable.GetToken1().GetRealAmount(tokenAmount))
						usdPriceOfTokenOut = e.Exchangable.GetToken1().USDPrice
					} else {
						tokenAmountReal.Set(e.Exchangable.GetToken0().GetRealAmount(tokenAmount))
						usdPriceOfTokenOut = e.Exchangable.GetToken0().USDPrice
					}

					resultUSD := new(big.Float).Mul(tokenAmountReal, usdPriceOfTokenOut)

					if resultUSD.Cmp(big.NewFloat(0.15)) < 0 {
						continue
					}

					arb.ResultUSD = resultUSD
					arbitrages = append(arbitrages, arb)
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

	slices.SortFunc(arbitrages, func(a1, a2 Arbitrage) int {
		return -1 * new(big.Int).Sub(a1.Amounts[len(a1.Amounts)-1], a2.Amounts[len(a2.Amounts)-1]).Sign()
	})

	// fmt.Println("Len arbs: ", len(arbitrages))
	// if len(arbitrages) > 0 {
	// 	fmt.Println("best: ", arbitrages[0])
	// 	fmt.Println("worst: ", arbitrages[len(arbitrages)-1])
	// }

	usedExchangables := map[string]any{}
	uniqueArbs := []Arbitrage{}

arbLoop:
	for _, arb := range arbitrages {
		for _, edge := range arb.UsedEdges {
			if _, ok := usedExchangables[edge.Exchangable.Address()]; ok {
				continue arbLoop
			}
		}
		for _, edge := range arb.UsedEdges {
			usedExchangables[edge.Exchangable.Address()] = new(any)
		}
		uniqueArbs = append(uniqueArbs, arb)
	}

	return uniqueArbs, nil
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
