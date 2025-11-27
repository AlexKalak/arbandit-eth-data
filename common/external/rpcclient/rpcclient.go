package rpcclient

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

//go:embed rpcclientassets/v3pooldataABI.json
var V3PoolDataABIStr string

//go:embed rpcclientassets/v2pairdataABI.json
var V2PairDataABIStr string

//go:embed rpcclientassets/multicallABI.json
var multicallABIStr string

type RpcClient interface {
	GetPoolsData(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error)
	GetPoolsTicks(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNUmber *big.Int) ([]models.UniswapV3Pool, error)
	GetPairsData(ctx context.Context, pairs []models.UniswapV2Pair, chainID uint, blockNumber *big.Int) ([]models.UniswapV2Pair, error)
}

type RpcClientConfig struct {
	WsURL   string
	HttpURL string
}

type rpcClient struct {
	config RpcClientConfig
	//chainID -> client
	client           *ethclient.Client
	multicallAddress common.Address

	v3PoolDataABI abi.ABI
	v2PairDataABI abi.ABI
	multicallABI  abi.ABI
}

func NewRpcClient(config RpcClientConfig) (RpcClient, error) {
	ethClient, err := ethclient.Dial(config.WsURL)
	if err != nil {
		return nil, err
	}

	v3PoolDataABI, err := abi.JSON(strings.NewReader(V3PoolDataABIStr))
	if err != nil {
		return nil, err
	}
	v2PairDataABI, err := abi.JSON(strings.NewReader(V2PairDataABIStr))
	if err != nil {
		return nil, err
	}
	multicallABI, err := abi.JSON(strings.NewReader(multicallABIStr))
	if err != nil {
		return nil, err
	}

	return &rpcClient{
		client:           ethClient,
		multicallAddress: common.HexToAddress("0xca11bde05977b3631167028862be2a173976ca11"),
		config:           config,
		v3PoolDataABI:    v3PoolDataABI,
		v2PairDataABI:    v2PairDataABI,
		multicallABI:     multicallABI,
	}, nil
}

type call struct {
	Target   common.Address
	CallData []byte
}

func (c *rpcClient) GetPoolsData(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error) {
	chunkSize := 10

	res := struct {
		mu    sync.Mutex
		pools []models.UniswapV3Pool
	}{}

	res.pools = make([]models.UniswapV3Pool, 0, len(pools))

	chunk := -chunkSize

	wg := sync.WaitGroup{}
upperLoop:
	for {
		for range 7 {
			chunk += chunkSize

			if chunk == len(pools) {
				break upperLoop
			}

			if chunk > len(pools) {
				chunk = len(pools) - chunkSize
			}

			currentChunk := chunk
			repeatedTimes := 0
			f := func() {}
			f = func() {
				fmt.Println("chunk: ", currentChunk/chunkSize)
				slice := pools[currentChunk:]
				if currentChunk+chunkSize < len(pools) {
					slice = pools[currentChunk : currentChunk+chunkSize]
				}
				calls := []call{}

				for _, pool := range slice {
					poolAddress := common.HexToAddress(pool.Address)
					data, err := c.v3PoolDataABI.Pack("liquidity")
					if err != nil {
						return
					}

					calls = append(calls, call{poolAddress, data})

					data, err = c.v3PoolDataABI.Pack("tickSpacing")
					if err != nil {
						return
					}
					calls = append(calls, call{poolAddress, data})

					data, err = c.v3PoolDataABI.Pack("slot0")
					if err != nil {
						return
					}
					calls = append(calls, call{poolAddress, data})
				}

				returnBytes, err := c.Multicall(ctx, calls, blockNumber, c.client, c.multicallAddress)
				if err != nil {
					fmt.Println("Error calling rpc multicall", err)
					if repeatedTimes < 2 {
						repeatedTimes++
						time.Sleep(1 * time.Second)
						f()
					}

					time.Sleep(1 * time.Second)
					return
				}

				updatedPools, err := c.handleGetPoolDataReturnBytes(slice, returnBytes)
				if err != nil {
					return
				}
				res.mu.Lock()
				res.pools = append(res.pools, updatedPools...)
				res.mu.Unlock()
			}

			wg.Go(f)

		}
		wg.Wait()
	}
	wg.Wait()

	for i := range res.pools {
		res.pools[i].BlockNumber = int(blockNumber.Int64())
	}

	fmt.Println("Len: ", len(res.pools))

	return res.pools, nil
}

func (c *rpcClient) handleGetPoolDataReturnBytes(pools []models.UniswapV3Pool, returnBytes [][]byte) ([]models.UniswapV3Pool, error) {
	poolPackLen := 3
	if len(pools)*poolPackLen != len(returnBytes) {
		return nil, errors.New("multicall data len is corrupted")
	}

	updatedPools := make([]models.UniswapV3Pool, 0, len(returnBytes)/poolPackLen)
	for i := poolPackLen; i <= len(returnBytes); i += poolPackLen {
		poolIndex := (i - poolPackLen) / poolPackLen

		liquidityData := returnBytes[i-poolPackLen]
		tickSpacingData := returnBytes[i-poolPackLen+1]
		slot0Data := returnBytes[i-poolPackLen+2]

		liquidityOut, err := c.v3PoolDataABI.Unpack("liquidity", liquidityData)
		if err != nil {
			fmt.Println("Error unpacking liquidity ", err)
			continue
		}
		liquidity, ok := liquidityOut[0].(*big.Int)

		if !ok {
			return nil, errors.New("error convert liquidity")
		}

		tickSpacingOut, err := c.v3PoolDataABI.Unpack("tickSpacing", tickSpacingData)
		if err != nil {
			fmt.Println("Error unpacking tickSpacing", err)
			return nil, err
		}
		tickSpacing, ok := tickSpacingOut[0].(*big.Int)
		if !ok {
			return nil, errors.New("error convert tickSpacing")
		}

		slot0Out, err := c.v3PoolDataABI.Unpack("slot0", slot0Data)
		if err != nil {
			fmt.Println("Error unpacking slot0", err)
			return nil, err
		}

		sqrtPriceX96, ok := slot0Out[0].(*big.Int)
		if !ok {
			return nil, errors.New("error convert sqrtPrice")
		}
		tick, ok := slot0Out[1].(*big.Int)
		if !ok {
			return nil, errors.New("error convert tick")
		}
		// unlocked, ok := slot0Out[6].(bool)
		// if !ok {
		// 	return nil, errors.New("error convert unlocked")
		// }

		pool := pools[poolIndex]
		pool.Liquidity = liquidity
		pool.TickSpacing = int(tickSpacing.Int64())
		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Tick = int(tick.Int64())

		updatedPools = append(updatedPools, pool)
	}

	return updatedPools, nil
}

func (c *rpcClient) GetPairsData(ctx context.Context, pairs []models.UniswapV2Pair, chainID uint, blockNumber *big.Int) ([]models.UniswapV2Pair, error) {
	chunkSize := 10

	res := struct {
		mu    sync.Mutex
		pairs []models.UniswapV2Pair
	}{}

	res.pairs = make([]models.UniswapV2Pair, 0, len(pairs))

	offset := -chunkSize

	wg := sync.WaitGroup{}
upperLoop:
	for {
		for range 7 {
			offset += chunkSize

			if offset == len(pairs) {
				break upperLoop
			}

			if offset > len(pairs) {
				offset = len(pairs) - chunkSize
			}

			currentOffset := offset
			repeatedTimes := 0
			f := func() {}
			f = func() {
				fmt.Println("chunk: ", currentOffset/chunkSize)

				slice := pairs[currentOffset:]
				if currentOffset+chunkSize < len(pairs) {
					slice = pairs[currentOffset : currentOffset+chunkSize]
				}

				calls := []call{}

				for _, pair := range slice {
					pairAddress := common.HexToAddress(pair.Address)
					data, err := c.v2PairDataABI.Pack("getReserves")
					if err != nil {
						return
					}

					calls = append(calls, call{pairAddress, data})
				}

				returnBytes, err := c.Multicall(ctx, calls, blockNumber, c.client, c.multicallAddress)
				if err != nil {
					fmt.Println("Error calling rpc multicall", err)
					if repeatedTimes < 2 {
						repeatedTimes++
						time.Sleep(1 * time.Second)
						f()
					}

					time.Sleep(1 * time.Second)
					return
				}

				updatedPairs, err := c.handleGetPairDataReturnBytes(slice, returnBytes)
				if err != nil {
					return
				}
				res.mu.Lock()
				res.pairs = append(res.pairs, updatedPairs...)
				res.mu.Unlock()
			}

			wg.Go(f)

		}
		wg.Wait()
	}
	wg.Wait()

	for i := range res.pairs {
		res.pairs[i].BlockNumber = int(blockNumber.Int64())
	}

	fmt.Println("Len: ", len(res.pairs))

	return res.pairs, nil
}

func (c *rpcClient) handleGetPairDataReturnBytes(pairs []models.UniswapV2Pair, returnBytes [][]byte) ([]models.UniswapV2Pair, error) {
	//for case if pairData will contain more than one call
	pairPackLen := 1

	if len(pairs)*pairPackLen != len(returnBytes) {
		return nil, errors.New("multicall data len is corrupted")
	}

	updatedPairs := make([]models.UniswapV2Pair, 0, len(returnBytes)/pairPackLen)
	for i := pairPackLen; i <= len(returnBytes); i += pairPackLen {
		pairIndex := (i - pairPackLen) / pairPackLen

		getReservesData := returnBytes[i-pairPackLen]
		reservesOut, err := c.v2PairDataABI.Unpack("getReserves", getReservesData)
		if err != nil {
			fmt.Println("Error unpacking getReserves", err)
			continue
		}

		amount0, ok := reservesOut[0].(*big.Int)
		if !ok {
			return nil, errors.New("error convert amount0")
		}
		amount1, ok := reservesOut[1].(*big.Int)
		if !ok {
			return nil, errors.New("error convert amount1")
		}

		pair := pairs[pairIndex]
		pair.Amount0 = amount0
		pair.Amount1 = amount1

		updatedPairs = append(updatedPairs, pair)
	}

	return updatedPairs, nil
}
