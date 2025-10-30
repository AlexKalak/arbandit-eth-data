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

//go:embed rpcclientassets/multicallABI.json
var multicallABIStr string

type RpcClient interface {
	GetPoolsData(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error)
	GetPoolsTicks(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNUmber *big.Int) ([]models.UniswapV3Pool, error)
}

type RpcClientConfig struct {
	EthMainnetWs   string
	EthMainnetHttp string
}

type rpcClient struct {
	config RpcClientConfig
	//chainID -> client
	clients            map[uint]*ethclient.Client
	multicallAddresses map[uint]common.Address

	v3PoolDataABI abi.ABI
	multicallABI  abi.ABI
}

func NewRpcClient(config RpcClientConfig) (RpcClient, error) {
	ethClient, err := ethclient.Dial(config.EthMainnetWs)
	if err != nil {
		return nil, err
	}

	v3PoolDataABI, err := abi.JSON(strings.NewReader(V3PoolDataABIStr))
	if err != nil {
		return nil, err
	}
	multicallABI, err := abi.JSON(strings.NewReader(multicallABIStr))
	if err != nil {
		return nil, err
	}

	return &rpcClient{
		clients: map[uint]*ethclient.Client{
			1: ethClient,
		},
		multicallAddresses: map[uint]common.Address{
			1: common.HexToAddress("0xca11bde05977b3631167028862be2a173976ca11"),
		},
		config:        config,
		v3PoolDataABI: v3PoolDataABI,
		multicallABI:  multicallABI,
	}, nil
}

type call struct {
	Target   common.Address
	CallData []byte
}

func (c *rpcClient) GetPoolsData(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error) {
	client, ok := c.clients[chainID]
	if !ok {
		return nil, fmt.Errorf("client for chain %d not found", chainID)
	}

	multicallAddress, ok := c.multicallAddresses[chainID]
	if !ok {
		return nil, fmt.Errorf("multicall address for chain %d not found", chainID)
	}

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

				returnBytes, err := c.Multicall(ctx, calls, blockNumber, client, multicallAddress)
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
			return nil, err
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
