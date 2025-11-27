package rpcclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/ethereum/go-ethereum/common"
)

func (c *rpcClient) GetPoolsTicks(ctx context.Context, pools []models.UniswapV3Pool, chainID uint, blockNumber *big.Int) ([]models.UniswapV3Pool, error) {
	chunkSize := 3

	res := struct {
		mu    sync.Mutex
		pools []models.UniswapV3Pool
	}{}

	res.pools = make([]models.UniswapV3Pool, 0, len(pools))

	chunk := -chunkSize

	wg := sync.WaitGroup{}
upperLoop:
	for {
		for range 20 {
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
					bitmapWords := getBitmapWords(&pool)

					for _, word := range bitmapWords {
						data, err := c.v3PoolDataABI.Pack("tickBitmap", int16(word))
						if err != nil {
							fmt.Println("Error pacing error: ", err)
							return
						}
						calls = append(calls, call{poolAddress, data})
					}

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

				updatedPools, err := c.handleGetPoolBitmapsReturnBytes(slice, returnBytes)
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

	// for i := range res.pools {
	// 	res.pools[i].BlockNumber = int(blockNumber.Int64())
	// }

	return res.pools, nil
}

func getBitmapWords(pool *models.UniswapV3Pool) []int {
	ticksCoveredByOneWord := pool.TickSpacing * 256
	word := pool.Tick / (ticksCoveredByOneWord)

	lowerBound := word - 20
	upperBound := word + 20

	res := make([]int, 0)
	for i := lowerBound; i < upperBound; i++ {
		res = append(res, i)
	}

	return res

}

func (c *rpcClient) handleGetPoolBitmapsReturnBytes(pools []models.UniswapV3Pool, returnBytes [][]byte) ([]models.UniswapV3Pool, error) {
	bitmapsForPool := 40

	if len(pools)*bitmapsForPool != len(returnBytes) {
		return nil, errors.New("multicall data len is corrupted")
	}

	updatedPools := make([]models.UniswapV3Pool, 0, len(returnBytes)/bitmapsForPool)

	for i := 0; i < len(returnBytes); i += bitmapsForPool {
		poolIndex := i / bitmapsForPool
		pool := pools[poolIndex]

		initializedTicks := make([]int, 0, 0)

		for wordIndex := -bitmapsForPool / 2; wordIndex < bitmapsForPool/2; wordIndex++ {
			bitmapOut, err := c.v3PoolDataABI.Unpack("tickBitmap", returnBytes[i+wordIndex+bitmapsForPool/2])
			if err != nil {
				fmt.Println("Error unpacking liquidity ", err)
				return nil, err
			}

			bitmap, ok := bitmapOut[0].(*big.Int)
			if !ok {
				return nil, errors.New("error convert liquidity")
			}

			for i := range 256 {
				isInitialized := bitmap.Bit(i) == 1

				if isInitialized {
					tickIndex := pool.Tick + 256*pool.TickSpacing*wordIndex + i
					initializedTicks = append(initializedTicks, tickIndex)
				}
			}

		}
		fmt.Println("Initalized ticks: ", initializedTicks)

		var lowerTick int64 = math.MinInt64
		var upperTick int64 = math.MaxInt64

		for _, tick := range initializedTicks {
			if int64(tick) > lowerTick && tick < pool.Tick {
				lowerTick = int64(tick)
			} else if upperTick > int64(tick) && tick > pool.Tick {
				upperTick = int64(tick)
			}
		}

		if lowerTick > math.MinInt64 && upperTick < math.MaxInt64 {
			pool.TickLower = int(lowerTick)
			pool.TickUpper = int(upperTick)

			//Fix: This is not working
			// nearTicksJSON, err := json.Marshal(initializedTicks)
			// if err != nil {
			// 	fmt.Println("Unable to marshal nearTicksJSON")
			// }
			// pool.= string(nearTicksJSON)
		}

		updatedPools = append(updatedPools, pool)
	}

	return updatedPools, nil
}
