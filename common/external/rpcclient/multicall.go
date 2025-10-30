package rpcclient

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func (c *rpcClient) Multicall(
	ctx context.Context,
	calls []call,
	blockNumber *big.Int,
	client *ethclient.Client,
	multicallAddress common.Address,
) ([][]byte, error) {
	if client == nil {
		return nil, errors.New("client for multicall cannot be nil")
	}
	input, err := c.multicallABI.Pack("aggregate", calls)
	if err != nil {
		return nil, err
	}

	callMsg := ethereum.CallMsg{To: &multicallAddress, Data: input}

	res, err := client.CallContract(ctx, callMsg, blockNumber)
	if err != nil {
		return nil, err
	}

	var (
		proceededBlockNumber *big.Int
		returnData           [][]byte
	)

	err = c.multicallABI.UnpackIntoInterface(&[]interface{}{&proceededBlockNumber, &returnData}, "aggregate", res)
	if err != nil {
		return nil, err
	}

	return returnData, nil

}
