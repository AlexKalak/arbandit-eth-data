package rpcerrors

import "errors"

var (
	ErrUnableToParseABI              = errors.New("unable to parse abi")
	ErrChainRPCClientNotFound        = errors.New("chain rpc client not found")
	ErrUnableToUnpackLiquidity       = errors.New("unable to unpack liquidity")
	ErrUnableToUnpackTickSpacing     = errors.New("unable to unpack tick spacing")
	ErrUnableToUnpackSlot0           = errors.New("unable to unpack slot0")
	ErrUnableToUnpackBalanceOfToken0 = errors.New("unable to unpack balance of token0")
	ErrUnableToUnpackBalanceOfToken1 = errors.New("unable to unpack balance of token1")
	ErrNoTopicsInLog                 = errors.New("no topics in log")
)
