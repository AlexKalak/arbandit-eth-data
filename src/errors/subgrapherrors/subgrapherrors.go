package subgrapherrors

import "errors"

var ErrInvalidSubgraphClient = errors.New("invalid subgraph client")
var ErrChainIDNotFound = errors.New("chain id not found")
var ErrExchangeTypeNotFound = errors.New("exchange type not found")
var ErrExchangesForTokenNotFound = errors.New("exchanges not found for token")
var ErrInvalidExchangeType = errors.New("invalid exchange type")
var ErrCannotFindExchangeFeeTierByName = errors.New("cannot find exchange fee tier by name")
