package exchangegrapherrors

import "errors"

var ErrInvalidTokenIndexGraph = errors.New("invalid token index in graph")
var ErrTokenNotFoundInGraph = errors.New("token not found in graph")
