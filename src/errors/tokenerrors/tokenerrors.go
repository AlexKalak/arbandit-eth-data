package tokenerrors

import "errors"

var ErrUnableToFetchTokens = errors.New("unable to fetch tokens")
var ErrUnableToParseTokenResp = errors.New("unable to parse token resp")
var ErrUnableToCreateTokenStorage = errors.New("unable to create token storage")
