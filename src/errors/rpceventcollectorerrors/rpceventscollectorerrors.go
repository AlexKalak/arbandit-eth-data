package rpceventcollectorerrors

import "errors"

var (
	ErrInvalidDependencies      = errors.New("invalid dependencies")
	ErrUnableToInitWsLogsClient = errors.New("unable to init ws logs client")
	ErrUnableToParseLog         = errors.New("unable to parse log")
	ErrAbiError                 = errors.New("abi error")
	ErrLogTypeNotFound = errors.New("log type not found")
	ErrUnableToInitKafka = errors.New("unable to init kafka")
)
