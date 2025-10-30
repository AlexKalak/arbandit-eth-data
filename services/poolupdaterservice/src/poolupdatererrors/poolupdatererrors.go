package poolupdatererrors

import "errors"

var ErrInvalidPoolUpdaterServiceDependencies = errors.New("invalid pool updater service dependencies")
var ErrInvalidPoolUpdaterServiceConfig = errors.New("invalid pool updater service config")
