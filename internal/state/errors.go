package state

import "errors"

// ErrState identifies failures in the durable migration state protocol.
var ErrState = errors.New("migration state error")
