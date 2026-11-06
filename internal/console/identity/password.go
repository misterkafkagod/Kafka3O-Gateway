package identity

import "errors"

const (
	passwordMinRunes = 12
	passwordMaxBytes = 1024
	argonMemoryKiB   = 19 * 1024
	argonIterations  = 2
	argonParallelism = 1
	argonSaltBytes   = 16
	argonKeyBytes    = 32
)

var (
	ErrInvalidPassword     = errors.New("password does not meet the local password policy")
	ErrInvalidPasswordHash = errors.New("invalid or unsupported local password hash")
	ErrRandomSource        = errors.New("secure random source is required")
)

type passwordHash struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
	salt        []byte
	key         []byte
}
