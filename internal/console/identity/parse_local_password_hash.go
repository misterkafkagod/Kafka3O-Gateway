package identity

import (
	"encoding/base64"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

func parseLocalPasswordHash(encoded string) (passwordHash, error) {
	if len(encoded) > 1024 {
		return passwordHash{}, ErrInvalidPasswordHash
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v="+strconv.Itoa(argon2.Version) {
		return passwordHash{}, ErrInvalidPasswordHash
	}
	parameters := strings.Split(parts[3], ",")
	if len(parameters) != 3 {
		return passwordHash{}, ErrInvalidPasswordHash
	}
	values := make([]uint64, 3)
	for i, prefix := range []string{"m=", "t=", "p="} {
		if !strings.HasPrefix(parameters[i], prefix) {
			return passwordHash{}, ErrInvalidPasswordHash
		}
		raw := strings.TrimPrefix(parameters[i], prefix)
		value, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || strconv.FormatUint(value, 10) != raw {
			return passwordHash{}, ErrInvalidPasswordHash
		}
		values[i] = value
	}
	if values[0] < argonMemoryKiB || values[0] > 64*1024 || values[1] < argonIterations || values[1] > 4 || values[2] < 1 || values[2] > 4 {
		return passwordHash{}, ErrInvalidPasswordHash
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil || len(salt) < argonSaltBytes || len(salt) > 32 {
		return passwordHash{}, ErrInvalidPasswordHash
	}
	key, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil || len(key) != argonKeyBytes {
		return passwordHash{}, ErrInvalidPasswordHash
	}
	return passwordHash{memory: uint32(values[0]), iterations: uint32(values[1]), parallelism: uint8(values[2]), salt: salt, key: key}, nil
}
