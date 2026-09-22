package message

import (
	"time"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// resolveIntBound applies def when v is zero, and reports BoundExceeded
// (naming the offending field) when v exceeds ceil (FUNC-SPEC §8.8).
func resolveIntBound(name string, v, def, ceil int) (int, error) {
	if v == 0 {
		return def, nil
	}
	if v > ceil {
		return 0, &core.PolicyError{Code: core.BoundExceeded, Message: name + " exceeds the configured ceiling"}
	}
	return v, nil
}

// resolveBytesBound is resolveIntBound for a byte-count bound.
func resolveBytesBound(name string, v, def, ceil int64) (int64, error) {
	if v == 0 {
		return def, nil
	}
	if v > ceil {
		return 0, &core.PolicyError{Code: core.BoundExceeded, Message: name + " exceeds the configured ceiling"}
	}
	return v, nil
}

// resolveMaxTime converts maxTimeMs (0 = apply def) into a time.Duration,
// reporting BoundExceeded when it exceeds ceil.
func resolveMaxTime(maxTimeMs int64, def, ceil time.Duration) (time.Duration, error) {
	if maxTimeMs == 0 {
		return def, nil
	}
	d := time.Duration(maxTimeMs) * time.Millisecond
	if d > ceil {
		return 0, &core.PolicyError{Code: core.BoundExceeded, Message: "maxTimeMs exceeds the configured ceiling"}
	}
	return d, nil
}
