package cognito

import (
	"time"
)

func minTime(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
