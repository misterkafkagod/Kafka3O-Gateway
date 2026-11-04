package groups

import "strconv"

func consumerOffsetString(offset *int64) *string {
	if offset == nil {
		return nil
	}
	return new(strconv.FormatInt(*offset, 10))
}
