package metrics

import (
	"cmp"
	"math/big"
)

// CompareGroupLags puts the largest known total lag first; groups without a known lag follow, then by name.
func CompareGroupLags(a, b GroupLag) int {
	switch {
	case a.Lag != nil && b.Lag == nil:
		return -1
	case a.Lag == nil && b.Lag != nil:
		return 1
	case a.Lag != nil && b.Lag != nil:
		x, _ := new(big.Int).SetString(*a.Lag, 10)
		y, _ := new(big.Int).SetString(*b.Lag, 10)
		if order := y.Cmp(x); order != 0 {
			return order
		}
	}
	return cmp.Compare(a.Group, b.Group)
}
