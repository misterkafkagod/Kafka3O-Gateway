package cluster

import "testing"

func TestCompareBrokersByID(t *testing.T) {
	cases := []struct {
		name string
		a, b Broker
		want int
	}{
		{"less", Broker{ID: 1}, Broker{ID: 2}, -1},
		{"equal", Broker{ID: 2}, Broker{ID: 2}, 0},
		{"greater", Broker{ID: 3}, Broker{ID: 2}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CompareBrokersByID(tc.a, tc.b); got != tc.want {
				t.Errorf("CompareBrokersByID = %d, want %d", got, tc.want)
			}
		})
	}
}
