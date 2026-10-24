package topics

import "testing"

func TestCompareTopicsByName(t *testing.T) {
	cases := []struct {
		name string
		a, b Topic
		want int
	}{
		{"less", Topic{Name: "audit"}, Topic{Name: "orders"}, -1},
		{"equal", Topic{Name: "orders"}, Topic{Name: "orders"}, 0},
		{"greater", Topic{Name: "orders"}, Topic{Name: "audit"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CompareTopicsByName(tc.a, tc.b); got != tc.want {
				t.Errorf("CompareTopicsByName = %d, want %d", got, tc.want)
			}
		})
	}
}
