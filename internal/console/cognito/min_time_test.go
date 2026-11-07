package cognito

import (
	"testing"
	"time"
)

func TestCognitoMinTime(t *testing.T) {
	earlier := time.Unix(100, 0)
	later := time.Unix(200, 0)
	for _, tt := range []struct {
		name          string
		first, second time.Time
		want          time.Time
	}{
		{"first earlier", earlier, later, earlier},
		{"second earlier", later, earlier, earlier},
		{"equal", earlier, earlier, earlier},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := minTime(tt.first, tt.second); !got.Equal(tt.want) {
				t.Fatalf("minTime = %v, want %v", got, tt.want)
			}
		})
	}
}
