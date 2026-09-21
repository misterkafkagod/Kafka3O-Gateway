package franz

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzConsumer_OptionsNoGroupReadCommitted(t *testing.T) {
	t.Parallel()

	t.Run("isolation level maps read_committed and read_uncommitted, defaulting to committed", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			level string
			want  kgo.IsolationLevel
		}{
			{"read_committed", kgo.ReadCommitted()},
			{"read_uncommitted", kgo.ReadUncommitted()},
			{"", kgo.ReadCommitted()},
			{"bogus", kgo.ReadCommitted()},
		}
		for _, tc := range cases {
			if got := isolationLevelOpt(tc.level); got != tc.want {
				t.Errorf("isolationLevelOpt(%q) = %v, want %v", tc.level, got, tc.want)
			}
		}
	})

	t.Run("NewConsumer builds a client with no ConsumerGroup option, without dialing a broker", func(t *testing.T) {
		t.Parallel()
		c, err := NewConsumer(Config{Bootstrap: []string{"127.0.0.1:0"}}, "read_committed")
		if err != nil {
			t.Fatalf("NewConsumer() error: %v", err)
		}
		defer c.Close()

		// franz-go refuses to combine ConsumePartitions/AddConsumePartitions
		// with a consumer group on the same client; successfully assigning
		// partitions here is itself proof no group option was ever set
		// (FUNC-SPEC O4; TECH-SPEC C4).
		if err := c.Assign(t.Context(), "t", []int32{0}, map[int32]int64{0: 0}); err != nil {
			t.Errorf("Assign() on a freshly built Consumer error: %v", err)
		}
	})
}
