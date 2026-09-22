package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// newTestService builds a Service directly (package-internal: New alone
// exposes no way to fake the sleep C10 waits out, TECH-SPEC §4.9).
func newTestService(admin kafka.Admin, sleep func(time.Duration)) *Service {
	return &Service{admin: admin, now: time.Now, sleep: sleep, runner: core.Runner{}}
}

// TestClusterService_Throughput_TwoSnapshotsSecondsApart proves Throughput
// takes exactly two end-offset snapshots (FUNC-SPEC §8.7 C10), with
// messagesPerSecond derived from however many records were produced between
// them — sleep is faked so the test itself never waits.
func TestClusterService_Throughput_TwoSnapshotsSecondsApart(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Value: []byte("a")}, kafka.Record{Value: []byte("b")})

	// Produce three more records "during" the faked sleep, simulating
	// throughput between the two snapshots — sleep itself never blocks.
	var slept time.Duration
	svc := newTestService(f, func(d time.Duration) {
		slept = d
		_, _ = f.Produce(context.Background(), "t", []kafka.ProduceRequest{
			{Value: []byte("c")}, {Value: []byte("d")}, {Value: []byte("e")},
		})
	})

	items, seconds, err := svc.Throughput(context.Background(), "t", 5)
	if err != nil {
		t.Fatalf("Throughput() error: %v", err)
	}
	if seconds != 5 {
		t.Errorf("seconds = %d, want 5", seconds)
	}
	if slept != 5*time.Second {
		t.Errorf("slept = %v, want 5s (the sleep call itself never blocked)", slept)
	}
	if len(items) != 1 || items[0].Topic != "t" {
		t.Fatalf("items = %+v, want exactly one entry for t", items)
	}
	if items[0].MessagesPerSecond != 0.6 { // 3 records / 5 seconds
		t.Errorf("MessagesPerSecond = %v, want 0.6 (3 records over 5 seconds)", items[0].MessagesPerSecond)
	}
}

func TestClusterService_Throughput_SecondsAboveCeilingIsBoundExceeded(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1)
	svc := newTestService(f, func(time.Duration) {})

	_, _, err := svc.Throughput(context.Background(), "t", 999)
	if !core.IsCode(err, core.BoundExceeded) {
		t.Fatalf("Throughput(seconds above ceiling) error = %v, want *core.PolicyError{Code: BoundExceeded}", err)
	}
}
