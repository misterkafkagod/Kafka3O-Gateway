package fake

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/porttest"
)

func TestFake_PortContract(t *testing.T) {
	t.Parallel()
	porttest.Run(t, New())
}

func TestFake_ConsumerPortContract(t *testing.T) {
	t.Parallel()
	f := New()
	porttest.RunConsumer(t, f, f)
}

func TestFake_ProducerPortContract(t *testing.T) {
	t.Parallel()
	f := New()
	porttest.RunProducer(t, f, f)
}

func TestFake_FailNext_FiresOnce(t *testing.T) {
	t.Parallel()
	f := New()
	f.SeedBroker(1, "b1", 9092, "")
	f.FailNext("DescribeCluster", kafka.KindBroker)

	if _, err := f.DescribeCluster(context.Background()); !kafka.IsKind(err, kafka.KindBroker) {
		t.Fatalf("first call error = %v, want KindBroker", err)
	}

	info, err := f.DescribeCluster(context.Background())
	if err != nil {
		t.Fatalf("second call error = %v, want nil (FailNext must fire once)", err)
	}
	if len(info.Brokers) != 1 {
		t.Errorf("Brokers = %d, want 1", len(info.Brokers))
	}
}

func TestFake_FailAlways_Persists(t *testing.T) {
	t.Parallel()
	f := New()
	f.FailAlways("DescribeCluster", kafka.KindUnavailable)

	for i := range 3 {
		if _, err := f.DescribeCluster(context.Background()); !kafka.IsKind(err, kafka.KindUnavailable) {
			t.Fatalf("call %d error = %v, want KindUnavailable", i, err)
		}
	}
}

func TestFake_Latency_HonoursCtx(t *testing.T) {
	t.Parallel()
	f := New()
	f.Latency("DescribeCluster", 200*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := f.DescribeCluster(ctx)
	elapsed := time.Since(start)

	if !kafka.IsKind(err, kafka.KindTimeout) {
		t.Fatalf("error = %v, want KindTimeout", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("DescribeCluster took %s, want it to return at the ~20ms context deadline, not the full 200ms latency", elapsed)
	}
}

func TestFake_MutatingCallsExcludesReads(t *testing.T) {
	t.Parallel()
	f := New()
	// DescribeCluster is a read; MutatingCalls must exclude it. No mutating
	// port method exists yet (arrives with the first W command), so the
	// inclusion side of the filter is proven directly via record, which this
	// white-box test file may call.
	f.record("ReadOp", false, nil)
	f.record("WriteOp", true, nil)

	if got := len(f.Calls()); got != 2 {
		t.Fatalf("Calls() = %d entries, want 2", got)
	}
	mut := f.MutatingCalls()
	if len(mut) != 1 || mut[0].Method != "WriteOp" {
		t.Fatalf("MutatingCalls() = %+v, want exactly one entry named WriteOp", mut)
	}
}

func TestFake_Race(t *testing.T) {
	t.Parallel()
	f := New()
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(n int32) {
			defer wg.Done()
			f.SeedBroker(n, "b", 9092, "")
			f.SeedTopic(fmt.Sprintf("t%d", n), 1, kafka.Record{Value: []byte("v")})
			f.SeedGroup(fmt.Sprintf("g%d", n), GroupOffset{Topic: fmt.Sprintf("t%d", n), Partition: 0, Offset: 1})
			_, _ = f.DescribeCluster(context.Background())
			_ = f.Calls()
			_ = f.MutatingCalls()
		}(int32(i))
	}
	wg.Wait()

	if got := len(f.Calls()); got != 20 {
		t.Errorf("Calls() = %d entries, want 20", got)
	}
}

func TestFake_AssertCalled_And_AssertNoCommitsNoGroupJoin(t *testing.T) {
	t.Parallel()
	f := New()
	_, _ = f.DescribeCluster(context.Background())

	f.AssertCalled(t, "DescribeCluster")
	f.AssertNoCommits(t)
	f.AssertNoGroupJoin(t)
}

func TestFake_SeedTopic_AssignsSequentialOffsetsPerPartition(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := New(WithClock(func() time.Time { return fixed }))
	f.SeedTopic("t", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 1, Value: []byte("c")},
	)

	p0 := f.model.topics["t"].partitions[0].records
	p1 := f.model.topics["t"].partitions[1].records
	if len(p0) != 2 || p0[0].Offset != 0 || p0[1].Offset != 1 {
		t.Fatalf("partition 0 = %+v, want offsets 0, 1", p0)
	}
	if len(p1) != 1 || p1[0].Offset != 0 {
		t.Fatalf("partition 1 = %+v, want offset 0", p1)
	}
	if !p0[0].Timestamp.Equal(fixed) {
		t.Errorf("Timestamp = %v, want the injected clock value %v", p0[0].Timestamp, fixed)
	}
}
