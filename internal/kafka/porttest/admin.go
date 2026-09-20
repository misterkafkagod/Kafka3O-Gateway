package porttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// adminSeeder is the optional capability internal/kafka/fake exposes to seed
// brokers for the happy-path case. internal/kafka/franz has no equivalent —
// at Level 2 the real cluster already has whatever brokers it has — so that
// sub-test is skipped when port doesn't implement it.
type adminSeeder interface {
	SeedBroker(id int32, host string, port int32, rack string)
}

// unreachabler is the optional capability internal/kafka/fake exposes to
// force every call to fail as KindUnavailable.
type unreachabler interface {
	Unreachable(bool)
}

// RunAdmin exercises kafka.Admin (FUNC-SPEC §8.1, C1).
func RunAdmin(t *testing.T, port kafka.Admin) {
	t.Helper()

	t.Run("Admin_DescribeCluster_ReturnsSeededBrokers", func(t *testing.T) {
		seeder, ok := port.(adminSeeder)
		if !ok {
			t.Skip("port does not implement the seeding capability")
		}
		seeder.SeedBroker(1, "broker-1", 9092, "rack-a")
		seeder.SeedBroker(2, "broker-2", 9092, "")

		info, err := port.DescribeCluster(context.Background())
		if err != nil {
			t.Fatalf("DescribeCluster() error: %v", err)
		}
		found := map[int32]bool{}
		for _, b := range info.Brokers {
			found[b.ID] = true
		}
		if !found[1] || !found[2] {
			t.Errorf("DescribeCluster() brokers = %+v, missing seeded ids 1 and 2", info.Brokers)
		}
	})

	t.Run("Admin_Unreachable_KindUnavailable", func(t *testing.T) {
		u, ok := port.(unreachabler)
		if !ok {
			t.Skip("port does not implement the unreachable capability")
		}
		u.Unreachable(true)
		defer u.Unreachable(false)

		_, err := port.DescribeCluster(context.Background())
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindUnavailable {
			t.Fatalf("DescribeCluster() while unreachable = %v, want *kafka.Error{Kind: KindUnavailable}", err)
		}
	})

	t.Run("Any_DeadlineExceeded_KindTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()

		_, err := port.DescribeCluster(ctx)
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindTimeout {
			t.Fatalf("DescribeCluster() with an expired context = %v, want *kafka.Error{Kind: KindTimeout}", err)
		}
	})
}
