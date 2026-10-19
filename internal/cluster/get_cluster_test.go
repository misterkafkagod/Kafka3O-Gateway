package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
)

type fakeDescribeCluster struct {
	cluster  Cluster
	err      error
	deadline time.Time
	hasDL    bool
}

func (f *fakeDescribeCluster) Execute(ctx context.Context) (Cluster, error) {
	f.deadline, f.hasDL = ctx.Deadline()
	return f.cluster, f.err
}

func TestGetCluster_SortsBrokersByID(t *testing.T) {
	fake := &fakeDescribeCluster{cluster: Cluster{
		ID:           "abc",
		ControllerID: 2,
		Brokers:      []Broker{{ID: 3, Host: "c"}, {ID: 1, Host: "a"}, {ID: 2, Host: "b"}},
	}}

	got, err := NewGetCluster(fake, time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	want := Cluster{
		ID:           "abc",
		ControllerID: 2,
		Brokers:      []Broker{{ID: 1, Host: "a"}, {ID: 2, Host: "b"}, {ID: 3, Host: "c"}},
	}
	if diff := gocmp.Diff(want, got); diff != "" {
		t.Errorf("cluster mismatch (-want +got):\n%s", diff)
	}
}

func TestGetCluster_PassesConfiguredDeadline(t *testing.T) {
	fake := &fakeDescribeCluster{}
	start := time.Now()

	if _, err := NewGetCluster(fake, 2*time.Second).Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	end := time.Now()

	if !fake.hasDL {
		t.Fatal("expected the port to receive a context with a deadline")
	}
	if fake.deadline.Before(start.Add(2*time.Second)) || fake.deadline.After(end.Add(2*time.Second)) {
		t.Errorf("deadline %v not within [start+2s, end+2s]", fake.deadline)
	}
}

func TestGetCluster_ReturnsPortError(t *testing.T) {
	boom := errors.New("boom")

	_, err := NewGetCluster(&fakeDescribeCluster{err: boom}, time.Second).Execute(context.Background())

	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want %v", err, boom)
	}
}
