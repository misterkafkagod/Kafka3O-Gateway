package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type fakeEndOffsetsAdmin struct {
	offsets kadm.ListedOffsets
	err     error
	asked   []string
}

func (f *fakeEndOffsetsAdmin) ListEndOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.asked = topics
	return f.offsets, f.err
}

func TestReadEndOffsets_SumsPartitionsBeyondTheSafeIntegerRange(t *testing.T) {
	admin := &fakeEndOffsetsAdmin{offsets: kadm.ListedOffsets{
		"orders":   {0: {Topic: "orders", Offset: 9007199254740993}, 1: {Topic: "orders", Partition: 1, Offset: 9007199254740993}},
		"audit":    {0: {Topic: "audit", Offset: 4}},
		"payments": {0: {Topic: "payments", Offset: 1}, 1: {Topic: "payments", Partition: 1, Err: errors.New("NOT_LEADER_OR_FOLLOWER")}},
	}}
	got, err := NewReadEndOffsets(admin).Execute(context.Background(), []string{"payments", "orders", "audit", "gone"})
	if err != nil {
		t.Fatal(err)
	}
	want := []metrics.TopicEndOffset{
		{Topic: "audit", EndOffset: "4"},
		{Topic: "gone", Error: "no end offsets were reported"},
		{Topic: "orders", EndOffset: "18014398509481986"},
		{Topic: "payments", Error: "partition 1: NOT_LEADER_OR_FOLLOWER"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestReadEndOffsets_ReportsAFailedRequestAndSkipsAnEmptyList(t *testing.T) {
	admin := &fakeEndOffsetsAdmin{err: errors.New("refused")}
	if _, err := NewReadEndOffsets(admin).Execute(context.Background(), []string{"orders"}); err == nil || err.Error() != "list end offsets: refused" {
		t.Fatalf("err = %v", err)
	}
	empty := &fakeEndOffsetsAdmin{}
	got, err := NewReadEndOffsets(empty).Execute(context.Background(), nil)
	if err != nil || got == nil || len(got) != 0 || empty.asked != nil {
		t.Fatalf("got %+v, err = %v, asked = %v", got, err, empty.asked)
	}
}
