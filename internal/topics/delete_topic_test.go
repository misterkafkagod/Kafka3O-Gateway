package topics

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeDeleteKafkaTopic struct {
	calls int
	name  string
	hasDL bool
	err   error
}

func (f *fakeDeleteKafkaTopic) Execute(ctx context.Context, name string) error {
	f.calls++
	f.name = name
	_, f.hasDL = ctx.Deadline()
	return f.err
}

func TestDeleteTopic_ValidatesAndForwardsDeadline(t *testing.T) {
	adapter := &fakeDeleteKafkaTopic{}
	action := NewDeleteTopic(adapter, time.Second)
	if err := action.Execute(context.Background(), ".."); !errors.Is(err, ErrInvalidTopic) || adapter.calls != 0 {
		t.Fatalf("invalid error = %v, calls = %d", err, adapter.calls)
	}
	want := errors.New("broker unavailable")
	adapter.err = want
	if err := action.Execute(context.Background(), "orders"); !errors.Is(err, want) ||
		adapter.name != "orders" || !adapter.hasDL || adapter.calls != 1 {
		t.Fatalf("error = %v, adapter = %#v", err, adapter)
	}
}
