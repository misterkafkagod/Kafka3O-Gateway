package settings

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGetConfigurationsChecksResourceAndPassesDeadline(t *testing.T) {
	describe := &fakeDescribeConfigurations{entries: []Configuration{{Name: "retention.ms"}}}
	action := NewGetConfigurations(describe, time.Second)
	got, err := action.Execute(context.Background(), Resource{Kind: Broker, Name: "0"})
	if err != nil || len(got) != 1 || !describe.hasLimit {
		t.Fatalf("got %#v, error %v, deadline %v", got, err, describe.hasLimit)
	}
	if _, err := action.Execute(context.Background(), Resource{Kind: Broker, Name: "00"}); !errors.Is(err, ErrInvalidResource) {
		t.Fatalf("non-canonical broker ID error = %v", err)
	}
	describe.err = errors.New("unavailable")
	if _, err := action.Execute(context.Background(), Resource{Kind: Topic, Name: "orders"}); !errors.Is(err, describe.err) {
		t.Fatalf("describe error = %v", err)
	}
}
