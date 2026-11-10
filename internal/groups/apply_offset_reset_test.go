package groups

import (
	"context"
	"testing"
	"time"
)

type commitRecorder struct{ deadline time.Time }

func (c *commitRecorder) Execute(ctx context.Context, group, topic string, _ []OffsetResetRequest) (OffsetResetResult, error) {
	c.deadline, _ = ctx.Deadline()
	return OffsetResetResult{Group: group, Topic: topic}, nil
}

func TestApplyOffsetResetAppliesTheKafkaTimeout(t *testing.T) {
	recorder := &commitRecorder{}
	before := time.Now()
	result, err := NewApplyOffsetReset(recorder, 3*time.Second).Execute(context.Background(), "billing", "orders", nil)
	if err != nil || result.Group != "billing" || result.Topic != "orders" {
		t.Fatalf("result = %+v, %v", result, err)
	}
	if recorder.deadline.Before(before.Add(2*time.Second)) || recorder.deadline.After(before.Add(4*time.Second)) {
		t.Fatalf("deadline = %v", recorder.deadline.Sub(before))
	}
}
