package groups

import (
	"context"
	"testing"
	"time"
)

type deadlineRecorder struct{ deadline time.Time }

func (d *deadlineRecorder) Execute(ctx context.Context, group, topic string, _ OffsetResetTarget) (OffsetResetPreview, error) {
	d.deadline, _ = ctx.Deadline()
	return OffsetResetPreview{Group: group, Topic: topic}, nil
}

func TestPreviewOffsetResetAppliesTheKafkaTimeout(t *testing.T) {
	recorder := &deadlineRecorder{}
	before := time.Now()
	preview, err := NewPreviewOffsetReset(recorder, 3*time.Second).Execute(context.Background(), "billing", "orders", OffsetResetTarget{Kind: "latest"})
	if err != nil || preview.Group != "billing" || preview.Topic != "orders" {
		t.Fatalf("preview = %+v, %v", preview, err)
	}
	if recorder.deadline.Before(before.Add(2*time.Second)) || recorder.deadline.After(before.Add(4*time.Second)) {
		t.Fatalf("deadline = %v", recorder.deadline.Sub(before))
	}
}
