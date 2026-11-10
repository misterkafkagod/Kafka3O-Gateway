package connect

import (
	"context"
	"testing"
	"time"
)

type actionRecorder struct {
	deadline time.Time
	action   Action
}

func (a *actionRecorder) Execute(ctx context.Context, action Action) error {
	a.deadline, _ = ctx.Deadline()
	a.action = action
	return nil
}

func TestManageConnectorAppliesItsTimeout(t *testing.T) {
	recorder := &actionRecorder{}
	before := time.Now()
	if err := NewManageConnector(recorder, 3*time.Second).Execute(context.Background(), Action{Kind: ActionPause, Connector: "sink"}); err != nil {
		t.Fatal(err)
	}
	if recorder.action.Connector != "sink" || recorder.deadline.Before(before.Add(2*time.Second)) || recorder.deadline.After(before.Add(4*time.Second)) {
		t.Fatalf("recorded %+v, deadline %v", recorder.action, recorder.deadline.Sub(before))
	}
}
