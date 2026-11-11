package metrics

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

func TestSummarizeConnect(t *testing.T) {
	summary := SummarizeConnect([]connect.Connector{
		{Name: "running", State: "RUNNING", Tasks: []connect.Task{{ID: 0, State: "RUNNING"}, {ID: 1, State: "RUNNING"}}},
		{Name: "failing", State: "RUNNING", Tasks: []connect.Task{{ID: 0, State: "FAILED", Error: "ConnectException: no such file"}}},
		{Name: "paused", State: "PAUSED", Tasks: []connect.Task{{ID: 0, State: "PAUSED"}}},
		{Name: "broken", State: "FAILED", Error: "ClassNotFoundException"},
	})
	if len(summary.Connectors) != 3 || summary.Connectors[0] != (StateCount{State: "FAILED", Count: 1}) || summary.Connectors[2] != (StateCount{State: "RUNNING", Count: 2}) {
		t.Fatalf("connectors = %+v", summary.Connectors)
	}
	if len(summary.Tasks) != 3 || summary.Tasks[0] != (StateCount{State: "FAILED", Count: 1}) || summary.Tasks[2] != (StateCount{State: "RUNNING", Count: 2}) {
		t.Fatalf("tasks = %+v", summary.Tasks)
	}
	if len(summary.Failures) != 2 || summary.Failures[0].Connector != "failing" || *summary.Failures[0].Task != 0 ||
		summary.Failures[1] != (ConnectFailure{Connector: "broken", Error: "ClassNotFoundException"}) {
		t.Fatalf("failures = %+v", summary.Failures)
	}
	if empty := SummarizeConnect(nil); empty.Connectors == nil || empty.Tasks == nil || empty.Failures == nil {
		t.Fatalf("empty = %+v", empty)
	}
}
