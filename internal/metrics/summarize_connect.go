package metrics

import (
	"slices"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

// SummarizeConnect counts connectors and tasks by the state Connect reports and lists failed connectors and tasks.
func SummarizeConnect(connectors []connect.Connector) Connect {
	summary := Connect{Connectors: []StateCount{}, Tasks: []StateCount{}, Failures: []ConnectFailure{}}
	connectorStates, taskStates := map[string]int{}, map[string]int{}
	for _, connector := range connectors {
		connectorStates[connector.State]++
		if connector.State == "FAILED" {
			summary.Failures = append(summary.Failures, ConnectFailure{Connector: connector.Name, Error: connector.Error})
		}
		for _, task := range connector.Tasks {
			taskStates[task.State]++
			if task.State == "FAILED" {
				id := task.ID
				summary.Failures = append(summary.Failures, ConnectFailure{Connector: connector.Name, Task: &id, Error: task.Error})
			}
		}
	}
	for state, count := range connectorStates {
		summary.Connectors = append(summary.Connectors, StateCount{State: state, Count: count})
	}
	for state, count := range taskStates {
		summary.Tasks = append(summary.Tasks, StateCount{State: state, Count: count})
	}
	slices.SortFunc(summary.Connectors, CompareStateCounts)
	slices.SortFunc(summary.Tasks, CompareStateCounts)
	return summary
}
