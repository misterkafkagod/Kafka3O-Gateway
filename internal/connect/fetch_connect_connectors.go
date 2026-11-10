package connect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

// FetchConnectors reads connector and task status from Kafka Connect.
type FetchConnectors interface {
	Execute(ctx context.Context) ([]Connector, error)
}

type fetchConnectConnectors struct {
	baseURL string
	client  *http.Client
}

// NewFetchConnectConnectors reads from the Connect REST API at baseURL; an empty baseURL means Connect is not
// configured for this cluster.
func NewFetchConnectConnectors(baseURL string, client *http.Client) FetchConnectors {
	return fetchConnectConnectors{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

type connectStatus struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Connector struct {
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
		Trace    string `json:"trace"`
	} `json:"connector"`
	Tasks []struct {
		ID       int32  `json:"id"`
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
		Trace    string `json:"trace"`
	} `json:"tasks"`
}

// Execute uses only ?expand=status, never the configuration, as the kafka-connect-api discovery recipe requires.
func (f fetchConnectConnectors) Execute(ctx context.Context) ([]Connector, error) {
	if f.baseURL == "" {
		return nil, ErrConnectNotConfigured
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.baseURL+"/connectors?expand=status", nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectUnreachable, err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := f.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectUnreachable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrConnectUnreachable, response.StatusCode)
	}
	var listed map[string]struct {
		Status connectStatus `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&listed); err != nil {
		return nil, fmt.Errorf("%w: decode status: %w", ErrConnectUnreachable, err)
	}
	connectors := make([]Connector, 0, len(listed))
	for name, entry := range listed {
		status := entry.Status
		connector := Connector{Name: name, Type: status.Type, State: status.Connector.State, WorkerID: status.Connector.WorkerID,
			Error: SummarizeConnectTrace(status.Connector.Trace), Tasks: make([]Task, 0, len(status.Tasks))}
		for _, task := range status.Tasks {
			connector.Tasks = append(connector.Tasks, Task{ID: task.ID, State: task.State, WorkerID: task.WorkerID, Error: SummarizeConnectTrace(task.Trace)})
		}
		slices.SortFunc(connector.Tasks, CompareConnectTasks)
		connectors = append(connectors, connector)
	}
	slices.SortFunc(connectors, CompareConnectors)
	return connectors, nil
}
