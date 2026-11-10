package connect

import "errors"

type Task struct {
	ID       int32  `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"workerId"`
	// Error is the first line of the task's failure trace, if any.
	Error string `json:"error,omitempty"`
}

// Connector is the status of one connector; its configuration is deliberately not part of it.
type Connector struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	State    string `json:"state"`
	WorkerID string `json:"workerId"`
	Error    string `json:"error,omitempty"`
	Tasks    []Task `json:"tasks"`
}

var (
	ErrConnectNotConfigured = errors.New("kafka connect is not configured for this cluster")
	ErrConnectUnreachable   = errors.New("kafka connect is unreachable")
)
