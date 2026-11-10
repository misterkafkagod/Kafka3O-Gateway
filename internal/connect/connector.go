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

// Action is a connector lifecycle request; Task is used only by ActionRestartTask.
type Action struct {
	Kind      string
	Connector string
	Task      int32
}

const (
	ActionPause       = "pause"
	ActionResume      = "resume"
	ActionRestart     = "restart"
	ActionRestartTask = "restart-task"
)

var (
	ErrConnectNotConfigured = errors.New("kafka connect is not configured for this cluster")
	ErrConnectUnreachable   = errors.New("kafka connect is unreachable")
	ErrConnectorNotFound    = errors.New("the connector or task does not exist")
	ErrConnectRejected      = errors.New("kafka connect rejected the action")
	ErrConnectActionUnknown = errors.New("the connect action may have been applied")
	ErrInvalidConnectorName = errors.New("the connector name is invalid")
	ErrInvalidConnectAction = errors.New("the connect action is invalid")
)
