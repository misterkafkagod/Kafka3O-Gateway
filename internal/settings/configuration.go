package settings

import "errors"

type ResourceKind string

const (
	Topic  ResourceKind = "topic"
	Broker ResourceKind = "broker"
)

type Resource struct {
	Kind ResourceKind
	Name string
}

type Configuration struct {
	Name          string  `json:"name"`
	Value         *string `json:"value"`
	Sensitive     bool    `json:"sensitive"`
	ReadOnly      bool    `json:"readOnly"`
	Source        string  `json:"source"`
	Type          string  `json:"type"`
	Documentation string  `json:"documentation,omitempty"`
}

type Change struct {
	Name      string  `json:"name"`
	Operation string  `json:"operation"`
	Value     *string `json:"value"`
}

var (
	ErrInvalidResource = errors.New("invalid configuration resource")
	ErrInvalidChange   = errors.New("invalid configuration change")
	ErrNotFound        = errors.New("configuration resource or setting not found")
	ErrNotEditable     = errors.New("configuration setting is sensitive or read-only")
	ErrRejected        = errors.New("configuration change rejected")
	ErrDenied          = errors.New("configuration access denied")
	ErrWriteUnknown    = errors.New("configuration change outcome unknown")
)
