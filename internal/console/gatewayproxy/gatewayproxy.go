// Package gatewayproxy forwards an authenticated browser request from the Console to one registered secured
// Gateway, once, with a single-use delegation bound to the exact request (see the gateway-service-trust discovery).
package gatewayproxy

import (
	"errors"
	"net/url"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

// Timeout is the Console's budget for one Gateway request (policy 1 s + Kafka 3 s + audit 1 s inside it).
const Timeout = 6 * time.Second

// MaxResponseBytes bounds the Gateway response relayed to the browser.
const MaxResponseBytes = 8 << 20

type Request struct {
	SessionID   string
	ClusterID   string
	GatewayURL  string
	Method      string
	EscapedPath string
	Query       url.Values
	Body        []byte
	Action      authorization.Action
	Reason      string
}

type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

var (
	ErrConfig             = errors.New("Gateway forwarding dependencies are required")
	ErrGatewayUnavailable = errors.New("the Gateway could not be reached")
	ErrOutcomeUnknown     = errors.New("the Gateway may have received the request; its outcome is unknown")
)
