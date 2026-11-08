// Package gatewayauth enforces delegated Console authorization in a Gateway's secured mode.
package gatewayauth

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"

// Route is the authorization classification of one Gateway request.
type Route struct {
	Action    authorization.Action
	Anonymous bool
}
