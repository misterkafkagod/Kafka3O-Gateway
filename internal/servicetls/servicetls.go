// Package servicetls holds the mutual-TLS service identities shared by the Console and secured Gateways:
// a dedicated service CA and exactly one URI SAN per certificate (see the gateway-service-trust discovery).
package servicetls

import "errors"

// ConsoleURI identifies the Console both as a client of Gateways and as the policy server.
const ConsoleURI = "spiffe://kafka3o/console"

// GatewayURIPrefix is followed by the Gateway's registered cluster ID.
const GatewayURIPrefix = "spiffe://kafka3o/gateway/"

var ErrInvalidServiceTLS = errors.New("invalid service TLS configuration")
