package gatewayauth

import (
	"crypto/tls"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

// isConsolePeer reports whether the connection presented a verified Console service certificate.
func isConsolePeer(state *tls.ConnectionState) bool {
	return state != nil && len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0 &&
		len(state.VerifiedChains[0][0].URIs) == 1 && state.VerifiedChains[0][0].URIs[0].String() == servicetls.ConsoleURI
}
