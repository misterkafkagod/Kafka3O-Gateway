package servicetls

import (
	"crypto/tls"
	"strings"
)

// GatewayCluster returns the cluster ID a verified Gateway client certificate is issued for.
func GatewayCluster(state *tls.ConnectionState) (string, bool) {
	if state == nil || len(state.VerifiedChains) == 0 {
		return "", false
	}
	uri, ok := peerURI(state.VerifiedChains)
	if !ok || !strings.HasPrefix(uri, GatewayURIPrefix) {
		return "", false
	}
	cluster := strings.TrimPrefix(uri, GatewayURIPrefix)
	if cluster == "" || len(cluster) > 128 || strings.ContainsAny(cluster, "/?#%") {
		return "", false
	}
	return cluster, true
}
