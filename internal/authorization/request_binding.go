package authorization

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
)

// RequestBinding is delegation binding v1: it ties a delegation to the cluster, method, escaped path,
// sorted query and exact body bytes of one Gateway request (see the gateway-service-trust discovery).
func RequestBinding(clusterID, method, escapedPath string, query url.Values, body []byte) [32]byte {
	bodyDigest := sha256.Sum256(body)
	return sha256.Sum256([]byte(strings.Join([]string{
		"kafka3o-delegation-v1", clusterID, method, escapedPath, query.Encode(), hex.EncodeToString(bodyDigest[:]),
	}, "\n")))
}
