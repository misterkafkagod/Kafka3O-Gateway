package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

var clusterIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// SecuredMode is the validated secured-mode configuration with the Gateway's own service identity loaded.
type SecuredMode struct {
	ClusterID string
	PolicyURL string
	Identity  tls.Certificate
	ServiceCA *x509.CertPool
}

// LoadSecuredMode reports whether secured mode is selected and, if so, validates it. Any other mode than ""
// (legacy shared token) or "secured" is rejected.
func LoadSecuredMode(security SecurityConfig) (SecuredMode, bool, error) {
	switch security.Mode {
	case "":
		return SecuredMode{}, false, nil
	case "secured":
	default:
		return SecuredMode{}, false, errors.New("security.mode must be empty (shared token) or \"secured\"")
	}
	if !clusterIDPattern.MatchString(security.ClusterID) {
		return SecuredMode{}, false, errors.New("security.clusterId must be the registered cluster ID")
	}
	policy, err := url.Parse(security.PolicyURL)
	if err != nil || policy.Scheme != "https" || policy.Host == "" || policy.User != nil || policy.RawQuery != "" ||
		policy.Fragment != "" || (policy.Path != "" && policy.Path != "/") {
		return SecuredMode{}, false, errors.New("security.policyUrl must be the Console policy listener's HTTPS origin")
	}
	for _, path := range []string{security.ServiceCAFile, security.ClientCertificateFile, security.ClientKeyFile} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return SecuredMode{}, false, errors.New("security service CA and client certificate paths must be absolute")
		}
	}
	identity, pool, err := servicetls.LoadServiceIdentity(security.ClientCertificateFile, security.ClientKeyFile,
		security.ServiceCAFile, servicetls.GatewayURIPrefix+security.ClusterID)
	if err != nil {
		return SecuredMode{}, false, fmt.Errorf("load Gateway service identity: %w", err)
	}
	return SecuredMode{ClusterID: security.ClusterID, PolicyURL: "https://" + policy.Host, Identity: identity, ServiceCA: pool}, true, nil
}
