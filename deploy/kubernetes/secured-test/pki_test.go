package securedtest

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

// TestServicePKIScriptProducesIdentitiesTheLoadersAccept runs prepare-service-pki.sh (OpenSSL 3, as in the pod
// image) and checks every identity with the production loaders and a real mTLS handshake.
func TestServicePKIScriptProducesIdentitiesTheLoadersAccept(t *testing.T) {
	version, err := exec.Command("openssl", "version").Output()
	if err != nil || !strings.HasPrefix(string(version), "OpenSSL 3") {
		t.Skip("needs OpenSSL 3 like the apache/kafka image")
	}
	root := t.TempDir()
	for _, directory := range []string{"pki-ca", "pki-console", "pki-gateway-a", "pki-gateway-b"} {
		if err := os.MkdirAll(filepath.Join(root, "run", directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("bash", "prepare-service-pki.sh")
	command.Env = append(os.Environ(), "PKI_ROOT="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("script failed: %v\n%s", err, output)
	}
	identity := func(directory, uri string) tls.Certificate {
		t.Helper()
		base := filepath.Join(root, "run", directory)
		certificate, _, err := servicetls.LoadServiceIdentity(filepath.Join(base, "tls.crt"), filepath.Join(base, "tls.key"), filepath.Join(base, "ca.crt"), uri)
		if err != nil {
			t.Fatalf("%s: %v", directory, err)
		}
		return certificate
	}
	console := identity("pki-console", servicetls.ConsoleURI)
	gatewayA := identity("pki-gateway-a", servicetls.GatewayURIPrefix+"kafka-a")
	identity("pki-gateway-b", servicetls.GatewayURIPrefix+"kafka-b")
	_, pool, err := servicetls.LoadServiceIdentity(filepath.Join(root, "run/pki-console/tls.crt"), filepath.Join(root, "run/pki-console/tls.key"),
		filepath.Join(root, "run/pki-ca/ca.crt"), servicetls.ConsoleURI)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster, _ := servicetls.GatewayCluster(r.TLS)
		_, _ = io.WriteString(w, cluster)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{console}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	defer server.Close()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{gatewayA},
		VerifyPeerCertificate: servicetls.RequirePeerURI(func(uri string) bool { return uri == servicetls.ConsoleURI })}}}
	response, err := client.Get(strings.Replace(server.URL, "127.0.0.1", "127.0.0.1", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if string(body) != "kafka-a" {
		t.Fatalf("handshake identified %q", body)
	}
	for _, directory := range []string{"pki-console", "pki-gateway-a", "pki-gateway-b"} {
		info, err := os.Stat(filepath.Join(root, "run", directory, "tls.key"))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s key mode = %v %v", directory, info.Mode().Perm(), err)
		}
	}
}
