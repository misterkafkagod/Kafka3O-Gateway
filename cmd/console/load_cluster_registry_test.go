package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestClusterRegistryFileParsing(t *testing.T) {
	directory := t.TempDir()
	write := func(content string) string {
		t.Helper()
		path := filepath.Join(directory, "clusters.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if clusters, configured, err := loadClusterRegistry(func(string) string { return "" }); err != nil || configured || clusters != nil {
		t.Fatalf("unset cluster file = %+v %t %v", clusters, configured, err)
	}
	path := write(`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"https://dev.example:8081"},{"id":"prod","name":"Prod","gatewayUrl":"https://prod.example/"}]}`)
	clusters, configured, err := loadClusterRegistry(func(string) string { return path })
	want := []identity.RegisteredCluster{{ID: "dev", Name: "Dev", GatewayURL: "https://dev.example:8081"}, {ID: "prod", Name: "Prod", GatewayURL: "https://prod.example/"}}
	if err != nil || !configured || len(clusters) != 2 || clusters[0] != want[0] || clusters[1] != want[1] {
		t.Fatalf("cluster file = %+v %t %v", clusters, configured, err)
	}
	for _, content := range []string{
		`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"http://dev.example"}]}`,
		`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"https://user@dev.example"}]}`,
		`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"https://dev.example/path"}]}`,
		`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"https://dev.example?x=1"}]}`,
		`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"https://dev.example","environment":"PRODUCTION"}]}`,
		`{"clusters":[{"id":"dev","name":"Dev","gatewayUrl":"https://a.example"},{"id":"dev","name":"Dev","gatewayUrl":"https://b.example"}]}`,
		`{"clusters":[{"id":"dev","name":"De\u0007v","gatewayUrl":"https://dev.example"}]}`,
		`{"clusters":[{"id":"bad id","name":"Dev","gatewayUrl":"https://dev.example"}]}`,
		`{"clusters":[]}{}`, `{}`, `[]`, `{"clusters":[` + strings.Repeat(" ", 1<<20) + `]}`,
	} {
		path := write(content)
		if _, _, err := loadClusterRegistry(func(string) string { return path }); !errors.Is(err, errInvalidServerConfig) {
			t.Fatalf("cluster file %.80q error = %v", content, err)
		}
	}
	if _, _, err := loadClusterRegistry(func(string) string { return "relative/clusters.json" }); !errors.Is(err, errInvalidServerConfig) {
		t.Fatalf("relative path error = %v", err)
	}
}
