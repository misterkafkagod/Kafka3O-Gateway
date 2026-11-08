package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

type clusterRegistryFile struct {
	Clusters []struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		GatewayURL string `json:"gatewayUrl"`
	} `json:"clusters"`
}

// loadClusterRegistry reads the optional operator-owned cluster file named by KAFKA3O_CONSOLE_CLUSTERS_FILE.
func loadClusterRegistry(getenv func(string) string) ([]identity.RegisteredCluster, bool, error) {
	path := getenv("KAFKA3O_CONSOLE_CLUSTERS_FILE")
	if path == "" {
		return nil, false, nil
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, false, fmt.Errorf("%w: the cluster file path must be absolute", errInvalidServerConfig)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, fmt.Errorf("open cluster file: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return nil, false, fmt.Errorf("read cluster file: %w", err)
	}
	if len(data) > 1<<20 {
		return nil, false, fmt.Errorf("%w: the cluster file exceeds 1 MiB", errInvalidServerConfig)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var parsed clusterRegistryFile
	var extra json.RawMessage
	if err := decoder.Decode(&parsed); err != nil || decoder.Decode(&extra) != io.EOF || parsed.Clusters == nil {
		return nil, false, fmt.Errorf("%w: the cluster file must be one JSON object with a clusters list", errInvalidServerConfig)
	}
	clusters := make([]identity.RegisteredCluster, 0, len(parsed.Clusters))
	seen := make(map[string]struct{}, len(parsed.Clusters))
	for _, entry := range parsed.Clusters {
		gateway, err := url.Parse(entry.GatewayURL)
		_, duplicate := seen[entry.ID]
		if duplicate || identity.ValidateClusterID(entry.ID) != nil || entry.Name == "" || len(entry.Name) > 128 || !utf8.ValidString(entry.Name) ||
			strings.IndexFunc(entry.Name, unicode.IsControl) >= 0 ||
			err != nil || gateway.Scheme != "https" || gateway.Host == "" || gateway.User != nil ||
			gateway.RawQuery != "" || gateway.Fragment != "" || (gateway.Path != "" && gateway.Path != "/") ||
			gateway.String() != entry.GatewayURL || len(entry.GatewayURL) > 512 {
			return nil, false, fmt.Errorf("%w: invalid registered cluster entry", errInvalidServerConfig)
		}
		seen[entry.ID] = struct{}{}
		clusters = append(clusters, identity.RegisteredCluster{ID: entry.ID, Name: entry.Name, GatewayURL: entry.GatewayURL})
	}
	return clusters, true, nil
}
