package connect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// FetchConnectorConfig reads a Gateway-managed connector's configuration; it is used inside the Gateway only and
// never returned to the browser.
type FetchConnectorConfig interface {
	Execute(ctx context.Context, name string) (map[string]string, error)
}

type fetchConnectorConfig struct {
	baseURL string
	client  *http.Client
}

func NewFetchConnectorConfig(baseURL string, client *http.Client) FetchConnectorConfig {
	return fetchConnectorConfig{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

func (f fetchConnectorConfig) Execute(ctx context.Context, name string) (map[string]string, error) {
	if f.baseURL == "" {
		return nil, ErrConnectNotConfigured
	}
	if err := ValidateConnectorName(name); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.baseURL+"/connectors/"+url.PathEscape(name)+"/config", nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectUnreachable, err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := f.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectUnreachable, err)
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusNotFound:
		return nil, ErrConnectorNotFound
	case response.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("%w: status %d", ErrConnectUnreachable, response.StatusCode)
	}
	var config map[string]string
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&config); err != nil {
		return nil, fmt.Errorf("%w: decode config: %w", ErrConnectUnreachable, err)
	}
	return config, nil
}
