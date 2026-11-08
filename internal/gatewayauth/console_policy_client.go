package gatewayauth

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

type consolePolicyClient struct {
	url    string
	client *http.Client
}

// NewConsolePolicyClient calls the Console policy listener over mTLS with the Gateway's service identity and
// accepts only a server presenting the Console URI SAN. Redirects and retries are never followed.
func NewConsolePolicyClient(policyURL string, identity tls.Certificate, serviceCA *x509.CertPool) (PolicyClient, error) {
	if !strings.HasPrefix(policyURL, "https://") || serviceCA == nil || len(identity.Certificate) == 0 {
		return nil, ErrConfig
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: serviceCA, Certificates: []tls.Certificate{identity},
		VerifyPeerCertificate: servicetls.RequirePeerURI(func(uri string) bool { return uri == servicetls.ConsoleURI }),
	}, MaxIdleConnsPerHost: 16}
	return consolePolicyClient{url: strings.TrimSuffix(policyURL, "/") + "/internal/v1/delegations/consume", client: &http.Client{
		Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (c consolePolicyClient) Consume(ctx context.Context, handle string, binding [32]byte, action authorization.Action) (PolicyDecision, error) {
	payload, err := json.Marshal(map[string]string{
		"handle": handle, "binding": base64.RawURLEncoding.EncodeToString(binding[:]), "action": string(action),
	})
	if err != nil {
		return PolicyDecision{}, errors.Join(ErrPolicyUnavailable, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return PolicyDecision{}, errors.Join(ErrPolicyUnavailable, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return PolicyDecision{}, errors.Join(ErrPolicyUnavailable, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8192))
	if err != nil {
		return PolicyDecision{}, errors.Join(ErrPolicyUnavailable, err)
	}
	if response.StatusCode == http.StatusUnauthorized {
		return PolicyDecision{}, ErrInvalidDelegation
	}
	var decision struct {
		OperationID string `json:"operationId"`
		Allowed     *bool  `json:"allowed"`
		ActorID     string `json:"actorId"`
		Source      string `json:"source"`
		Denial      string `json:"denial"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &decision) != nil || decision.Allowed == nil {
		return PolicyDecision{}, ErrPolicyUnavailable
	}
	return PolicyDecision{Allowed: *decision.Allowed, ActorID: decision.ActorID, Source: decision.Source,
		Denial: decision.Denial, OperationID: decision.OperationID}, nil
}

// RecordOutcome reports the outcome of an allowed mutation once; any failure is returned, never retried.
func (c consolePolicyClient) RecordOutcome(ctx context.Context, operationID, outcome string) error {
	payload, err := json.Marshal(map[string]string{"operationId": operationID, "outcome": outcome})
	if err != nil {
		return errors.Join(ErrPolicyUnavailable, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.url, "/consume")+"/outcome", bytes.NewReader(payload))
	if err != nil {
		return errors.Join(ErrPolicyUnavailable, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return errors.Join(ErrPolicyUnavailable, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusNoContent {
		return ErrPolicyUnavailable
	}
	return nil
}
