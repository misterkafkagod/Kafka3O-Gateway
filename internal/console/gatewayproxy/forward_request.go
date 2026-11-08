package gatewayproxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
)

type ForwardRequest interface {
	Execute(ctx context.Context, request Request) (Response, error)
}

type forwardRequest struct {
	mint      delegation.MintDelegation
	identity  tls.Certificate
	serviceCA *x509.CertPool
	mu        sync.Mutex
	clients   map[string]*http.Client
}

func NewForwardRequest(mint delegation.MintDelegation, identity tls.Certificate, serviceCA *x509.CertPool) (ForwardRequest, error) {
	if mint == nil || len(identity.Certificate) == 0 || serviceCA == nil {
		return nil, ErrConfig
	}
	return &forwardRequest{mint: mint, identity: identity, serviceCA: serviceCA, clients: map[string]*http.Client{}}, nil
}

// Execute mints a delegation for the exact bytes it then sends once. A transport failure is ErrGatewayUnavailable
// for reads and ErrOutcomeUnknown for writes, which callers must never retry.
func (f *forwardRequest) Execute(ctx context.Context, request Request) (Response, error) {
	if !strings.HasPrefix(request.GatewayURL, "https://") || request.SessionID == "" || request.ClusterID == "" ||
		!strings.HasPrefix(request.EscapedPath, "/api/v1/") || request.Action == "" {
		return Response{}, ErrConfig
	}
	binding := authorization.RequestBinding(request.ClusterID, request.Method, request.EscapedPath, request.Query, request.Body)
	handle, err := f.mint.Execute(ctx, request.SessionID, request.ClusterID, request.Action, request.EscapedPath, binding, request.Reason)
	if err != nil {
		return Response{}, err
	}
	target := strings.TrimSuffix(request.GatewayURL, "/") + request.EscapedPath
	if encoded := request.Query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	outgoing, err := http.NewRequestWithContext(ctx, request.Method, target, bytes.NewReader(request.Body))
	if err != nil {
		return Response{}, err
	}
	outgoing.Header.Set(gatewayauth.DelegationHeader, handle)
	outgoing.Header.Set("Accept", "application/json")
	if len(request.Body) > 0 {
		outgoing.Header.Set("Content-Type", "application/json")
	}
	f.mu.Lock()
	client, ok := f.clients[request.ClusterID]
	if !ok {
		client = gatewayClient(request.ClusterID, f.identity, f.serviceCA)
		f.clients[request.ClusterID] = client
	}
	f.mu.Unlock()
	failure := ErrGatewayUnavailable
	if request.Method != http.MethodGet {
		failure = ErrOutcomeUnknown
	}
	response, err := client.Do(outgoing)
	if err != nil {
		return Response{}, errors.Join(failure, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil || len(body) > MaxResponseBytes {
		return Response{}, errors.Join(failure, errors.New("the Gateway response could not be read"))
	}
	return Response{Status: response.StatusCode, ContentType: response.Header.Get("Content-Type"), Body: body}, nil
}
