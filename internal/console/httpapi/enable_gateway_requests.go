package httpapi

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/gatewayproxy"

// EnableGatewayRequests mounts same-origin forwarding to secured Gateways; it requires EnableGatewayForwarding.
func (a *SessionAPI) EnableGatewayRequests(forwarder gatewayproxy.ForwardRequest) error {
	if a == nil || forwarder == nil || a.activeClusters == nil || a.forwarder != nil {
		return ErrInvalidAPIConfig
	}
	a.forwarder = forwarder
	a.mux.HandleFunc("/api/clusters/{clusterID}/gateway/", a.forwardGatewayRequest)
	return nil
}
