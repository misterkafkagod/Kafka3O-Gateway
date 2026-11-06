package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

type fakeGetConfigurations struct {
	resource settings.Resource
	err      error
}

func (f *fakeGetConfigurations) Execute(_ context.Context, resource settings.Resource) ([]settings.Configuration, error) {
	f.resource = resource
	if f.err != nil {
		return nil, f.err
	}
	value := "delete"
	return []settings.Configuration{{Name: "cleanup.policy", Value: &value}}, nil
}

type fakeAlterConfiguration struct {
	resource settings.Resource
	change   settings.Change
	err      error
}

func (f *fakeAlterConfiguration) Execute(_ context.Context, resource settings.Resource, change settings.Change) error {
	f.resource = resource
	f.change = change
	return f.err
}

func TestConfigurationEndpointsRouteTopicAndBroker(t *testing.T) {
	_, api := humatest.New(t)
	get := &fakeGetConfigurations{}
	alter := &fakeAlterConfiguration{}
	RegisterGetTopicConfigurations(api, get)
	RegisterGetBrokerConfigurations(api, get)
	RegisterAlterTopicConfiguration(api, alter)
	RegisterAlterBrokerConfiguration(api, alter)

	response := api.Get("/api/v1/topics/orders/config")
	if response.Code != http.StatusOK || get.resource != (settings.Resource{Kind: settings.Topic, Name: "orders"}) ||
		!strings.Contains(response.Body.String(), `"name":"cleanup.policy"`) {
		t.Fatalf("topic GET: status %d, resource %#v, body %s", response.Code, get.resource, response.Body.String())
	}
	response = api.Get("/api/v1/brokers/3/config")
	if response.Code != http.StatusOK || get.resource != (settings.Resource{Kind: settings.Broker, Name: "3"}) {
		t.Fatalf("broker GET: status %d, resource %#v", response.Code, get.resource)
	}

	response = api.Post("/api/v1/topics/orders/config", map[string]any{"name": "cleanup.policy", "operation": "set", "value": "compact"})
	if response.Code != http.StatusOK || alter.resource != (settings.Resource{Kind: settings.Topic, Name: "orders"}) ||
		alter.change.Value == nil || *alter.change.Value != "compact" || !strings.Contains(response.Body.String(), `"status":"accepted"`) {
		t.Fatalf("topic POST: status %d, action %#v, body %s", response.Code, alter, response.Body.String())
	}
	response = api.Post("/api/v1/brokers/3/config", map[string]any{"name": "log.retention.ms", "operation": "reset", "value": nil})
	if response.Code != http.StatusOK || alter.resource != (settings.Resource{Kind: settings.Broker, Name: "3"}) ||
		alter.change.Operation != "reset" || alter.change.Value != nil {
		t.Fatalf("broker POST: status %d, action %#v, body %s", response.Code, alter, response.Body.String())
	}
}

func TestConfigurationEndpointsReportErrorsWithoutLeakingDetails(t *testing.T) {
	_, api := humatest.New(t)
	alter := &fakeAlterConfiguration{}
	RegisterAlterTopicConfiguration(api, alter)
	for _, test := range []struct {
		err    error
		status int
		code   string
	}{
		{settings.ErrInvalidChange, http.StatusBadRequest, "invalid_configuration"},
		{settings.ErrNotFound, http.StatusNotFound, "configuration_not_found"},
		{settings.ErrNotEditable, http.StatusConflict, "configuration_not_editable"},
		{settings.ErrDenied, http.StatusForbidden, "configuration_denied"},
		{settings.ErrRejected, http.StatusUnprocessableEntity, "configuration_rejected"},
		{settings.ErrWriteUnknown, http.StatusServiceUnavailable, "configuration_write_unknown"},
		{errors.New("private upstream detail"), http.StatusServiceUnavailable, "cluster_unreachable"},
	} {
		alter.err = test.err
		response := api.Post("/api/v1/topics/orders/config", map[string]any{"name": "cleanup.policy", "operation": "set", "value": "compact"})
		if response.Code != test.status || !strings.Contains(response.Body.String(), test.code) ||
			strings.Contains(response.Body.String(), "private upstream detail") {
			t.Errorf("error %v: status %d, body %s", test.err, response.Code, response.Body.String())
		}
	}
}
