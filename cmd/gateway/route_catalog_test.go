package main

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
)

// TestRouteCatalogCoversEveryRegisteredOperation fails when a Gateway route is added without a policy action.
func TestRouteCatalogCoversEveryRegisteredOperation(t *testing.T) {
	api := humago.New(http.NewServeMux(), huma.DefaultConfig("Kafka3O Gateway", "test"))
	registerGatewayRoutes(api, gatewayActions{})
	parameter := regexp.MustCompile(`\{[^}]+\}`)
	operations := 0
	for path, item := range api.OpenAPI().Paths {
		sample := parameter.ReplaceAllString(path, "sample")
		for method, operation := range map[string]*huma.Operation{
			http.MethodGet: item.Get, http.MethodPost: item.Post, http.MethodPut: item.Put,
			http.MethodPatch: item.Patch, http.MethodDelete: item.Delete, http.MethodHead: item.Head,
		} {
			if operation == nil {
				continue
			}
			operations++
			route, ok := gatewayauth.ClassifyRoute(method, sample)
			if !ok {
				t.Errorf("%s %s has no policy action", method, path)
			}
			if route.Anonymous && path != "/api/v1/health/live" {
				t.Errorf("%s %s is anonymous", method, path)
			}
		}
	}
	if operations != 19 {
		t.Fatalf("checked %d operations; update this test when routes change", operations)
	}
}
