package authorization

import (
	"net/url"
	"testing"
)

func TestRequestBindingCoversEveryRequestPart(t *testing.T) {
	base := RequestBinding("prod", "POST", "/api/v1/topics/orders/messages", url.Values{"a": {"1"}, "b": {"2"}}, []byte(`{"value":"v"}`))
	if base != RequestBinding("prod", "POST", "/api/v1/topics/orders/messages", url.Values{"b": {"2"}, "a": {"1"}}, []byte(`{"value":"v"}`)) {
		t.Fatal("query key order changed the binding")
	}
	if RequestBinding("prod", "DELETE", "/api/v1/topics/orders", nil, nil) != RequestBinding("prod", "DELETE", "/api/v1/topics/orders", url.Values{}, []byte{}) {
		t.Fatal("empty and missing query or body differ")
	}
	for name, other := range map[string][32]byte{
		"cluster": RequestBinding("dev", "POST", "/api/v1/topics/orders/messages", url.Values{"a": {"1"}, "b": {"2"}}, []byte(`{"value":"v"}`)),
		"method":  RequestBinding("prod", "PUT", "/api/v1/topics/orders/messages", url.Values{"a": {"1"}, "b": {"2"}}, []byte(`{"value":"v"}`)),
		"path":    RequestBinding("prod", "POST", "/api/v1/topics/payments/messages", url.Values{"a": {"1"}, "b": {"2"}}, []byte(`{"value":"v"}`)),
		"query":   RequestBinding("prod", "POST", "/api/v1/topics/orders/messages", url.Values{"a": {"1"}, "b": {"3"}}, []byte(`{"value":"v"}`)),
		"body":    RequestBinding("prod", "POST", "/api/v1/topics/orders/messages", url.Values{"a": {"1"}, "b": {"2"}}, []byte(`{"value": "v"}`)),
	} {
		if other == base {
			t.Fatalf("changing the %s kept the binding", name)
		}
	}
}
