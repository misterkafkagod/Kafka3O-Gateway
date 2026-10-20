package httpapi

import (
	"errors"
	"net/http"
	"testing"
)

func TestNewClusterUnreachable_Builds503ProblemWithCode(t *testing.T) {
	p := NewClusterUnreachable(errors.New("dial tcp: connection refused"))

	if p.Status != http.StatusServiceUnavailable || p.GetStatus() != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", p.Status)
	}
	if p.Title != "Cluster unreachable" {
		t.Errorf("title = %q", p.Title)
	}
	if p.Detail != "Kafka is unreachable: dial tcp: connection refused" {
		t.Errorf("detail = %q", p.Detail)
	}
	if p.Code != "cluster_unreachable" {
		t.Errorf("code = %q", p.Code)
	}
}
