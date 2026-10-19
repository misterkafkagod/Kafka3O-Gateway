package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"
)

func TestRegisterLive_Returns200WithStatusOK(t *testing.T) {
	_, api := humatest.New(t)
	RegisterLive(api)

	resp := api.Get("/api/v1/health/live")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	if !strings.Contains(resp.Body.String(), `"status":"ok"`) {
		t.Errorf("body = %s, want status ok", resp.Body.String())
	}
}
