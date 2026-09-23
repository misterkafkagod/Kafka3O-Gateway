//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_S1_SCRAMUsers(t *testing.T) {
	t.Parallel()
	user := "acc-" + runID + "-s1"

	createResp := doAcceptanceJSON(t, http.MethodPost, "/v1/scram-users", operatorKey, map[string]any{
		"name": user, "mechanism": "SCRAM-SHA-256", "password": "s3cret",
	})
	var createBody struct {
		Name      string `json:"name"`
		Mechanism string `json:"mechanism"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&createBody); err != nil {
		drainAndClose(createResp)
		t.Fatalf("decode create response: %v", err)
	}
	drainAndClose(createResp)
	if createResp.StatusCode != http.StatusCreated || createBody.Name != user {
		t.Fatalf("create status = %d, body = %+v, want 201 name %s", createResp.StatusCode, createBody, user)
	}

	listResp := doAcceptance(t, http.MethodGet, "/v1/scram-users", operatorKey)
	var listBody struct {
		Users []struct {
			Name       string `json:"name"`
			Mechanisms []struct {
				Iterations int `json:"iterations"`
			} `json:"mechanisms"`
		} `json:"users"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listBody); err != nil {
		drainAndClose(listResp)
		t.Fatalf("decode list response: %v", err)
	}
	drainAndClose(listResp)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200", listResp.StatusCode)
	}
	found := false
	for _, u := range listBody.Users {
		if u.Name == user {
			found = true
			if len(u.Mechanisms) != 1 || u.Mechanisms[0].Iterations != 4096 {
				t.Errorf("user %s mechanisms = %+v, want one entry with iterations 4096", user, u.Mechanisms)
			}
		}
	}
	if !found {
		t.Fatalf("list = %+v, want %s listed", listBody.Users, user)
	}

	deleteResp := doAcceptanceJSON(t, http.MethodDelete, "/v1/scram-users/"+user, operatorKey, map[string]any{"confirm": user})
	drainAndClose(deleteResp)
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", deleteResp.StatusCode)
	}
}

func TestAcceptance_S2_Quotas(t *testing.T) {
	t.Parallel()
	user := "acc-" + runID + "-s2"

	patchResp := doAcceptanceJSON(t, http.MethodPatch, "/v1/quotas", operatorKey, map[string]any{
		"confirm": "user:" + user,
		"entity":  map[string]any{"user": user},
		"set":     map[string]any{"producerByteRate": 1048576},
	})
	var patchBody struct {
		Entity struct {
			User string `json:"user"`
		} `json:"entity"`
		Quotas struct {
			ProducerByteRate float64 `json:"producerByteRate"`
		} `json:"quotas"`
	}
	if err := json.NewDecoder(patchResp.Body).Decode(&patchBody); err != nil {
		drainAndClose(patchResp)
		t.Fatalf("decode patch response: %v", err)
	}
	drainAndClose(patchResp)
	if patchResp.StatusCode != http.StatusOK || patchBody.Entity.User != user || patchBody.Quotas.ProducerByteRate != 1048576 {
		t.Fatalf("patch status = %d, body = %+v, want 200 user %s producerByteRate 1048576", patchResp.StatusCode, patchBody, user)
	}

	listResp := doAcceptance(t, http.MethodGet, "/v1/quotas?entityType=user", operatorKey)
	var listBody struct {
		Items []struct {
			Entity struct {
				User string `json:"user"`
			} `json:"entity"`
		} `json:"items"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listBody); err != nil {
		drainAndClose(listResp)
		t.Fatalf("decode list response: %v", err)
	}
	drainAndClose(listResp)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200", listResp.StatusCode)
	}
	found := false
	for _, item := range listBody.Items {
		if item.Entity.User == user {
			found = true
		}
	}
	if !found {
		t.Fatalf("list = %+v, want %s listed", listBody.Items, user)
	}

	removeResp := doAcceptanceJSON(t, http.MethodPatch, "/v1/quotas", operatorKey, map[string]any{
		"confirm": "user:" + user,
		"entity":  map[string]any{"user": user},
		"remove":  []string{"producerByteRate"},
	})
	drainAndClose(removeResp)
	if removeResp.StatusCode != http.StatusOK {
		t.Fatalf("remove status = %d, want 200", removeResp.StatusCode)
	}
}
