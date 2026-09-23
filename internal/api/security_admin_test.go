package api_test

import (
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_S1_Create201NoPasswordEcho(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Do(t, http.MethodPost, "/v1/scram-users", map[string]any{
		"name": "alice", "mechanism": "SCRAM-SHA-256", "password": "s3cret",
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %v", resp.StatusCode, body)
	}
	if body["name"] != "alice" || body["mechanism"] != "SCRAM-SHA-256" {
		t.Errorf("body = %v, want name alice, mechanism SCRAM-SHA-256", body)
	}
	if _, ok := body["password"]; ok {
		t.Errorf("body = %v, must not echo the password", body)
	}

	listResp := gw.Get(t, "/v1/scram-users")
	listBody := decodeBody(t, listResp)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200: %v", listResp.StatusCode, listBody)
	}
	users, _ := listBody["users"].([]any)
	if len(users) != 1 {
		t.Fatalf("users = %v, want one entry", users)
	}
	user, _ := users[0].(map[string]any)
	if user["name"] != "alice" {
		t.Errorf("users[0] = %v, want name alice", user)
	}
	mechanisms, _ := user["mechanisms"].([]any)
	if len(mechanisms) != 1 {
		t.Fatalf("mechanisms = %v, want one entry", mechanisms)
	}
	mech, _ := mechanisms[0].(map[string]any)
	if mech["iterations"] != float64(4096) {
		t.Errorf("mechanisms[0].iterations = %v, want 4096", mech["iterations"])
	}
}

func TestAPI_S1_Create_ExistingIs409AlreadyExists(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	params := map[string]any{"name": "dup", "mechanism": "SCRAM-SHA-256", "password": "pw"}

	first := gw.Do(t, http.MethodPost, "/v1/scram-users", params)
	_ = decodeBody(t, first)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201", first.StatusCode)
	}

	second := gw.Do(t, http.MethodPost, "/v1/scram-users", params)
	body := decodeBody(t, second)
	if second.StatusCode != http.StatusConflict {
		t.Fatalf("second create status = %d, want 409: %v", second.StatusCode, body)
	}
}

func TestAPI_S1_DeleteWithBodyConfirm(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	createResp := gw.Do(t, http.MethodPost, "/v1/scram-users", map[string]any{
		"name": "bob", "mechanism": "SCRAM-SHA-256", "password": "pw",
	})
	_ = decodeBody(t, createResp)
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createResp.StatusCode)
	}

	resp := gw.Do(t, http.MethodDelete, "/v1/scram-users/bob", map[string]any{"confirm": "bob"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["deleted"] != "bob" {
		t.Errorf("body = %v, want deleted bob", body)
	}

	listResp := gw.Get(t, "/v1/scram-users")
	listBody := decodeBody(t, listResp)
	users, _ := listBody["users"].([]any)
	if len(users) != 0 {
		t.Errorf("users after delete = %v, want none", users)
	}
}

func TestAPI_S1_Delete_ConfirmationMismatch400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	createResp := gw.Do(t, http.MethodPost, "/v1/scram-users", map[string]any{
		"name": "carol", "mechanism": "SCRAM-SHA-256", "password": "pw",
	})
	_ = decodeBody(t, createResp)

	resp := gw.Do(t, http.MethodDelete, "/v1/scram-users/carol", map[string]any{"confirm": "wrong-name"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "CONFIRMATION_MISMATCH" {
		t.Errorf("code = %v, want CONFIRMATION_MISMATCH", errBody["code"])
	}
}

func TestAPI_S2_PatchAndList(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Do(t, http.MethodPatch, "/v1/quotas", map[string]any{
		"confirm": "user:alice",
		"entity":  map[string]any{"user": "alice"},
		"set":     map[string]any{"producerByteRate": 1048576},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch status = %d, want 200: %v", resp.StatusCode, body)
	}
	entity, _ := body["entity"].(map[string]any)
	if entity["user"] != "alice" {
		t.Errorf("entity = %v, want user alice", entity)
	}
	quotas, _ := body["quotas"].(map[string]any)
	if quotas["producerByteRate"] != float64(1048576) {
		t.Errorf("quotas = %v, want producerByteRate 1048576", quotas)
	}

	listResp := gw.Get(t, "/v1/quotas?entityType=user")
	listBody := decodeBody(t, listResp)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200: %v", listResp.StatusCode, listBody)
	}
	items, _ := listBody["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %v, want one entry", items)
	}
	item, _ := items[0].(map[string]any)
	itemEntity, _ := item["entity"].(map[string]any)
	if itemEntity["user"] != "alice" {
		t.Errorf("items[0].entity = %v, want user alice", itemEntity)
	}
}

func TestAPI_S2_AlterQuota_DryRunPlan(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Do(t, http.MethodPatch, "/v1/quotas?dryRun=true", map[string]any{
		"confirm": "user:dave",
		"entity":  map[string]any{"user": "dave"},
		"set":     map[string]any{"producerByteRate": 2097152},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["dryRun"] != true {
		t.Errorf("dryRun = %v, want true", body["dryRun"])
	}
	plan, ok := body["plan"].(map[string]any)
	if !ok {
		t.Fatalf("plan = %v, want a plan object", body["plan"])
	}
	changes, _ := plan["changes"].([]any)
	if len(changes) != 1 {
		t.Fatalf("plan.changes = %v, want one change", changes)
	}

	listResp := gw.Get(t, "/v1/quotas?entityType=user")
	listBody := decodeBody(t, listResp)
	items, _ := listBody["items"].([]any)
	if len(items) != 0 {
		t.Errorf("items after dry-run = %v, want none (nothing applied)", items)
	}
}
