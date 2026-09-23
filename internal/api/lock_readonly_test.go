package api_test

import (
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/api"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

// routeFixture is one command's request shape, for tests that must exercise
// every currently-implemented route of some kind generically (TECH-SPEC
// §4.5 O9) rather than hardcoding just the ones that exist today.
type routeFixture struct {
	method string
	path   string
	body   any
}

// wRoutes covers every W command currently wired to a route (api.Pending()'s
// complement, restricted to Access W). wRoutesComplete below fails loudly
// the day a future phase adds a W route without an entry here. T5/T6 create
// a fresh topic rather than acting on the given one, so their names are
// derived from it to stay unique per call.
func wRoutes(topic string) map[string]routeFixture {
	return map[string]routeFixture{
		"M5": {http.MethodPost, "/v1/topics/" + topic + "/messages", map[string]any{"value": "v"}},
		"M6": {http.MethodPost, "/v1/topics/" + topic + "/messages/bulk", []map[string]any{{"value": "v"}}},
		"M7": {http.MethodPost, "/v1/topics/" + topic + "/tombstones", map[string]any{"key": "k"}},
		"T5": {http.MethodPost, "/v1/topics", map[string]any{"name": topic + "-t5", "partitions": 1, "replicationFactor": 1}},
		"T6": {http.MethodPost, "/v1/batch/topics", map[string]any{
			"topics": []map[string]any{{"name": topic + "-t6", "partitions": 1, "replicationFactor": 1}},
		}},
		"T9":  {http.MethodPatch, "/v1/topics/" + topic + "/config", map[string]any{"confirm": topic, "set": map[string]any{"retention.ms": "60000"}}},
		"T10": {http.MethodPost, "/v1/topics/" + topic + "/partitions", map[string]any{"confirm": topic, "partitions": 4}},
		"T7":  {http.MethodDelete, "/v1/topics/" + topic, map[string]any{"confirm": topic}},
		"T8":  {http.MethodPost, "/v1/batch/topics/delete", map[string]any{"confirm": "irrelevant", "topics": []string{topic + "-t8-missing"}}},
		"T11": {http.MethodPost, "/v1/topics/" + topic + "/delete-records", map[string]any{"confirm": topic, "offsets": map[string]any{"0": 0}}},
		"T12": {http.MethodPost, "/v1/topics/" + topic + "/purge", map[string]any{"confirm": topic}},
		"G4": {http.MethodPost, "/v1/consumer-groups/" + topic + "-g/reset-offsets", map[string]any{
			"confirm": topic + "-g", "target": map[string]any{"mode": "earliest"}, "topics": []string{topic},
		}},
		"G5": {http.MethodDelete, "/v1/consumer-groups/" + topic + "-g", map[string]any{"confirm": topic + "-g"}},
		"G6": {http.MethodPost, "/v1/consumer-groups/" + topic + "-g/remove-members", map[string]any{"confirm": topic + "-g"}},
		"G7": {http.MethodPost, "/v1/consumer-groups/" + topic + "-g/clone-offsets", map[string]any{"confirm": topic + "-g", "source": topic + "-g-source"}},
		"M8": {http.MethodPost, "/v1/replays", map[string]any{
			"confirm": topic + "-m8dst",
			"source":  map[string]any{"topic": topic, "from": "beginning"},
			"target":  map[string]any{"topic": topic + "-m8dst"},
		}},
		"C5": {http.MethodPatch, "/v1/cluster/brokers/1/config", map[string]any{"confirm": "1", "set": map[string]any{"log.retention.hours": "168"}}},
		"C9": {http.MethodPost, "/v1/cluster/reassignments", map[string]any{
			"confirm": "irrelevant", "reassignments": []map[string]any{{"topic": topic, "partition": 0, "replicas": []int{1}}},
		}},
		"C12": {http.MethodPost, "/v1/batch/topics/apply", map[string]any{"confirm": "irrelevant", "topics": []map[string]any{}}},
		"S1":  {http.MethodPost, "/v1/scram-users", map[string]any{"name": topic + "-s1", "mechanism": "SCRAM-SHA-256", "password": "pw"}},
		"S2": {http.MethodPatch, "/v1/quotas", map[string]any{
			"confirm": "user:" + topic, "entity": map[string]any{"user": topic}, "set": map[string]any{"producerByteRate": 1048576},
		}},
	}
}

// dataPlaneRoutes covers every M1-M8 command currently wired to a route
// (FUNC-SPEC §9.5 F6 covers M1-M8) — hardcoded rather than merged from
// wRoutes(), since wRoutes() also carries W commands that are not
// data-plane (T5-T12, G4-G7).
func dataPlaneRoutes(topic string) map[string]routeFixture {
	all := wRoutes(topic)
	return map[string]routeFixture{
		"M1": {http.MethodGet, "/v1/topics/" + topic + "/messages?from=beginning&limit=1", nil},
		"M2": {http.MethodGet, "/v1/topics/" + topic + "/partitions/0/messages/0", nil},
		"M3": {http.MethodPost, "/v1/topics/" + topic + "/messages/search", map[string]any{"regex": ".", "from": "beginning", "maxMatches": 1}},
		"M4": {http.MethodPost, "/v1/topics/" + topic + "/messages/filter", map[string]any{
			"filter": map[string]any{"path": "$", "op": "exists"}, "from": "beginning", "maxMatches": 1,
		}},
		"M5": all["M5"],
		"M6": all["M6"],
		"M7": all["M7"],
		"M8": all["M8"],
	}
}

// wRoutesComplete fails t unless routes' keys exactly match command.Table's
// W entries with a registered route.
func wRoutesComplete(t *testing.T, routes map[string]routeFixture) {
	t.Helper()
	pending := map[string]bool{}
	for _, id := range api.Pending() {
		pending[id] = true
	}
	for _, d := range command.Table() {
		if d.Access != command.W || pending[d.ID] {
			continue
		}
		if _, ok := routes[d.ID]; !ok {
			t.Errorf("routes is missing implemented W command %s (%s)", d.ID, d.Name)
		}
	}
	for id := range routes {
		desc, ok := command.Lookup(id)
		if !ok || desc.Access != command.W {
			t.Errorf("routes has %s, which is not an implemented W command", id)
		}
	}
}

func TestAPI_ReadOnly_EveryImplementedWIs403ReadOnlyMode(t *testing.T) {
	t.Parallel()
	routes := wRoutes("t-demo")
	wRoutesComplete(t, routes)

	gw := testutil.NewTestGateway(t, testutil.WithReadOnlyMode())
	gw.Fake.SeedTopic("t-demo", 1)

	for id, r := range routes {
		t.Run(id, func(t *testing.T) {
			resp := gw.Do(t, r.method, r.path, r.body)
			body := decodeBody(t, resp)
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("%s = %d, want 403: %v", id, resp.StatusCode, body)
			}
			errBody, _ := body["error"].(map[string]any)
			if errBody["code"] != "READ_ONLY_MODE" {
				t.Errorf("%s code = %v, want READ_ONLY_MODE", id, errBody["code"])
			}
		})
	}
}

func TestAPI_ReadOnly_RRoutesUnaffected(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithReadOnlyMode())
	gw.Fake.SeedTopic("t-demo", 1)

	for _, path := range []string{"/v1/topics", "/v1/topics/t-demo/messages?from=beginning&limit=1"} {
		resp := gw.Get(t, path)
		body := decodeBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s under read-only mode = %d, want 200: %v", path, resp.StatusCode, body)
		}
	}
}

func TestAPI_Lock_M1ToM7Are403DataPlaneLocked(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithDataPlaneLock())
	gw.Fake.SeedTopic("t-demo", 1)

	for id, r := range dataPlaneRoutes("t-demo") {
		t.Run(id, func(t *testing.T) {
			resp := gw.Do(t, r.method, r.path, r.body)
			body := decodeBody(t, resp)
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("%s = %d, want 403: %v", id, resp.StatusCode, body)
			}
			errBody, _ := body["error"].(map[string]any)
			if errBody["code"] != "DATA_PLANE_LOCKED" {
				t.Errorf("%s code = %v, want DATA_PLANE_LOCKED", id, errBody["code"])
			}
		})
	}
}

func TestAPI_Lock_NonDataPlaneUnaffected(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithDataPlaneLock())
	gw.Fake.SeedTopic("t-demo", 1)

	resp := gw.Get(t, "/v1/topics")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/topics under the data-plane lock = %d, want 200: %v", resp.StatusCode, body)
	}
}

func TestAPI_Lock_OperatorBreakGlass200AndHighAudit(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithDataPlaneLock())
	gw.Fake.SeedTopic("t-demo", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	resp := gw.DoWithHeaders(t, http.MethodGet, "/v1/topics/t-demo/messages?from=beginning&limit=1", nil,
		testutil.DefaultOperatorKey, map[string]string{middleware.HeaderBreakGlass: "incident-42"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}

	events := gw.Audit.Events()
	if len(events) != 1 || events[0].Outcome != audit.OutcomeSucceeded || events[0].Severity != audit.SeverityHigh {
		t.Fatalf("events = %+v, want one SUCCEEDED HIGH event", events)
	}
	if events[0].CommandID != "M1" {
		t.Errorf("CommandID = %q, want M1", events[0].CommandID)
	}
	if events[0].BreakGlass == nil || events[0].BreakGlass.Reason != "incident-42" {
		t.Errorf("BreakGlass = %+v, want reason incident-42", events[0].BreakGlass)
	}
}

func TestAPI_Lock_ReaderBreakGlass403AndHighAudit(t *testing.T) {
	t.Parallel()
	gw := readerGateway(t, testutil.WithDataPlaneLock())
	gw.Fake.SeedTopic("t-demo", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	resp := gw.DoWithHeaders(t, http.MethodGet, "/v1/topics/t-demo/messages?from=beginning&limit=1", nil,
		readerSecret, map[string]string{middleware.HeaderBreakGlass: "incident-42"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "DATA_PLANE_LOCKED" {
		t.Errorf("code = %v, want DATA_PLANE_LOCKED", errBody["code"])
	}

	events := gw.Audit.Events()
	if len(events) != 1 || events[0].Outcome != audit.OutcomeRejected || events[0].Severity != audit.SeverityHigh {
		t.Fatalf("events = %+v, want one REJECTED HIGH event (break-glass was attempted)", events)
	}
}

func TestAPI_Lock_HeaderUnderReadOnlyStill403(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithReadOnlyMode())
	gw.Fake.SeedTopic("t-demo", 1)

	resp := gw.DoWithHeaders(t, http.MethodPost, "/v1/topics/t-demo/messages", map[string]any{"value": "v"},
		testutil.DefaultOperatorKey, map[string]string{middleware.HeaderBreakGlass: "incident-42"})
	body := decodeBody(t, resp)

	// Break-glass bypasses F6 (the data-plane lock) only, never F2
	// (read-only mode) — FUNC-SPEC §9.1 rules C1.
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "READ_ONLY_MODE" {
		t.Errorf("code = %v, want READ_ONLY_MODE", errBody["code"])
	}
}

func TestAPI_Rejection_ReaderOnProduceEmitsWarnRejectedAudit(t *testing.T) {
	t.Parallel()
	gw := readerGateway(t)
	gw.Fake.SeedTopic("t-demo", 1)

	resp := gw.DoWithKey(t, http.MethodPost, "/v1/topics/t-demo/messages", map[string]any{"value": "v"}, readerSecret)
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %v", resp.StatusCode, body)
	}

	events := gw.Audit.Events()
	if len(events) != 1 || events[0].Outcome != audit.OutcomeRejected || events[0].Severity != audit.SeverityWarn {
		t.Fatalf("events = %+v, want one REJECTED WARN event", events)
	}
	if events[0].CommandID != "M5" {
		t.Errorf("CommandID = %q, want M5", events[0].CommandID)
	}
}

// TestAPI_Lock_DryRunStillLocked proves F6 (the data-plane lock) applies
// even to a dry run: core.Destructive's gate check (CheckAudited) runs
// before the dryRun short-circuit, so a locked M8 replay never even builds
// a plan, dry-run or not (FUNC-SPEC §9.1 rules).
func TestAPI_Lock_DryRunStillLocked(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithDataPlaneLock())
	gw.Fake.SeedTopic("src", 1)
	gw.Fake.SeedTopic("dst", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/replays?dryRun=true", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "beginning"},
		"target":  map[string]any{"topic": "dst"},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "DATA_PLANE_LOCKED" {
		t.Errorf("code = %v, want DATA_PLANE_LOCKED", errBody["code"])
	}
}
