//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_M1_ReadMessages(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic+"/messages?from=beginning&limit=10", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Items []struct {
			Topic  string `json:"topic"`
			Offset int64  `json:"offset"`
		} `json:"items"`
		Scan struct {
			Scanned      int              `json:"scanned"`
			ReachedEnd   bool             `json:"reachedEnd"`
			Continuation map[string]int64 `json:"continuation"`
		} `json:"scan"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) == 0 {
		t.Error("items = [], want at least one message (KGW_ACC_TOPIC must have data)")
	}
	for _, item := range body.Items {
		if item.Topic != topic {
			t.Errorf("item.topic = %q, want %q", item.Topic, topic)
		}
	}
}

func TestAcceptance_M2_GetMessage(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic+"/partitions/0/messages/0", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (partition 0 offset 0 must exist on KGW_ACC_TOPIC)", resp.StatusCode)
	}
	var body struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    int64  `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Topic != topic || body.Partition != 0 || body.Offset != 0 {
		t.Errorf("body = %+v, want topic=%s partition=0 offset=0", body, topic)
	}
}
