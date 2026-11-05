//go:build integration

package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
	"github.com/misterkafkagod/Kafka3O-Gateway/test/integration"
)

func TestGateway_AuthenticatedKafkaWriteLifecycle(t *testing.T) {
	var cfg config.KafkaConfig
	secureBroker, secure := os.LookupEnv("KAFKA3O_TEST_SECURE_BROKER")
	if secure {
		if err := integration.ValidateExternalTestBroker(secureBroker, os.Getenv("KAFKA3O_TEST_DISPOSABLE")); err != nil {
			t.Fatal(err)
		}
		t.Setenv("KAFKA3O_KAFKA__BROKERS", secureBroker)
		loaded, err := config.LoadConfig("")
		if err != nil {
			t.Fatal(err)
		}
		cfg = loaded.Kafka
		if !cfg.TLS.Enabled || strings.ToUpper(cfg.SASL.Mechanism) != "SCRAM-SHA-512" {
			t.Fatal("secured gateway test requires TLS and SCRAM-SHA-512")
		}
	} else {
		cfg.Brokers = integration.StartKafka(t)
	}
	cfg.Timeout = 30 * time.Second
	client, err := kafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if secure {
		integration.VerifyExternalTestBroker(t, client, secureBroker, os.Getenv("KAFKA3O_TEST_DISPOSABLE"))
	}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	admin, writeAdmin := kadm.NewClient(client), kadm.NewClient(producer)

	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("integration", "1.0"))
	httpapi.RegisterLive(api)
	httpapi.RegisterGetCluster(api, cluster.NewGetCluster(kafka.NewDescribeCluster(client), cfg.Timeout))
	httpapi.RegisterListTopics(api, topics.NewListTopics(kafka.NewListTopicMetadata(client), kafka.NewCountTopicMessages(admin), cfg.Timeout))
	httpapi.RegisterGetTopicDetail(api, topics.NewGetTopicDetail(kafka.NewDescribeTopic(admin), cfg.Timeout))
	httpapi.RegisterListLatestTopicMessages(api,
		topics.NewListLatestTopicMessages(kafka.NewListLatestTopicMessages(admin, client), cfg.Timeout),
		topics.NewReadTopicMessages(kafka.NewReadTopicMessages(admin, client), cfg.Timeout))
	httpapi.RegisterCreateTopic(api, topics.NewCreateTopic(kafka.NewCreateKafkaTopic(writeAdmin), cfg.Timeout))
	httpapi.RegisterProduceTopicMessage(api, topics.NewProduceTopicMessage(kafka.NewSendTopicMessage(producer), cfg.Timeout))
	httpapi.RegisterDeleteTopic(api, topics.NewDeleteTopic(kafka.NewDeleteKafkaTopic(writeAdmin), cfg.Timeout))

	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := fixture.TLS.Certificates[0]
	fixture.Close()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	token := hex.EncodeToString(random)
	const origin = "https://ui.example"
	gateway := newGatewayHTTPServer("", mux, []string{origin}, token, certificate)
	server := httptest.NewUnstartedServer(gateway.Handler)
	server.TLS = gateway.TLSConfig
	server.StartTLS()
	defer server.Close()
	httpClient := server.Client()
	httpClient.Timeout = 40 * time.Second

	request := func(method, path, body string, authorized bool) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if authorized {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, data
	}

	if status, _ := request(http.MethodGet, "/api/v1/health/live", "", false); status != http.StatusOK {
		t.Fatalf("anonymous liveness status = %d", status)
	}
	preflight, err := http.NewRequest(http.MethodOptions, server.URL+"/api/v1/topics", nil)
	if err != nil {
		t.Fatal(err)
	}
	preflight.Header.Set("Origin", origin)
	preflight.Header.Set("Access-Control-Request-Method", http.MethodPost)
	preflight.Header.Set("Access-Control-Request-Headers", "authorization,content-type")
	response, err := httpClient.Do(preflight)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.Header.Get("Access-Control-Allow-Origin") != origin ||
		!strings.Contains(response.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("preflight status = %d, headers = %v", response.StatusCode, response.Header)
	}
	if status, _ := request(http.MethodGet, "/api/v1/cluster", "", false); status != http.StatusUnauthorized {
		t.Fatalf("anonymous cluster status = %d", status)
	}
	if status, body := request(http.MethodGet, "/api/v1/cluster", "", true); status != http.StatusOK {
		t.Fatalf("authenticated cluster status = %d", status)
	} else {
		var got cluster.Cluster
		if err := json.Unmarshal(body, &got); err != nil || got.ID == "" || len(got.Brokers) != 1 {
			t.Fatalf("authenticated cluster response is invalid: %v", err)
		}
	}

	const create = `{"name":"web-integration","partitions":1,"replicationFactor":1}`
	if status, _ := request(http.MethodPost, "/api/v1/topics", create, false); status != http.StatusUnauthorized {
		t.Fatalf("anonymous create status = %d", status)
	}
	if status, _ := request(http.MethodPost, "/api/v1/topics", create, true); status != http.StatusCreated {
		t.Fatalf("authenticated create status = %d", status)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, body := request(http.MethodGet, "/api/v1/topics", "", true)
		if status != http.StatusOK {
			t.Fatalf("list topics status = %d", status)
		}
		var listed []topics.Topic
		if err := json.Unmarshal(body, &listed); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, topic := range listed {
			found = found || topic.Name == "web-integration"
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("created topic did not appear in the topic list")
		}
		time.Sleep(100 * time.Millisecond)
	}

	status, body := request(http.MethodGet, "/api/v1/topics/web-integration/messages", "", true)
	if status != http.StatusOK {
		t.Fatalf("read empty topic status = %d", status)
	}
	var emptyMessages []topics.TopicMessage
	if err := json.Unmarshal(body, &emptyMessages); err != nil || emptyMessages == nil || len(emptyMessages) != 0 {
		t.Fatalf("read empty topic = %#v, error = %v", emptyMessages, err)
	}

	const message = `{"value":"café","key":"","headers":[{"key":"source","value":"integration"}]}`
	status, body = request(http.MethodPost, "/api/v1/topics/web-integration/messages", message, true)
	if status != http.StatusOK {
		t.Fatalf("produce status = %d", status)
	}
	var position topics.ProduceMessageResult
	if err := json.Unmarshal(body, &position); err != nil || position.Partition != 0 || position.Offset != "0" {
		t.Fatalf("produce response = %#v, error = %v", position, err)
	}
	status, body = request(http.MethodGet, "/api/v1/topics/web-integration/messages?partition=0&offset=0", "", true)
	if status != http.StatusOK {
		t.Fatalf("read produced message status = %d", status)
	}
	var messages []topics.TopicMessage
	if err := json.Unmarshal(body, &messages); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Partition != position.Partition || messages[0].OffsetExact != position.Offset ||
		messages[0].Key == nil || len(messages[0].Key) != 0 || string(messages[0].Value) != "café" ||
		len(messages[0].Headers) != 1 || messages[0].Headers[0].Key != "source" || string(messages[0].Headers[0].Value) != "integration" {
		t.Fatalf("read produced message = %#v", messages)
	}

	if status, _ := request(http.MethodDelete, "/api/v1/topics/web-integration", "", false); status != http.StatusUnauthorized {
		t.Fatalf("anonymous delete status = %d", status)
	}
	status, body = request(http.MethodDelete, "/api/v1/topics/web-integration", "", true)
	if status != http.StatusAccepted {
		t.Fatalf("delete status = %d", status)
	}
	var accepted struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &accepted); err != nil || accepted.Name != "web-integration" || accepted.Status != "accepted" {
		t.Fatalf("delete response = %#v, error = %v", accepted, err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		status, body := request(http.MethodGet, "/api/v1/topics/web-integration", "", true)
		if status == http.StatusNotFound {
			break
		}
		if status != http.StatusOK {
			var problem struct {
				Code   string `json:"code"`
				Detail string `json:"detail"`
			}
			if err := json.Unmarshal(body, &problem); err != nil {
				t.Fatalf("deleted topic status = %d; invalid problem response: %v", status, err)
			}
			t.Fatalf("deleted topic status = %d, code = %q, detail = %q", status, problem.Code, problem.Detail)
		}
		if time.Now().After(deadline) {
			t.Fatal("deleted topic remained visible")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
