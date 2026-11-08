package gatewayauth

import "testing"

func TestClassifyOutcome(t *testing.T) {
	for _, tt := range []struct {
		status int
		body   string
		want   string
	}{
		{200, `{"offset":1}`, "confirmed"}, {201, ``, "confirmed"}, {204, ``, "confirmed"},
		{400, `{"code":"invalid_message"}`, "validation_rejected"}, {409, `{"code":"topic_exists"}`, "validation_rejected"},
		{503, `{"code":"delivery_unknown"}`, "unknown"}, {503, `{"code":"topic_write_unknown"}`, "unknown"},
		{503, `{"code":"configuration_write_unknown"}`, "unknown"}, {503, `{"code":"partition_write_unknown"}`, "unknown"},
		{503, `{"code":"cluster_unreachable"}`, "failed"}, {500, `not json`, "failed"}, {302, ``, "unknown"},
	} {
		if got := classifyOutcome(tt.status, []byte(tt.body)); got != tt.want {
			t.Errorf("classifyOutcome(%d, %s) = %s, want %s", tt.status, tt.body, got, tt.want)
		}
	}
}
