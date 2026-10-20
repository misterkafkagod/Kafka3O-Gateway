package config

import "testing"

func TestEnvKey_StripsPrefixAndNestsOnDoubleUnderscore(t *testing.T) {
	key, value := EnvKey("KAFKA3O_KAFKA__TIMEOUT", "7s")

	if key != "kafka.timeout" {
		t.Errorf("key = %q, want kafka.timeout", key)
	}
	if value != "7s" {
		t.Errorf("value = %v, want 7s", value)
	}
}

func TestEnvKey_LowercasesCamelCaseKeys(t *testing.T) {
	key, _ := EnvKey("KAFKA3O_HTTP__CORS__ALLOWEDORIGINS", "http://localhost:5173")

	if key != "http.cors.allowedorigins" {
		t.Errorf("key = %q, want http.cors.allowedorigins", key)
	}
}

func TestEnvKey_KeepsSingleUnderscores(t *testing.T) {
	key, _ := EnvKey("KAFKA3O_KAFKA__SASL_MECHANISM", "PLAIN")

	if key != "kafka.sasl_mechanism" {
		t.Errorf("key = %q, want kafka.sasl_mechanism", key)
	}
}
