package config

import (
	"errors"
	"slices"
	"testing"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadMissingBrokers(t *testing.T) {
	for _, v := range []string{"", "  ", " , ,"} {
		_, err := Load(env(map[string]string{"KAFKA3O_BROKERS": v}))
		if !errors.Is(err, ErrNoBrokers) {
			t.Errorf("brokers %q: got err %v, want ErrNoBrokers", v, err)
		}
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"KAFKA3O_BROKERS": "kafka:9092"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":8080" {
		t.Errorf("Listen = %q, want :8080", cfg.Listen)
	}
	if cfg.UIDir != "" {
		t.Errorf("UIDir = %q, want empty", cfg.UIDir)
	}
}

func TestLoadBrokerListSplitAndTrimmed(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"KAFKA3O_BROKERS": " a:9092, b:9092 ,,c:9092 ",
		"KAFKA3O_LISTEN":  "127.0.0.1:9000",
		"KAFKA3O_UI_DIR":  "/srv/ui",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a:9092", "b:9092", "c:9092"}; !slices.Equal(cfg.Brokers, want) {
		t.Errorf("Brokers = %v, want %v", cfg.Brokers, want)
	}
	if cfg.Listen != "127.0.0.1:9000" || cfg.UIDir != "/srv/ui" {
		t.Errorf("got Listen %q UIDir %q", cfg.Listen, cfg.UIDir)
	}
}
