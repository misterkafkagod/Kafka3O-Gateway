package main

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestGatewayMainStartupFailsClosed(t *testing.T) {
	token := strings.Repeat("a", 32)
	tests := []struct {
		name        string
		token       string
		certificate string
		key         string
		want        string
	}{
		{name: "missing token", want: "KAFKA3O_GATEWAY_TOKEN"},
		{name: "missing certificate", token: token, want: "http.tls.certificateFile"},
		{name: "unreadable certificate", token: token, certificate: "missing-cert.pem", key: "missing-key.pem", want: "load gateway TLS certificate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestGatewayStartupHelper$")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(strings.ToUpper(entry), "KAFKA3O_") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env,
				"KAFKA3O_TEST_STARTUP=1",
				"KAFKA3O_KAFKA__BROKERS=localhost:9092",
				"KAFKA3O_GATEWAY_TOKEN="+tt.token,
				"KAFKA3O_HTTP__TLS__CERTIFICATEFILE="+tt.certificate,
				"KAFKA3O_HTTP__TLS__KEYFILE="+tt.key,
			)
			output, err := cmd.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), tt.want) {
				t.Fatalf("exit = %v, output = %s", err, output)
			}
			if tt.token != "" && strings.Contains(string(output), tt.token) {
				t.Fatal("startup error disclosed the token")
			}
		})
	}
}

func TestGatewayStartupHelper(t *testing.T) {
	if os.Getenv("KAFKA3O_TEST_STARTUP") != "1" {
		return
	}
	flag.CommandLine = flag.NewFlagSet("gateway", flag.ContinueOnError)
	os.Args = []string{"gateway"}
	main()
}
