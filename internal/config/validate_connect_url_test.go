package config

import "testing"

func TestValidateConnectURL(t *testing.T) {
	for _, valid := range []string{"", "http://kafka-connect.kafka3o-connect-test.svc.cluster.local:8083", "https://connect.example/base/"} {
		if err := ValidateConnectURL(valid); err != nil {
			t.Errorf("%q: %v", valid, err)
		}
	}
	for _, invalid := range []string{"kafka-connect:8083", "ftp://connect", "http://user:pass@connect:8083", "http://connect:8083/?x=1", "http://connect#a", "http://"} {
		if err := ValidateConnectURL(invalid); err == nil {
			t.Errorf("%q was accepted", invalid)
		}
	}
}
