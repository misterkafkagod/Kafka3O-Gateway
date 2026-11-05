//go:build integration

package integration

import (
	"errors"
	"net"
	"strconv"
)

func ValidateExternalTestBroker(address, disposable string) error {
	if disposable != "true" {
		return errors.New("set KAFKA3O_TEST_DISPOSABLE=true only for a disposable Kafka broker")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("KAFKA3O_TEST_BROKER must be a loopback IP address and port")
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return errors.New("KAFKA3O_TEST_BROKER must use a valid TCP port")
	}
	return nil
}
