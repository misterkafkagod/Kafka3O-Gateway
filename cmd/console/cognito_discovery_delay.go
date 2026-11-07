package main

import "time"

func cognitoDiscoveryDelay(attempt int) time.Duration {
	if attempt >= 6 {
		return time.Minute
	}
	return time.Second << attempt
}
