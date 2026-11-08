package policyapi

import "fmt"

// errorType names an error's type for logs without exposing its message.
func errorType(err error) string {
	return fmt.Sprintf("%T", err)
}
