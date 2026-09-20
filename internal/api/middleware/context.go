package middleware

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// callerKey is the context key the api-key middleware sets.
type callerKey struct{}

// withCaller returns a context carrying c, for the api-key middleware.
func withCaller(ctx context.Context, c core.Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// CallerFrom returns the Caller the api-key middleware built for this
// request, for handlers and services registered in later phases.
func CallerFrom(ctx context.Context) (core.Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(core.Caller)
	return c, ok
}

// clientIPKey is the context key the client-ip middleware sets.
type clientIPKey struct{}

// withClientIP returns a context carrying ip, for the api-key middleware to
// read when it builds a Caller.
func withClientIP(ctx context.Context, ip string) context.Context {
	return context.WithValue(ctx, clientIPKey{}, ip)
}

// clientIPFrom returns the resolved client IP for this request, or "" if the
// client-ip middleware did not run.
func clientIPFrom(ctx context.Context) string {
	ip, _ := ctx.Value(clientIPKey{}).(string)
	return ip
}
