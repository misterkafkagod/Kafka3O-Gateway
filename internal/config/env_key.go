package config

import "strings"

const envPrefix = "KAFKA3O_"

// EnvKey maps an environment variable to a config key for koanf's env provider:
// KAFKA3O_KAFKA__TIMEOUT -> kafka.timeout ("__" separates nested keys). The value passes through.
func EnvKey(key, value string) (string, any) {
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(key, envPrefix), "__", ".")), value
}
