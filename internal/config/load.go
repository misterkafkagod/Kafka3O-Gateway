package config

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// EnvPrefix is the prefix of every environment override (TECH-SPEC §5.4).
const EnvPrefix = "KGW_"

// delim separates nested keys in paths such as "http.read_header_timeout".
const delim = "."

// Options control Load. A nil Environ means os.Environ().
type Options struct {
	// Path of the YAML file; empty loads defaults + environment only.
	Path string
	// Environ overrides the process environment (tests); nil = os.Environ().
	Environ []string
}

// Load builds a validated Config: Default() overlaid by the YAML file (if any),
// overlaid by KGW_ environment variables — environment wins (TECH-SPEC §5.4).
// Unknown YAML keys and unknown KGW_ variables are errors, so typos fail fast.
func Load(o Options) (Config, error) {
	cfg := Default()
	sch := schema()
	k := koanf.New(delim)

	if o.Path != "" {
		if err := k.Load(file.Provider(o.Path), yaml.Parser()); err != nil {
			return Config{}, fmt.Errorf("config: read %s: %w", o.Path, err)
		}
		if err := unknownKeys(k.Keys(), sch); err != nil {
			return Config{}, fmt.Errorf("config: %s: %w", o.Path, err)
		}
	}

	environ := o.Environ
	if environ == nil {
		environ = os.Environ()
	}
	var unknownEnv []string
	provider := env.Provider(delim, env.Opt{
		Prefix:      EnvPrefix,
		EnvironFunc: func() []string { return environ },
		TransformFunc: func(name, value string) (string, any) {
			path, val, ok := sch.fromEnv(name, value)
			if !ok {
				unknownEnv = append(unknownEnv, name)
				return "", nil
			}
			return path, val
		},
	})
	if err := k.Load(provider, nil); err != nil {
		return Config{}, fmt.Errorf("config: environment: %w", err)
	}
	if len(unknownEnv) > 0 {
		sort.Strings(unknownEnv)
		return Config{}, fmt.Errorf("config: %w: unknown environment variable(s): %s",
			ErrInvalid, strings.Join(unknownEnv, ", "))
	}

	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("config: %w: %w", ErrInvalid, err)
	}
	if err := Validate(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// PathFromArgs returns the configuration file path from a "--config <path>" or
// "--config=<path>" argument, falling back to KGW_CONFIG, then "".
func PathFromArgs(args, environ []string) string {
	for i, a := range args {
		if a == "--config" && i+1 < len(args) {
			return args[i+1]
		}
		if p, ok := strings.CutPrefix(a, "--config="); ok {
			return p
		}
	}
	for _, e := range environ {
		if p, ok := strings.CutPrefix(e, EnvPrefix+"CONFIG="); ok {
			return p
		}
	}
	return ""
}

// reservedEnv are KGW_ variables that are read elsewhere, not configuration keys:
// KGW_CONFIG by PathFromArgs, KGW_PROBE_API_KEY by the probe subcommand (TECH-SPEC B3).
func reservedEnv() map[string]bool {
	return map[string]bool{EnvPrefix + "CONFIG": true, EnvPrefix + "PROBE_API_KEY": true}
}

// envAliases are the TECH-SPEC §6.3 variable names that do not follow the
// tree-derived form; both the alias and the derived name are accepted.
func envAliases() map[string]string {
	return map[string]string{
		EnvPrefix + "API_KEYS":              "auth.keys",
		EnvPrefix + "CORS_ORIGINS":          "http.cors.origins",
		EnvPrefix + "DOCS_ENABLED":          "http.docs.enabled",
		EnvPrefix + "HTTP_TLS_CERT":         "http.tls.cert_file",
		EnvPrefix + "HTTP_TLS_KEY":          "http.tls.key_file",
		EnvPrefix + "KAFKA_ISOLATION_LEVEL": "kafka.consumer.isolation_level",
	}
}

// leafKind says how an environment value must be shaped for a leaf path.
type leafKind int

const (
	leafScalar leafKind = iota // string, number, bool, duration, ByteSize
	leafList                   // []string — comma-separated in the environment
	leafJSON                   // []Key — a JSON document in the environment
)

// configSchema is the set of leaf paths of Config, derived by reflection so the
// struct stays the single source of truth for both YAML and environment keys.
type configSchema struct {
	leaves map[string]leafKind // "http.read_header_timeout" → kind
	byEnv  map[string]string   // "KGW_HTTP_READ_HEADER_TIMEOUT" → path
}

func schema() configSchema {
	s := configSchema{leaves: map[string]leafKind{}, byEnv: map[string]string{}}
	walk(reflect.TypeFor[Config](), "", s.leaves)
	for path := range s.leaves {
		s.byEnv[EnvName(path)] = path
	}
	for name, path := range envAliases() {
		s.byEnv[name] = path
	}
	return s
}

// EnvName is the environment variable that overrides a configuration path:
// "http.read_header_timeout" → "KGW_HTTP_READ_HEADER_TIMEOUT".
func EnvName(path string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(path, delim, "_"))
}

// Paths lists every leaf configuration path, sorted.
func Paths() []string {
	s := schema()
	paths := make([]string, 0, len(s.leaves))
	for p := range s.leaves {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

func walk(t reflect.Type, prefix string, out map[string]leafKind) {
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("koanf")
		if tag == "" {
			continue
		}
		path := tag
		if prefix != "" {
			path = prefix + delim + tag
		}
		switch {
		case f.Type.Kind() == reflect.Struct:
			walk(f.Type, path, out)
		case f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Struct:
			out[path] = leafJSON
		case f.Type.Kind() == reflect.Slice:
			out[path] = leafList
		default:
			out[path] = leafScalar
		}
	}
}

// fromEnv maps one environment variable to its configuration path and a value
// shaped for that leaf. ok is false for unknown variables; reserved variables
// are silently skipped (empty path, ok true).
func (s configSchema) fromEnv(name, value string) (string, any, bool) {
	if reservedEnv()[name] {
		return "", nil, true
	}
	path, known := s.byEnv[name]
	if !known {
		return "", nil, false
	}
	switch s.leaves[path] {
	case leafList:
		return path, splitList(value), true
	case leafJSON:
		var items []map[string]any
		if err := json.Unmarshal([]byte(value), &items); err != nil {
			// Surface the parse error through the decoder by passing the raw string.
			return path, value, true
		}
		return path, items, true
	case leafScalar:
	}
	return path, value, true
}

func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func unknownKeys(keys []string, s configSchema) error {
	var unknown []string
	for _, k := range keys {
		if _, ok := s.leaves[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("%w: unknown key(s): %s", ErrInvalid, strings.Join(unknown, ", "))
}

// yamlLeafPaths returns the set of leaf key paths present in a YAML file, in
// the same dotted form as Paths(); tests use it to prove the example file
// documents every key.
func yamlLeafPaths(path string) (map[string]bool, error) {
	k := koanf.New(delim)
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, err
	}
	present := make(map[string]bool, len(k.Keys()))
	for _, key := range k.Keys() {
		present[key] = true
	}
	return present, nil
}
