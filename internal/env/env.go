// Package env reads Clearline service configuration from environment
// variables, with defaults that match the local compose stack.
package env

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Defaults for the local compose stack.
const (
	DefaultDatabaseURL = "postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable" //nolint:gosec // G101: local compose dev credentials, not a secret
	DefaultBrokers     = "localhost:9092"
	DefaultRedisAddr   = "localhost:6379"
)

// String returns the variable, or def when it is unset or empty.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// DatabaseURL is CLEARLINE_DATABASE_URL.
func DatabaseURL() string { return String("CLEARLINE_DATABASE_URL", DefaultDatabaseURL) }

// Brokers is CLEARLINE_KAFKA_BROKERS, comma separated.
func Brokers() []string {
	return strings.Split(String("CLEARLINE_KAFKA_BROKERS", DefaultBrokers), ",")
}

// RedisAddr is CLEARLINE_REDIS_ADDR.
func RedisAddr() string { return String("CLEARLINE_REDIS_ADDR", DefaultRedisAddr) }

// RedisPrefix is CLEARLINE_REDIS_PREFIX, the key prefix for balances.
func RedisPrefix() string { return String("CLEARLINE_REDIS_PREFIX", "clearline:") }

// Int parses an integer variable, or returns def when unset.
func Int(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

// Duration parses a time.Duration variable such as "250ms", or returns def.
func Duration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
