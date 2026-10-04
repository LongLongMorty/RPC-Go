package config

import (
	"os"
	"strings"
)

func Get(flagValue, envKey, defaultVal string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return defaultVal
}

func Endpoints(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
