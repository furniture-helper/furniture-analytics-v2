package config

import (
	"os"
	"strconv"
	"strings"
)

func GetMaximumRuntime() uint {
	maxRuntimeEnv := strings.TrimSpace(os.Getenv("MAXIMUM_RUNTIME_SECS"))
	if maxRuntimeEnv == "" {
		return 60
	}

	maxRuntime, err := strconv.Atoi(maxRuntimeEnv)
	if err != nil || maxRuntime <= 0 {
		return 60
	}

	return uint(maxRuntime)
}
