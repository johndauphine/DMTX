package app

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("XDG_CACHE_HOME") != "" {
		os.Exit(m.Run())
	}
	cache, err := os.MkdirTemp("", "dmtx-app-test-cache-")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("XDG_CACHE_HOME", cache); err != nil {
		panic(err)
	}
	if err := os.Setenv("LOCALAPPDATA", cache); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(cache)
	os.Exit(code)
}
