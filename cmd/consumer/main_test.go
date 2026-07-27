package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestDefaultConsumerName_IncludesHostnameAndPID(t *testing.T) {
	name := defaultConsumerName()

	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	wantSuffix := "-" + strconv.Itoa(os.Getpid())

	if !strings.HasPrefix(name, host) {
		t.Fatalf("expected name %q to start with hostname %q", name, host)
	}
	if !strings.HasSuffix(name, wantSuffix) {
		t.Fatalf("expected name %q to end with pid suffix %q", name, wantSuffix)
	}
}

func TestDefaultConsumerName_StableAcrossCalls(t *testing.T) {
	// Two consumer instances started without -name should still get
	// distinguishable names in practice (different pid/host), but a single
	// process calling this twice must be deterministic.
	if defaultConsumerName() != defaultConsumerName() {
		t.Fatalf("expected defaultConsumerName to be stable within a process")
	}
}

func TestGroupLabel(t *testing.T) {
	cases := map[string]string{
		"":        "default",
		"workers": "workers",
	}
	for in, want := range cases {
		if got := groupLabel(in); got != want {
			t.Fatalf("groupLabel(%q) = %q, want %q", in, got, want)
		}
	}
}
