package duro_test

import (
	"context"
	"testing"
	"time"

	"github.com/lemonberrylabs/duro"
)

// TestConfigIdentityPropagates proves Config.ApplicationVersion and
// Config.ExecutorID reach the DBOS context — the two identity knobs that scope
// recovery, previously reachable only through DBOS__APPVERSION / DBOS__VMID.
func TestConfigIdentityPropagates(t *testing.T) {
	// Env vars override the Config fields (see the override test); make sure
	// neither is set so this test observes the Config values.
	t.Setenv("DBOS__APPVERSION", "")
	t.Setenv("DBOS__VMID", "")

	a, err := duro.New(context.Background(), duro.Config{
		Name:               "duro-test-identity",
		DatabaseURL:        testDatabaseURL(),
		ApplicationVersion: "v-config-1",
		ExecutorID:         "exec-config-1",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Shutdown(5 * time.Second)

	if got := a.Context().GetApplicationVersion(); got != "v-config-1" {
		t.Errorf("ApplicationVersion = %q, want %q", got, "v-config-1")
	}
	if got := a.Context().GetExecutorID(); got != "exec-config-1" {
		t.Errorf("ExecutorID = %q, want %q", got, "exec-config-1")
	}
}

// TestConfigIdentityEnvOverride proves the DBOS precedence duro inherits: the
// DBOS__APPVERSION / DBOS__VMID environment variables win over the Config
// fields, matching DBOS's own behavior so deployment tooling that sets them
// keeps working unchanged.
func TestConfigIdentityEnvOverride(t *testing.T) {
	t.Setenv("DBOS__APPVERSION", "v-env-2")
	t.Setenv("DBOS__VMID", "exec-env-2")

	a, err := duro.New(context.Background(), duro.Config{
		Name:               "duro-test-identity-env",
		DatabaseURL:        testDatabaseURL(),
		ApplicationVersion: "v-config-2",
		ExecutorID:         "exec-config-2",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Shutdown(5 * time.Second)

	if got := a.Context().GetApplicationVersion(); got != "v-env-2" {
		t.Errorf("ApplicationVersion = %q, want env override %q", got, "v-env-2")
	}
	if got := a.Context().GetExecutorID(); got != "exec-env-2" {
		t.Errorf("ExecutorID = %q, want env override %q", got, "exec-env-2")
	}
}

// TestConfigIdentityDefaults proves an empty ApplicationVersion still launches:
// DBOS falls back to its computed default (a binary hash) rather than failing.
func TestConfigIdentityDefaults(t *testing.T) {
	t.Setenv("DBOS__APPVERSION", "")
	t.Setenv("DBOS__VMID", "")

	a, err := duro.New(context.Background(), duro.Config{
		Name:        "duro-test-identity-default",
		DatabaseURL: testDatabaseURL(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Shutdown(5 * time.Second)

	if got := a.Context().GetApplicationVersion(); got == "" {
		t.Error("ApplicationVersion is empty, want DBOS's computed default")
	}
	if got := a.Context().GetExecutorID(); got != "local" {
		t.Errorf("ExecutorID = %q, want DBOS default %q", got, "local")
	}
}
