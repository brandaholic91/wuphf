package provider_test

import (
	"testing"

	"github.com/nex-crm/wuphf/internal/provider"
)

func TestKindHermesRegistered(t *testing.T) {
	e := provider.Lookup(provider.KindHermes)
	if e == nil {
		t.Fatal("KindHermes not registered in provider registry")
	}
	if !e.Capabilities.SupportsOneShot {
		t.Error("KindHermes should support one-shot")
	}
	if e.Capabilities.PaneEligible {
		t.Error("KindHermes should not be pane eligible (headless only)")
	}
}

func TestRunHermesOneShotNotFound(t *testing.T) {
	t.Setenv("PATH", "/nonexistent")
	_, err := provider.RunHermesOneShot("system prompt", "hello", t.TempDir())
	if err == nil {
		t.Fatal("expected error when hermes binary not found")
	}
}
