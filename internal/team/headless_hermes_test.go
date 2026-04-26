package team

import (
	"context"
	"os/exec"
	"testing"
)

func TestRunHeadlessHermesTurnBinaryNotFound(t *testing.T) {
	orig := headlessHermesLookPath
	headlessHermesLookPath = func(file string) (string, error) {
		return "", &exec.Error{Name: file, Err: exec.ErrNotFound}
	}
	defer func() { headlessHermesLookPath = orig }()

	l := &Launcher{broker: &Broker{}}
	err := l.runHeadlessHermesTurn(context.Background(), "director", "write a brief")
	if err == nil {
		t.Fatal("expected error when hermes not found")
	}
}
