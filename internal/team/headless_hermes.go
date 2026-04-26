package team

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/nex-crm/wuphf/internal/provider"
	"github.com/nex-crm/wuphf/internal/runtimebin"
)

// Hermes-specific test hooks, kept separate from Codex/Opencode hooks so test
// setups can stub one runtime without colliding with the others.
var (
	headlessHermesLookPath       = runtimebin.LookPath
	headlessHermesCommandContext = exec.CommandContext
)

// runHeadlessHermesTurn executes a single Hermes turn for slug using the
// `hermes -z <prompt>` one-shot CLI. The agent's reply is streamed to the
// broker's AgentStream and posted to channel when non-empty. This mirrors
// runHeadlessOpencodeTurn but targets the Hermes runtime instead.
func (l *Launcher) runHeadlessHermesTurn(ctx context.Context, slug string, notification string, channel ...string) error {
	if _, err := headlessHermesLookPath("hermes"); err != nil {
		return fmt.Errorf("hermes not found in PATH (install: pip install hermes-agent): %w", err)
	}
	if l == nil || l.broker == nil {
		return fmt.Errorf("broker is not running")
	}

	workspaceDir := strings.TrimSpace(l.cwd)
	if worktreeDir := l.headlessTaskWorkspaceDir(slug); worktreeDir != "" {
		workspaceDir = worktreeDir
	}
	workspaceDir = normalizeHeadlessWorkspaceDir(workspaceDir)
	if workspaceDir == "" {
		workspaceDir = "."
	}

	systemPrompt := l.buildPrompt(slug)
	fullPrompt := provider.BuildHermesPromptExported(systemPrompt, notification)

	args := []string{"-z", fullPrompt}

	cmd := headlessHermesCommandContext(ctx, "hermes", args...)
	cmd.Dir = workspaceDir
	configureHeadlessProcess(cmd)

	// Reuse the Codex env builder for broker/workspace plumbing, then
	// tag the provider so WUPHF telemetry can distinguish the runner.
	env := l.buildHeadlessCodexEnv(slug, workspaceDir, firstNonEmpty(channel...))
	env = setEnvValue(env, "WUPHF_HEADLESS_PROVIDER", "hermes")
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("attach hermes stdout: %w", err)
	}

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	startedAt := time.Now()
	if err := cmd.Start(); err != nil {
		return err
	}

	// C2: watch for context cancellation and terminate the child process so
	// the scanner goroutine is not blocked on a pipe that will never close.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			terminateHeadlessProcess(cmd)
			_ = stdout.Close()
		case <-done:
		}
	}()

	var agentStream *agentStreamBuffer
	if l.broker != nil {
		agentStream = l.broker.AgentStream(slug)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		if agentStream != nil && strings.TrimSpace(line) != "" {
			b, _ := json.Marshal(line)
			agentStream.Push(`{"type":"text","content":` + string(b) + `}`)
		}
	}

	// C1: if the scanner hit a >4 MiB line the pipe is full and cmd.Wait()
	// would deadlock; kill the child first so Wait returns promptly.
	if scanErr := scanner.Err(); scanErr != nil {
		if errors.Is(scanErr, bufio.ErrTooLong) {
			terminateHeadlessProcess(cmd)
		}
	}

	if err := cmd.Wait(); err != nil {
		stderr := strings.TrimSpace(stderrBuf.String())
		appendHeadlessCodexLog(slug, "hermes_stderr: "+stderr)
		return fmt.Errorf("hermes: %s: %w", stderr, err)
	}

	response := strings.Join(lines, "\n")
	text := strings.TrimSpace(response)
	if text != "" {
		appendHeadlessCodexLog(slug, "hermes_result: "+text)
		target := firstNonEmpty(channel...)
		msg, posted, err := l.postHeadlessFinalMessageIfSilent(slug, target, notification, text, startedAt)
		if err != nil {
			appendHeadlessCodexLog(slug, "hermes_fallback-post-error: "+err.Error())
		} else if posted {
			appendHeadlessCodexLog(slug, fmt.Sprintf("hermes_fallback-post: posted final output to #%s as %s", msg.Channel, msg.ID))
		}
	}
	return nil
}
