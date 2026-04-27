package team

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nex-crm/wuphf/internal/provider"
	"github.com/nex-crm/wuphf/internal/runtimebin"
)

// taskIDRe extracts task IDs like [task-82] from notification text.
var taskIDRe = regexp.MustCompile(`\[task-(\d+)\]`)

// extractTaskID returns the first task ID found in the notification, or "".
func extractTaskID(notification string) string {
	m := taskIDRe.FindStringSubmatch(notification)
	if len(m) < 2 {
		return ""
	}
	return "task-" + m[1]
}

// brokerTaskAction calls the WUPHF broker task API with the given action.
// It is a best-effort call — errors are logged but never fatal.
func brokerTaskAction(brokerURL, token, taskID, action, owner string) {
	if brokerURL == "" || token == "" || taskID == "" {
		return
	}
	body := map[string]string{"action": action, "id": taskID}
	if owner != "" {
		body["owner"] = owner
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, brokerURL+"/tasks", bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// hermesSessionStore persists the last Hermes session ID per agent slug so
// subsequent turns can resume the conversation with --resume <id>.
var (
	hermesSessionMu    sync.Mutex
	hermesSessionCache = map[string]string{} // slug → session_id
)

// hermesSessionPath returns the path to the per-slug session ID file.
func hermesSessionPath(slug string) string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".wuphf", "hermes-sessions", slug+".session")
}

// loadHermesSessionID reads the stored session ID for slug, or "" if none.
func loadHermesSessionID(slug string) string {
	hermesSessionMu.Lock()
	defer hermesSessionMu.Unlock()
	if id, ok := hermesSessionCache[slug]; ok {
		return id
	}
	data, err := os.ReadFile(hermesSessionPath(slug))
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(data))
	hermesSessionCache[slug] = id
	return id
}

// saveHermesSessionID persists the session ID for slug.
func saveHermesSessionID(slug, sessionID string) {
	if sessionID == "" {
		return
	}
	hermesSessionMu.Lock()
	defer hermesSessionMu.Unlock()
	hermesSessionCache[slug] = sessionID
	dir := filepath.Dir(hermesSessionPath(slug))
	_ = os.MkdirAll(dir, 0o700)
	_ = os.WriteFile(hermesSessionPath(slug), []byte(sessionID), 0o600)
}

// latestHermesSessionID finds the session file created after startedAt and
// returns its session_id field. Returns "" if nothing new is found.
func latestHermesSessionID(startedAt time.Time) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	sessionsDir := filepath.Join(home, ".hermes", "sessions")
	entries, err := os.ReadDir(sessionsDir)
	if err != nil {
		return ""
	}
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if !strings.HasPrefix(e.Name(), "session_") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().Before(startedAt) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(sessionsDir, e.Name()))
		if err != nil {
			continue
		}
		var s struct {
			SessionID string `json:"session_id"`
		}
		if err := json.Unmarshal(data, &s); err == nil && s.SessionID != "" {
			return s.SessionID
		}
	}
	return ""
}

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

	// Claim the active task via the broker so the Tasks UI shows in_progress
	// without the agent needing to burn an LLM call on a team_task tool call.
	brokerURL := l.BrokerBaseURL()
	brokerToken := ""
	if l.broker != nil {
		brokerToken = l.broker.Token()
	}
	activeTaskID := extractTaskID(notification)
	if activeTaskID != "" {
		brokerTaskAction(brokerURL, brokerToken, activeTaskID, "claim", slug)
		appendHeadlessCodexLog(slug, "hermes_task_claim: "+activeTaskID)
	}

	systemPrompt := l.buildPrompt(slug)
	fullPrompt := provider.BuildHermesPromptExported(systemPrompt, notification)

	args := []string{"-z", fullPrompt}
	if sessionID := loadHermesSessionID(slug); sessionID != "" {
		args = append(args, "--resume", sessionID)
	}

	cmd := headlessHermesCommandContext(ctx, "hermes", args...)
	cmd.Dir = workspaceDir
	configureHeadlessProcess(cmd)

	// Reuse the Codex env builder for broker/workspace plumbing, then
	// tag the provider so WUPHF telemetry can distinguish the runner.
	env := l.buildHeadlessCodexEnv(slug, workspaceDir, firstNonEmpty(channel...))
	env = setEnvValue(env, "WUPHF_HEADLESS_PROVIDER", "hermes")

	// Hermes reads its config and model credentials from the real user home
	// (~/.hermes/config.yaml, ~/.hermes/hermes-agent/). The Codex env builder
	// overrides HOME to an isolated sandbox — restore it so Hermes can find
	// its own config and API keys.
	if realHome, err := os.UserHomeDir(); err == nil && realHome != "" {
		env = setEnvValue(env, "HOME", realHome)
	}
	// Pass Hermes/OpenCode-Go credentials from the parent process environment
	// so the hermes subprocess can authenticate with the LLM provider.
	for _, key := range []string{
		"OPENCODE_GO_API_KEY",
		"OPENCODE_GO_BASE_URL",
		"OPENROUTER_API_KEY",
		"HERMES_PROVIDER_MODE",
		"TAVILY_API_KEY",
	} {
		if val := os.Getenv(key); val != "" {
			env = setEnvValue(env, key, val)
		}
	}
	// Use the "main" provider path: map OpenCode Go credentials to the
	// standard OPENAI_BASE_URL + OPENAI_API_KEY pair that Hermes's "main"
	// provider reads. This bypasses the aggregator routing logic that
	// otherwise falls back to OpenRouter for opencode-go.
	if val := os.Getenv("OPENCODE_GO_API_KEY"); val != "" {
		env = setEnvValue(env, "OPENAI_API_KEY", val)
	}
	env = setEnvValue(env, "OPENAI_BASE_URL", "https://opencode.ai/zen/go/v1")
	// Expose WUPHF broker credentials so the wuphf mcp-team MCP server
	// (configured in ~/.hermes/config.yaml) can authenticate with the broker.
	// WUPHF_MEMORY_BACKEND activates the team_wiki_write MCP tool when set to "markdown".
	if l.broker != nil {
		env = setEnvValue(env, "WUPHF_BROKER_TOKEN", l.broker.Token())
		env = setEnvValue(env, "WUPHF_BROKER_BASE_URL", l.BrokerBaseURL())
	}
	env = setEnvValue(env, "WUPHF_MEMORY_BACKEND", "markdown")
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("attach hermes stdout: %w", err)
	}

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	hermesStartedAt := time.Now()
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
		// Mark task blocked so the UI reflects the failure.
		if activeTaskID != "" {
			brokerTaskAction(brokerURL, brokerToken, activeTaskID, "block", "")
			appendHeadlessCodexLog(slug, "hermes_task_block: "+activeTaskID)
		}
		return fmt.Errorf("hermes: %s: %w", stderr, err)
	}

	// Persist the session ID so the next turn can resume with --resume.
	if newSessionID := latestHermesSessionID(hermesStartedAt); newSessionID != "" {
		saveHermesSessionID(slug, newSessionID)
		appendHeadlessCodexLog(slug, "hermes_session: "+newSessionID)
	}

	response := strings.Join(lines, "\n")
	text := strings.TrimSpace(response)
	// Mark task complete via broker so the Tasks UI stays accurate without
	// requiring the agent to spend an LLM call on team_task(action=complete).
	if activeTaskID != "" && text != "" {
		brokerTaskAction(brokerURL, brokerToken, activeTaskID, "complete", "")
		appendHeadlessCodexLog(slug, "hermes_task_complete: "+activeTaskID)
	}
	if text != "" {
		appendHeadlessCodexLog(slug, "hermes_result: "+text)
		target := firstNonEmpty(channel...)
		msg, posted, err := l.postHeadlessFinalMessageIfSilent(slug, target, notification, text, hermesStartedAt)
		if err != nil {
			appendHeadlessCodexLog(slug, "hermes_fallback-post-error: "+err.Error())
		} else if posted {
			appendHeadlessCodexLog(slug, fmt.Sprintf("hermes_fallback-post: posted final output to #%s as %s", msg.Channel, msg.ID))
		}
	}
	return nil
}
