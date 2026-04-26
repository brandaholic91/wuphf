package provider

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nex-crm/wuphf/internal/agent"
	"github.com/nex-crm/wuphf/internal/runtimebin"
)

var (
	hermesLookPath = runtimebin.LookPath
	hermesCommand  = exec.Command
	hermesGetwd    = os.Getwd
)

func init() {
	Register(&Entry{
		Kind:     KindHermes,
		StreamFn: CreateHermesCLIStreamFn,
		OneShot:  RunHermesOneShot,
		Capabilities: Capabilities{
			PaneEligible:    false,
			SupportsOneShot: true,
		},
	})
}

// CreateHermesCLIStreamFn returns a StreamFn that runs the Hermes CLI
// non-interactively. Each invocation is ephemeral: WUPHF owns the conversation
// history and hands Hermes a fresh prompt every turn.
//
// Hermes emits plain text on stdout (no JSONL surface), so we stream stdout
// line-by-line as text chunks rather than parsing structured events.
func CreateHermesCLIStreamFn(agentSlug string) agent.StreamFn {
	return func(msgs []agent.Message, tools []agent.AgentTool) <-chan agent.StreamChunk {
		ch := make(chan agent.StreamChunk, 64)
		go func() {
			defer close(ch)

			if _, err := hermesLookPath("hermes"); err != nil {
				ch <- agent.StreamChunk{Type: "error", Content: "Hermes CLI not found. Install hermes or use /provider to choose a different provider."}
				return
			}

			cwd, err := hermesGetwd()
			if err != nil {
				ch <- agent.StreamChunk{Type: "error", Content: fmt.Sprintf("resolve working directory: %v", err)}
				return
			}

			systemPrompt, prompt := buildClaudePrompts(msgs)
			if prompt == "" {
				prompt = "Proceed with the task."
			}

			startedAt := time.Now()
			var firstEventAt time.Time
			var firstTextAt time.Time
			text, err := runHermesOnce(systemPrompt, prompt, cwd, func(line string) {
				if firstEventAt.IsZero() {
					firstEventAt = time.Now()
				}
				if strings.TrimSpace(line) == "" {
					return
				}
				if firstTextAt.IsZero() {
					firstTextAt = time.Now()
				}
				ch <- agent.StreamChunk{Type: "text", Content: line}
			})
			if err != nil {
				appendHermesLatencyLog(agentSlug, fmt.Sprintf("status=error total_ms=%d first_event_ms=%d first_text_ms=%d detail=%q",
					time.Since(startedAt).Milliseconds(),
					durationMillis(startedAt, firstEventAt),
					durationMillis(startedAt, firstTextAt),
					err.Error(),
				))
				ch <- agent.StreamChunk{Type: "error", Content: describeHermesFailure(err)}
				return
			}
			appendHermesLatencyLog(agentSlug, fmt.Sprintf("status=ok total_ms=%d first_event_ms=%d first_text_ms=%d final_chars=%d",
				time.Since(startedAt).Milliseconds(),
				durationMillis(startedAt, firstEventAt),
				durationMillis(startedAt, firstTextAt),
				len(text),
			))
			if firstTextAt.IsZero() && strings.TrimSpace(text) != "" {
				streamTextChunks(ch, text)
			}
		}()
		return ch
	}
}

// RunHermesOneShot runs Hermes once with the given system prompt and user
// prompt and returns the final plain-text result.
func RunHermesOneShot(systemPrompt, prompt, cwd string) (string, error) {
	if cwd == "" {
		var err error
		cwd, err = hermesGetwd()
		if err != nil {
			return "", err
		}
	}
	return runHermesOnce(systemPrompt, prompt, cwd, nil)
}

// BuildHermesPromptExported is exported for use by internal/team/headless_hermes.go.
func BuildHermesPromptExported(systemPrompt, prompt string) string {
	return buildHermesPrompt(systemPrompt, prompt)
}

// runHermesOnce invokes `hermes -z <prompt>` with the caller's prompt as the
// positional argument, streams plain stdout lines via onLine (if provided),
// and returns the full concatenated output.
func runHermesOnce(systemPrompt, prompt, cwd string, onLine func(string)) (string, error) {
	if _, err := hermesLookPath("hermes"); err != nil {
		return "", fmt.Errorf("hermes binary not found in PATH: %w", err)
	}

	fullPrompt := buildHermesPrompt(systemPrompt, prompt)
	cmd := hermesCommand("hermes", "-z", fullPrompt)
	cmd.Dir = cwd

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("hermes stdout pipe: %w", err)
	}

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start hermes: %w", err)
	}

	var sb strings.Builder
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		sb.WriteString(line)
		sb.WriteByte('\n')
		if onLine != nil {
			onLine(line)
		}
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return sb.String(), fmt.Errorf("hermes output line exceeded 4 MiB buffer: %w", err)
		}
		return sb.String(), fmt.Errorf("read hermes stream: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		stderr := strings.TrimSpace(stderrBuf.String())
		return sb.String(), fmt.Errorf("hermes exited with error (%s): %w", stderr, err)
	}
	return sb.String(), nil
}

// buildHermesPrompt concatenates system and user text for delivery as a single
// positional argument to `hermes -z`. Any literal <system>/</system> tokens
// inside content are neutralised with a zero-width space.
func buildHermesPrompt(systemPrompt, prompt string) string {
	var parts []string
	if s := strings.TrimSpace(systemPrompt); s != "" {
		escaped := strings.ReplaceAll(s, "</system>", "</​system>")
		escaped = strings.ReplaceAll(escaped, "<system>", "<​system>")
		parts = append(parts, "<system>\n"+escaped+"\n</system>")
	}
	if p := strings.TrimSpace(prompt); p != "" {
		parts = append(parts, p)
	}
	return strings.Join(parts, "\n\n")
}

func describeHermesFailure(err error) string {
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(text, "login") || strings.Contains(text, "auth") || strings.Contains(text, "unauthorized") || strings.Contains(text, "api key") {
		return "Hermes CLI is not authenticated. Configure your Hermes credentials or use /provider to choose a different provider."
	}
	if strings.Contains(text, "exceeded 4 mib") {
		return "Hermes produced a stream line larger than 4 MiB; aborted. Retry with a smaller response."
	}
	return fmt.Sprintf("hermes exited with error: %v", err)
}

func appendHermesLatencyLog(agentSlug string, line string) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	logDir := filepath.Join(home, ".wuphf", "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return
	}
	path := filepath.Join(logDir, "hermes-latency.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = fmt.Fprintf(f, "[%s] agent=%s %s\n", time.Now().Format(time.RFC3339), strings.TrimSpace(agentSlug), strings.TrimSpace(line))
}
