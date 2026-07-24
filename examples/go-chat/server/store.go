package main

// SQLite-backed conversation storage, exposed to agent-kit through
// HistoryConfig — the framework's own persistence seam. AgentKit calls
// these hooks itself (CreateThread before the run, AppendUserMessage up
// front, Get to hydrate, AppendResults as results land), so the HTTP layer
// never assembles conversation context by hand.
//
// Why user turns are stored as AgentResults: an AgentResult carries only
// assistant output and tool results, never the user turn that prompted it.
// Loading results alone leaves the model reading its own answers with the
// questions missing. State.FormatHistory flattens each result's Output into
// the message list verbatim, so a user turn round-trips correctly as a
// result whose Output is a single user-role text message. That keeps the
// whole transcript inside the history layer instead of a parallel seed.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo

	agentkit "github.com/eadwinCode/agent-kit/go"
)

const schema = `
CREATE TABLE IF NOT EXISTS threads (
  id         TEXT PRIMARY KEY,
  title      TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
  id         TEXT NOT NULL,
  thread_id  TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL,
  role       TEXT NOT NULL,          -- 'user' | 'assistant'
  content    TEXT NOT NULL DEFAULT '',
  agent_name TEXT NOT NULL DEFAULT '',
  output     TEXT NOT NULL DEFAULT '[]',
  tool_calls TEXT NOT NULL DEFAULT '[]',
  checksum   TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMP NOT NULL,
  -- Token usage, lifted out of AgentResult.Raw when the result is stored.
  input_tokens  INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read    INTEGER NOT NULL DEFAULT 0,
  cache_write   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (thread_id, id)
);

CREATE INDEX IF NOT EXISTS messages_thread_seq ON messages(thread_id, seq);
`

// userAgentName marks history rows that represent a user turn.
const userAgentName = "user"

type store struct{ db *sql.DB }

func openStore(path string) (*store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	// SQLite has no ADD COLUMN IF NOT EXISTS; add usage columns to
	// databases created before they existed.
	for _, col := range []string{
		"input_tokens INTEGER NOT NULL DEFAULT 0",
		"output_tokens INTEGER NOT NULL DEFAULT 0",
		"cache_read INTEGER NOT NULL DEFAULT 0",
		"cache_write INTEGER NOT NULL DEFAULT 0",
	} {
		// Duplicate-column errors mean the migration already ran.
		_, _ = db.Exec("ALTER TABLE messages ADD COLUMN " + col)
	}
	return &store{db: db}, nil
}

func (s *store) Close() error { return s.db.Close() }

// --- thread queries used by the HTTP layer ---

type threadRow struct {
	ID        string
	Title     string
	Messages  int
	CreatedAt time.Time
	UpdatedAt time.Time
	Usage     usage
}

func (s *store) listThreads(ctx context.Context) ([]threadRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.title, t.created_at, t.updated_at,
		       (SELECT COUNT(*)                    FROM messages m WHERE m.thread_id = t.id),
		       (SELECT COALESCE(SUM(input_tokens), 0)  FROM messages m WHERE m.thread_id = t.id),
		       (SELECT COALESCE(SUM(output_tokens), 0) FROM messages m WHERE m.thread_id = t.id),
		       (SELECT COALESCE(SUM(cache_read), 0)    FROM messages m WHERE m.thread_id = t.id),
		       (SELECT COALESCE(SUM(cache_write), 0)   FROM messages m WHERE m.thread_id = t.id)
		FROM threads t ORDER BY t.updated_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []threadRow
	for rows.Next() {
		var t threadRow
		if err := rows.Scan(&t.ID, &t.Title, &t.CreatedAt, &t.UpdatedAt, &t.Messages,
			&t.Usage.InputTokens, &t.Usage.OutputTokens, &t.Usage.CacheRead, &t.Usage.CacheWrite); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *store) createThread(ctx context.Context, id string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO threads (id, title, created_at, updated_at) VALUES (?, '', ?, ?)
		ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at`, id, now, now)
	return err
}

func (s *store) deleteThread(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE thread_id = ?`, id); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, id)
	return err
}

func (s *store) setTitle(ctx context.Context, threadID, title string) error {
	if title == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE threads SET title = ?, updated_at = ? WHERE id = ?`,
		title, time.Now().UTC(), threadID)
	return err
}

// --- message rows ---

// usage is the token accounting for one inference, mirroring the
// snake_case keys agent-kit writes into AgentResult.Raw.
type usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	CacheRead    int `json:"cache_read_input_tokens"`
	CacheWrite   int `json:"cache_creation_input_tokens"`
}

// usageOf pulls token counts out of a result's Raw payload. Raw is the
// serialized inference response, so this is the only place the numbers
// exist — they are not on AgentResult itself.
func usageOf(r *agentkit.AgentResult) usage {
	if r.Raw == "" {
		return usage{}
	}
	var raw agentkit.SerializableResult
	if err := json.Unmarshal([]byte(r.Raw), &raw); err != nil || raw.Usage == nil {
		return usage{}
	}
	return usage{
		InputTokens:  raw.Usage.InputTokens,
		OutputTokens: raw.Usage.OutputTokens,
		CacheRead:    raw.Usage.CacheReadInputTokens,
		CacheWrite:   raw.Usage.CacheCreationInputTokens,
	}
}

type messageRow struct {
	ID        string
	Role      string
	Content   string
	AgentName string
	Output    []agentkit.Message
	ToolCalls []agentkit.Message
	CreatedAt time.Time
	Usage     usage
}

func (s *store) messages(ctx context.Context, threadID string) ([]messageRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, role, content, agent_name, output, tool_calls, created_at,
		       input_tokens, output_tokens, cache_read, cache_write
		FROM messages WHERE thread_id = ? ORDER BY seq ASC`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []messageRow
	for rows.Next() {
		var m messageRow
		var outJSON, tcJSON string
		if err := rows.Scan(&m.ID, &m.Role, &m.Content, &m.AgentName, &outJSON, &tcJSON, &m.CreatedAt,
			&m.Usage.InputTokens, &m.Usage.OutputTokens, &m.Usage.CacheRead, &m.Usage.CacheWrite); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(outJSON), &m.Output); err != nil {
			return nil, fmt.Errorf("decode output for %s: %w", m.ID, err)
		}
		if err := json.Unmarshal([]byte(tcJSON), &m.ToolCalls); err != nil {
			return nil, fmt.Errorf("decode toolCalls for %s: %w", m.ID, err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// insertMessage appends a row, ignoring replays of one already stored.
// AgentKit calls AppendResults both incrementally and as an end-of-run
// backstop, so this must be idempotent; the replay-stable AgentResult id
// is the dedupe key.
func (s *store) insertMessage(ctx context.Context, threadID string, m messageRow, checksum string) error {
	outJSON, err := json.Marshal(m.Output)
	if err != nil {
		return err
	}
	tcJSON, err := json.Marshal(m.ToolCalls)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	var seq int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE thread_id = ?`, threadID).Scan(&seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO messages (id, thread_id, seq, role, content, agent_name, output, tool_calls,
		                      checksum, created_at, input_tokens, output_tokens, cache_read, cache_write)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(thread_id, id) DO NOTHING`,
		m.ID, threadID, seq, m.Role, m.Content, m.AgentName,
		string(outJSON), string(tcJSON), checksum, m.CreatedAt.UTC(),
		m.Usage.InputTokens, m.Usage.OutputTokens, m.Usage.CacheRead, m.Usage.CacheWrite); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE threads SET updated_at = ? WHERE id = ?`, time.Now().UTC(), threadID); err != nil {
		return err
	}
	return tx.Commit()
}

// --- agent-kit history layer ---

// historyConfig wires the store into agent-kit. Passing this to
// NetworkConfig.History is all it takes: the framework then creates the
// thread, persists the user's turn, hydrates prior context, and saves new
// results — each inside its own durable step.
func (s *store) historyConfig() *agentkit.HistoryConfig[chatState] {
	return &agentkit.HistoryConfig[chatState]{
		CreateThread: func(ctx context.Context, hctx agentkit.HistoryContext[chatState]) (agentkit.CreateThreadResult, error) {
			id := hctx.State.ThreadID
			if id == "" {
				id = newID()
			}
			if err := s.createThread(ctx, id); err != nil {
				return agentkit.CreateThreadResult{}, err
			}
			return agentkit.CreateThreadResult{ThreadID: id}, nil
		},

		AppendUserMessage: func(ctx context.Context, hctx agentkit.HistoryContext[chatState], msg agentkit.UserMessageRecord) error {
			return s.insertMessage(ctx, hctx.ThreadID, messageRow{
				ID:        msg.ID,
				Role:      string(agentkit.RoleUser),
				Content:   msg.Content,
				AgentName: userAgentName,
				Output:    []agentkit.Message{userMessage(msg.Content)},
				CreatedAt: msg.Timestamp.Time,
			}, "")
		},

		Get: func(ctx context.Context, hctx agentkit.HistoryContext[chatState]) ([]*agentkit.AgentResult, error) {
			rows, err := s.messages(ctx, hctx.ThreadID)
			if err != nil {
				return nil, err
			}
			results := make([]*agentkit.AgentResult, 0, len(rows))
			for _, r := range rows {
				res := agentkit.NewAgentResult(
					r.AgentName, r.Output, r.ToolCalls, agentkit.Time{Time: r.CreatedAt})
				res.ID = r.ID
				results = append(results, res)
			}
			return results, nil
		},

		AppendResults: func(ctx context.Context, hctx agentkit.HistoryContext[chatState], newResults []*agentkit.AgentResult) error {
			for _, r := range newResults {
				sum, err := r.Checksum()
				if err != nil {
					return err
				}
				if err := s.insertMessage(ctx, hctx.ThreadID, messageRow{
					ID:        r.ID,
					Role:      string(agentkit.RoleAssistant),
					AgentName: r.AgentName,
					Output:    r.Output,
					ToolCalls: r.ToolCalls,
					CreatedAt: r.CreatedAt.Time,
					Usage:     usageOf(r),
				}, sum); err != nil {
					return err
				}
			}
			// The title is set by a tool mid-run, so persist it alongside
			// the results it was produced with.
			return s.setTitle(ctx, hctx.ThreadID, hctx.State.Data.Title)
		},
	}
}

// userMessage builds the single user-role text message that represents a
// user turn inside an AgentResult (see the file comment).
func userMessage(content string) agentkit.Message {
	return agentkit.Message{
		Type:    agentkit.MessageText,
		Role:    agentkit.RoleUser,
		Content: agentkit.TextContent(content),
	}
}
