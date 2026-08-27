package memadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

// StepResults is an in-memory conformance implementation of
// agentkit.StepResultStore. It stores exact, uncompressed JSON bytes and is
// safe for concurrent tests and local examples.
type StepResults struct {
	mu   sync.RWMutex
	rows map[string]stepResultRow
}

type stepResultRow struct {
	locator agentkit.StepResultLocator
	stored  agentkit.StoredStepResult
}

// NewStepResults creates an empty result store.
func NewStepResults() *StepResults {
	return &StepResults{rows: map[string]stepResultRow{}}
}

func stepResultKey(locator agentkit.StepResultLocator) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", locator.Scope, locator.RunID, locator.StepID, locator.SchemaVersion)
}

func stepResultChecksum(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func cloneStepResult(stored agentkit.StoredStepResult) agentkit.StoredStepResult {
	stored.Payload = append(json.RawMessage(nil), stored.Payload...)
	return stored
}

func (s *StepResults) Lookup(_ context.Context, locator agentkit.StepResultLocator) (agentkit.StoredStepResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row, ok := s.rows[stepResultKey(locator)]
	if !ok {
		return agentkit.StoredStepResult{}, agentkit.ErrStepResultNotFound
	}
	return cloneStepResult(row.stored), nil
}

func (s *StepResults) Put(_ context.Context, locator agentkit.StepResultLocator, payload json.RawMessage) (agentkit.StoredStepResult, error) {
	if locator.Scope.IsZero() || locator.RunID == "" || locator.StepID == "" || locator.SchemaVersion <= 0 {
		return agentkit.StoredStepResult{}, fmt.Errorf("%w: incomplete locator", agentkit.ErrStepResultUnauthorized)
	}
	if int64(len(payload)) > agentkit.MaxDurableStepResultBytes {
		return agentkit.StoredStepResult{}, &agentkit.StepResultTooLargeError{
			StepID: locator.StepID, SizeBytes: int64(len(payload)), MaxBytes: agentkit.MaxDurableStepResultBytes,
		}
	}
	if !json.Valid(payload) {
		return agentkit.StoredStepResult{}, fmt.Errorf("%w: invalid JSON", agentkit.ErrStepResultCorrupt)
	}
	key := stepResultKey(locator)
	checksum := stepResultChecksum(payload)
	s.mu.Lock()
	defer s.mu.Unlock()
	if row, ok := s.rows[key]; ok {
		existing := row.stored
		if existing.Ref.SHA256 != checksum || !bytes.Equal(existing.Payload, payload) {
			return agentkit.StoredStepResult{}, agentkit.ErrStepResultConflict
		}
		return cloneStepResult(existing), nil
	}
	refSum := sha256.Sum256([]byte(key))
	stored := agentkit.StoredStepResult{
		Ref: agentkit.StepResultRef{
			Ref: hex.EncodeToString(refSum[:]), SHA256: checksum,
			SizeBytes: int64(len(payload)), SchemaVersion: locator.SchemaVersion,
		},
		Payload: append(json.RawMessage(nil), payload...),
	}
	s.rows[key] = stepResultRow{locator: locator, stored: stored}
	return cloneStepResult(stored), nil
}

func (s *StepResults) Resolve(_ context.Context, locator agentkit.StepResultLocator, ref agentkit.StepResultRef) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row, ok := s.rows[stepResultKey(locator)]
	if !ok {
		return nil, agentkit.ErrStepResultNotFound
	}
	stored := row.stored
	if stored.Ref != ref {
		return nil, agentkit.ErrStepResultUnauthorized
	}
	return append(json.RawMessage(nil), stored.Payload...), nil
}

// LoadRun implements AgentKit's optional bulk replay path. It returns a cloned
// snapshot so callers cannot mutate the authoritative in-memory rows.
func (s *StepResults) LoadRun(_ context.Context, query agentkit.StepResultRunQuery) (map[string]agentkit.StoredStepResult, error) {
	if query.Scope.IsZero() || query.RunID == "" || query.SchemaVersion <= 0 {
		return nil, fmt.Errorf("%w: incomplete run query", agentkit.ErrStepResultUnauthorized)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := make(map[string]agentkit.StoredStepResult)
	for _, row := range s.rows {
		if row.locator.Scope != query.Scope || row.locator.RunID != query.RunID ||
			row.locator.SchemaVersion != query.SchemaVersion {
			continue
		}
		results[row.locator.StepID] = cloneStepResult(row.stored)
	}
	return results, nil
}

var _ agentkit.StepResultStore = (*StepResults)(nil)
var _ agentkit.StepResultRunLoader = (*StepResults)(nil)
