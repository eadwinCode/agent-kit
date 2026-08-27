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
	rows map[string]agentkit.StoredStepResult
}

// NewStepResults creates an empty result store.
func NewStepResults() *StepResults {
	return &StepResults{rows: map[string]agentkit.StoredStepResult{}}
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
	stored, ok := s.rows[stepResultKey(locator)]
	if !ok {
		return agentkit.StoredStepResult{}, agentkit.ErrStepResultNotFound
	}
	return cloneStepResult(stored), nil
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
	if existing, ok := s.rows[key]; ok {
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
	s.rows[key] = stored
	return cloneStepResult(stored), nil
}

func (s *StepResults) Resolve(_ context.Context, locator agentkit.StepResultLocator, ref agentkit.StepResultRef) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored, ok := s.rows[stepResultKey(locator)]
	if !ok {
		return nil, agentkit.ErrStepResultNotFound
	}
	if stored.Ref != ref {
		return nil, agentkit.ErrStepResultUnauthorized
	}
	return append(json.RawMessage(nil), stored.Payload...), nil
}

var _ agentkit.StepResultStore = (*StepResults)(nil)
