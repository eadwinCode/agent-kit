package agentkit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/eadwinCode/agent-kit/go/durable"
)

// ReplayPolicy is the public AgentKit replay contract. The zero value is
// ReplayMemoized; applications opt into ReplayRecompute only for reviewed,
// side-effect-free operations.
type ReplayPolicy = durable.ReplayPolicy

const (
	ReplayMemoized  = durable.ReplayMemoized
	ReplayRecompute = durable.ReplayRecompute
)

// MaxDurableStepResultBytes is the hard uncompressed result limit. AgentKit
// neither compresses an oversize payload nor silently changes its replay
// policy; a memoized result above this limit fails closed.
const MaxDurableStepResultBytes int64 = 2 << 20

const stepResultReferenceVersion = 1

var (
	ErrStepResultNotFound     = errors.New("agentkit: step result not found")
	ErrStepResultConflict     = errors.New("agentkit: step result conflict")
	ErrStepResultCorrupt      = errors.New("agentkit: step result corrupt")
	ErrStepResultUnavailable  = errors.New("agentkit: step result unavailable")
	ErrStepResultUnauthorized = errors.New("agentkit: step result unauthorized")
	ErrStepResultTooLarge     = errors.New("agentkit: step result too large")
)

// StepResultTooLargeError reports the rejected byte count without exposing the
// result. It unwraps to ErrStepResultTooLarge for stable classification.
type StepResultTooLargeError struct {
	StepID    string
	SizeBytes int64
	MaxBytes  int64
}

func (e *StepResultTooLargeError) Error() string {
	return fmt.Sprintf("agentkit: durable step %q result is %d bytes; maximum is %d: %v",
		e.StepID, e.SizeBytes, e.MaxBytes, ErrStepResultTooLarge)
}

func (e *StepResultTooLargeError) Unwrap() error { return ErrStepResultTooLarge }

// StepResultLocator is the immutable logical key for one memoized result.
// Scope is trusted application context and is never included in the Inngest
// reference envelope.
type StepResultLocator struct {
	Scope         SessionScope `json:"-"`
	RunID         string       `json:"runId"`
	StepID        string       `json:"stepId"`
	SchemaVersion int          `json:"schemaVersion"`
}

// StepResultRef is the bounded value memoized by Inngest. Ref is an opaque
// application-owned identifier; it is unusable without the trusted locator.
type StepResultRef struct {
	Ref           string `json:"ref"`
	SHA256        string `json:"sha256"`
	SizeBytes     int64  `json:"sizeBytes"`
	SchemaVersion int    `json:"schemaVersion"`
}

// StoredStepResult contains the authoritative uncompressed JSON and its
// bounded reference metadata.
type StoredStepResult struct {
	Ref     StepResultRef
	Payload json.RawMessage
}

// StepResultStore is implemented by the embedding application. Implementations
// must store Payload exactly as supplied: no compression and no lossy rewrite.
// Lookup and Put are scoped by the complete locator; Resolve must reauthorize
// both locator and reference.
type StepResultStore interface {
	Lookup(ctx context.Context, locator StepResultLocator) (StoredStepResult, error)
	Put(ctx context.Context, locator StepResultLocator, payload json.RawMessage) (StoredStepResult, error)
	Resolve(ctx context.Context, locator StepResultLocator, ref StepResultRef) (json.RawMessage, error)
}

// StepResultStepConfig binds a storage adapter to one replay-stable run.
type StepResultStepConfig struct {
	Scope         SessionScope
	RunID         string
	SchemaVersion int
}

// NewStepResultStep wraps an existing durability seam. The wrapper stores the
// exact result through StepResultStore inside the original durable step and
// gives Inngest only a small reference envelope. Existing inline results remain
// readable, which makes reference writes independently rollbackable.
func NewStepResultStep(inner durable.Step, store StepResultStore, cfg StepResultStepConfig) (durable.Step, error) {
	if inner == nil {
		inner = durable.Inngest()
	}
	if store == nil {
		return nil, fmt.Errorf("agentkit: step result store is required")
	}
	if cfg.Scope.IsZero() {
		return nil, fmt.Errorf("agentkit: step result scope is required")
	}
	cfg.RunID = strings.TrimSpace(cfg.RunID)
	if cfg.RunID == "" {
		return nil, fmt.Errorf("agentkit: step result run id is required")
	}
	if cfg.SchemaVersion == 0 {
		cfg.SchemaVersion = 1
	}
	if cfg.SchemaVersion < 0 {
		return nil, fmt.Errorf("agentkit: step result schema version must be positive")
	}
	return &stepResultStep{inner: inner, store: store, cfg: cfg}, nil
}

type stepResultStep struct {
	inner durable.Step
	store StepResultStore
	cfg   StepResultStepConfig
}

func (s *stepResultStep) Run(ctx context.Context, id string, fn durable.RunFn) (json.RawMessage, error) {
	// The enclosing durable result already covers nested work. Persisting a row
	// for every collapsed nested Run would add unused references and DB writes.
	if durable.IsWithinStep(ctx) {
		return fn(ctx)
	}
	locator := StepResultLocator{
		Scope: s.cfg.Scope, RunID: s.cfg.RunID, StepID: id,
		SchemaVersion: s.cfg.SchemaVersion,
	}
	var local *StoredStepResult
	raw, err := s.inner.Run(ctx, id, func(stepCtx context.Context) (json.RawMessage, error) {
		stored, lookupErr := s.store.Lookup(stepCtx, locator)
		switch {
		case lookupErr == nil:
			if err := validateStoredStepResult(id, locator, stored); err != nil {
				return nil, err
			}
			local = &stored
			return marshalStepResultEnvelope(stored.Ref)
		case !errors.Is(lookupErr, ErrStepResultNotFound):
			return nil, fmt.Errorf("agentkit: lookup durable step %q result: %w", id, lookupErr)
		}

		payload, runErr := fn(stepCtx)
		if runErr != nil {
			return nil, runErr
		}
		if err := validateStepResultPayload(id, payload); err != nil {
			return nil, err
		}
		stored, putErr := s.store.Put(stepCtx, locator, append(json.RawMessage(nil), payload...))
		if putErr != nil {
			return nil, fmt.Errorf("agentkit: store durable step %q result: %w", id, putErr)
		}
		if err := validateStoredStepResult(id, locator, stored); err != nil {
			return nil, err
		}
		want := checksumStepResult(payload)
		if stored.Ref.SHA256 != want || !bytes.Equal(stored.Payload, payload) {
			return nil, fmt.Errorf("%w: durable step %q store changed payload", ErrStepResultConflict, id)
		}
		local = &stored
		return marshalStepResultEnvelope(stored.Ref)
	})
	if err != nil {
		return nil, err
	}

	ref, referenced := unmarshalStepResultEnvelope(raw)
	if !referenced {
		// Compatibility with values memoized before reference writes shipped.
		return raw, nil
	}
	if local != nil && local.Ref == ref {
		return append(json.RawMessage(nil), local.Payload...), nil
	}
	payload, resolveErr := s.store.Resolve(ctx, locator, ref)
	if resolveErr != nil {
		if errors.Is(resolveErr, ErrStepResultNotFound) {
			return nil, fmt.Errorf("%w: durable step %q reference is missing", ErrStepResultCorrupt, id)
		}
		return nil, fmt.Errorf("agentkit: resolve durable step %q result: %w", id, resolveErr)
	}
	if err := validateResolvedStepResult(id, locator, ref, payload); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), payload...), nil
}

type stepResultEnvelope struct {
	Result stepResultEnvelopeBody `json:"_agentkitStepResult"`
}

type stepResultEnvelopeBody struct {
	Mode                 string `json:"mode"`
	ReferenceVersion     int    `json:"referenceVersion"`
	Ref                  string `json:"ref"`
	SHA256               string `json:"sha256"`
	SizeBytes            int64  `json:"sizeBytes"`
	PayloadSchemaVersion int    `json:"payloadSchemaVersion"`
}

func marshalStepResultEnvelope(ref StepResultRef) (json.RawMessage, error) {
	return json.Marshal(stepResultEnvelope{Result: stepResultEnvelopeBody{
		Mode: "reference", ReferenceVersion: stepResultReferenceVersion,
		Ref: ref.Ref, SHA256: ref.SHA256, SizeBytes: ref.SizeBytes,
		PayloadSchemaVersion: ref.SchemaVersion,
	}})
}

// unmarshalStepResultEnvelope treats an incomplete/unknown marker as ordinary
// legacy JSON. Only the complete reserved shape activates store resolution.
func unmarshalStepResultEnvelope(raw json.RawMessage) (StepResultRef, bool) {
	var outer map[string]json.RawMessage
	if json.Unmarshal(raw, &outer) != nil || len(outer) != 1 {
		return StepResultRef{}, false
	}
	body, ok := outer["_agentkitStepResult"]
	if !ok {
		return StepResultRef{}, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope stepResultEnvelopeBody
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		envelope.Mode != "reference" || envelope.ReferenceVersion != stepResultReferenceVersion ||
		envelope.Ref == "" || envelope.SizeBytes < 0 || envelope.SizeBytes > MaxDurableStepResultBytes ||
		envelope.PayloadSchemaVersion <= 0 || !validSHA256(envelope.SHA256) {
		return StepResultRef{}, false
	}
	return StepResultRef{
		Ref: envelope.Ref, SHA256: envelope.SHA256, SizeBytes: envelope.SizeBytes,
		SchemaVersion: envelope.PayloadSchemaVersion,
	}, true
}

func validateStoredStepResult(id string, locator StepResultLocator, stored StoredStepResult) error {
	return validateResolvedStepResult(id, locator, stored.Ref, stored.Payload)
}

func validateResolvedStepResult(id string, locator StepResultLocator, ref StepResultRef, payload json.RawMessage) error {
	if ref.Ref == "" || ref.SchemaVersion != locator.SchemaVersion ||
		ref.SizeBytes != int64(len(payload)) || !validSHA256(ref.SHA256) ||
		ref.SHA256 != checksumStepResult(payload) {
		return fmt.Errorf("%w: durable step %q result metadata mismatch", ErrStepResultCorrupt, id)
	}
	return validateStepResultPayload(id, payload)
}

func validateStepResultPayload(id string, payload json.RawMessage) error {
	if int64(len(payload)) > MaxDurableStepResultBytes {
		return &StepResultTooLargeError{
			StepID: id, SizeBytes: int64(len(payload)), MaxBytes: MaxDurableStepResultBytes,
		}
	}
	if !json.Valid(payload) {
		return fmt.Errorf("%w: durable step %q result is invalid JSON", ErrStepResultCorrupt, id)
	}
	return nil
}

func checksumStepResult(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
