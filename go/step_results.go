package agentkit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/eadwinCode/agent-kit/go/durable"
)

// ReplayPolicy is the public AgentKit replay contract. The zero value is
// ReplayMemoized; applications opt into a recompute policy only for reviewed,
// side-effect-free operations.
type ReplayPolicy = durable.ReplayPolicy

const (
	ReplayMemoized  = durable.ReplayMemoized
	ReplayRecompute = durable.ReplayRecompute
)

// MaxDurableStepResultBytes is the hard uncompressed storage limit. Results
// above the limit are never stored: AgentKit memoizes a bounded marker and
// recomputes the durable operation when the workflow driver replays it.
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

// StepResultRunQuery authenticates one complete run snapshot. A loader must
// return only results matching every field; AgentKit validates each returned
// result before making it available to replayed steps.
type StepResultRunQuery struct {
	Scope         SessionScope
	RunID         string
	SchemaVersion int
}

// StepResultRunLoader is the optional bulk-read extension to StepResultStore.
// Implementations should fetch the run in one storage operation. The map is
// keyed by StepResultLocator.StepID. AgentKit then resolves every reference in
// the current workflow callback from memory. A missing key always falls back to
// a fresh StepResultStore lookup, so bounded/partial snapshots and concurrent
// lost-ack recovery remain safe.
type StepResultRunLoader interface {
	LoadRun(ctx context.Context, query StepResultRunQuery) (map[string]StoredStepResult, error)
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
	return &stepResultStep{
		inner: inner, store: store, cfg: cfg,
		occurrences: map[string]uint64{},
	}, nil
}

type stepResultStep struct {
	inner       durable.Step
	store       StepResultStore
	cfg         StepResultStepConfig
	occurrences map[string]uint64
	loaded      bool
	loadedRows  map[string]StoredStepResult
}

func (s *stepResultStep) Run(ctx context.Context, id string, fn durable.RunFn) (json.RawMessage, error) {
	return s.RunWithOptions(ctx, id, durable.RunOptions{}, fn)
}

func (s *stepResultStep) RunWithOptions(ctx context.Context, id string, opts durable.RunOptions, fn durable.RunFn) (json.RawMessage, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	// The enclosing durable result already covers nested work. Persisting a row
	// for every collapsed nested Run would add unused references and DB writes.
	if durable.IsWithinStep(ctx) {
		return fn(ctx)
	}
	occurrence := s.occurrences[id]
	s.occurrences[id] = occurrence + 1
	locator := StepResultLocator{
		Scope: s.cfg.Scope, RunID: s.cfg.RunID,
		StepID:        qualifiedStepResultID(id, occurrence),
		SchemaVersion: s.cfg.SchemaVersion,
	}
	var local *StoredStepResult
	var localOversize json.RawMessage
	raw, err := s.inner.Run(ctx, id, func(stepCtx context.Context) (json.RawMessage, error) {
		stored, lookupErr := s.lookup(stepCtx, id, locator)
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
		if !json.Valid(payload) {
			return nil, fmt.Errorf("%w: durable step %q result is invalid JSON", ErrStepResultCorrupt, id)
		}
		if int64(len(payload)) > MaxDurableStepResultBytes {
			localOversize = append(json.RawMessage(nil), payload...)
			return marshalOversizeRecomputeEnvelope(int64(len(payload)), locator.SchemaVersion)
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
		s.cache(locator.StepID, stored)
		return marshalStepResultEnvelope(stored.Ref)
	})
	if err != nil {
		return nil, err
	}

	if _, recompute := unmarshalOversizeRecomputeEnvelope(raw, locator.SchemaVersion); recompute {
		if localOversize != nil {
			return append(json.RawMessage(nil), localOversize...), nil
		}
		payload, runErr := fn(ctx)
		if runErr != nil {
			return nil, runErr
		}
		if !json.Valid(payload) {
			return nil, fmt.Errorf("%w: recomputed durable step %q result is invalid JSON", ErrStepResultCorrupt, id)
		}
		return append(json.RawMessage(nil), payload...), nil
	}

	ref, referenced := unmarshalStepResultEnvelope(raw)
	if !referenced {
		// Compatibility with values memoized before reference writes shipped.
		return raw, nil
	}
	if local != nil && local.Ref == ref {
		return append(json.RawMessage(nil), local.Payload...), nil
	}
	payload, resolveErr := s.resolve(ctx, id, locator, ref)
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

// qualifiedStepResultID mirrors Inngest's positional step identity without
// changing the public step id passed to the workflow driver. Encoding the
// logical id makes the storage key unambiguous even when ids contain separators.
func qualifiedStepResultID(id string, occurrence uint64) string {
	return "v2:" + base64.RawURLEncoding.EncodeToString([]byte(id)) + ":" +
		strconv.FormatUint(occurrence, 10)
}

func (s *stepResultStep) loadRun(ctx context.Context) error {
	loader, ok := s.store.(StepResultRunLoader)
	if !ok || s.loaded {
		return nil
	}
	rows, err := loader.LoadRun(ctx, StepResultRunQuery{
		Scope: s.cfg.Scope, RunID: s.cfg.RunID, SchemaVersion: s.cfg.SchemaVersion,
	})
	if err != nil {
		return fmt.Errorf("agentkit: load durable results for run %q: %w", s.cfg.RunID, err)
	}
	loaded := make(map[string]StoredStepResult, len(rows))
	for stepID, stored := range rows {
		if strings.TrimSpace(stepID) == "" {
			return fmt.Errorf("%w: run %q returned an empty durable step id", ErrStepResultCorrupt, s.cfg.RunID)
		}
		locator := StepResultLocator{
			Scope: s.cfg.Scope, RunID: s.cfg.RunID, StepID: stepID,
			SchemaVersion: s.cfg.SchemaVersion,
		}
		if err := validateStoredStepResult(stepID, locator, stored); err != nil {
			return err
		}
		loaded[stepID] = cloneStepResultValue(stored)
	}
	s.loadedRows, s.loaded = loaded, true
	return nil
}

func (s *stepResultStep) lookup(ctx context.Context, logicalID string, locator StepResultLocator) (StoredStepResult, error) {
	if _, ok := s.store.(StepResultRunLoader); !ok {
		return s.store.Lookup(ctx, locator)
	}
	if err := s.loadRun(ctx); err != nil {
		return StoredStepResult{}, err
	}
	stored, ok := s.loadedRows[locator.StepID]
	if !ok {
		stored, err := s.store.Lookup(ctx, locator)
		if err != nil {
			return StoredStepResult{}, err
		}
		if err := validateStoredStepResult(logicalID, locator, stored); err != nil {
			return StoredStepResult{}, err
		}
		s.cache(locator.StepID, stored)
		return cloneStepResultValue(stored), nil
	}
	if err := validateStoredStepResult(logicalID, locator, stored); err != nil {
		return StoredStepResult{}, err
	}
	return cloneStepResultValue(stored), nil
}

func (s *stepResultStep) resolve(ctx context.Context, logicalID string, locator StepResultLocator, ref StepResultRef) (json.RawMessage, error) {
	payload, err := s.resolveLocator(ctx, logicalID, locator, ref)
	if err == nil || !errors.Is(err, ErrStepResultNotFound) {
		return payload, err
	}
	// Reference envelopes memoized before occurrence-qualified locators shipped
	// still point at the legacy bare logical step id. Resolve that exact reference
	// without using the legacy row for new lookup-before-work decisions.
	legacy := locator
	legacy.StepID = logicalID
	return s.store.Resolve(ctx, legacy, ref)
}

func (s *stepResultStep) resolveLocator(ctx context.Context, logicalID string, locator StepResultLocator, ref StepResultRef) (json.RawMessage, error) {
	if _, ok := s.store.(StepResultRunLoader); !ok {
		return s.store.Resolve(ctx, locator, ref)
	}
	stored, err := s.lookup(ctx, logicalID, locator)
	if err != nil {
		return nil, err
	}
	if stored.Ref != ref {
		return nil, ErrStepResultUnauthorized
	}
	return append(json.RawMessage(nil), stored.Payload...), nil
}

func (s *stepResultStep) cache(stepID string, stored StoredStepResult) {
	if _, ok := s.store.(StepResultRunLoader); !ok || !s.loaded {
		return
	}
	s.loadedRows[stepID] = cloneStepResultValue(stored)
}

func cloneStepResultValue(stored StoredStepResult) StoredStepResult {
	stored.Payload = append(json.RawMessage(nil), stored.Payload...)
	return stored
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

func marshalOversizeRecomputeEnvelope(sizeBytes int64, schemaVersion int) (json.RawMessage, error) {
	return json.Marshal(stepResultEnvelope{Result: stepResultEnvelopeBody{
		Mode: "recompute_oversize", ReferenceVersion: stepResultReferenceVersion,
		SizeBytes: sizeBytes, PayloadSchemaVersion: schemaVersion,
	}})
}

func unmarshalOversizeRecomputeEnvelope(raw json.RawMessage, schemaVersion int) (int64, bool) {
	var outer map[string]json.RawMessage
	if json.Unmarshal(raw, &outer) != nil || len(outer) != 1 {
		return 0, false
	}
	body, ok := outer["_agentkitStepResult"]
	if !ok {
		return 0, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope stepResultEnvelopeBody
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		envelope.Mode != "recompute_oversize" || envelope.ReferenceVersion != stepResultReferenceVersion ||
		envelope.Ref != "" || envelope.SHA256 != "" || envelope.SizeBytes <= MaxDurableStepResultBytes ||
		envelope.PayloadSchemaVersion != schemaVersion {
		return 0, false
	}
	return envelope.SizeBytes, true
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
