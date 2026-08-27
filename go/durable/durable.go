// Package durable is AgentKit's durability seam over Inngest steps.
//
// Every value that crosses a step boundary is JSON — the typed helper
// [Run] marshals results with AgentKit's parity encoder on every path,
// durable or not, so code behaves identically inside and outside Inngest
// (the TypeScript package skipped the round-trip outside Inngest, which is
// exactly how its replay-only checksum bug stayed invisible in tests).
//
// # Control-flow contract — read this before writing a tool
//
// Inngest's Go SDK suspends execution by panicking with an internal
// ControlHijack value that the framework catches at the function boundary.
// Two rules follow, and violating either produces a function that silently
// completes instead of suspending — brutal to debug:
//
//  1. NEVER wrap a step call in recover(). Not in a tool handler, not in a
//     lifecycle hook, not in a defer. If you must recover around your own
//     code, re-panic anything you did not throw yourself.
//  2. NEVER call a step from a goroutine you spawned. Steps must run on the
//     goroutine Inngest invoked the function on.
//
// AgentKit's own wrapping honors both rules; ManualStep tools that drive
// step.WaitForEvent / step.Invoke themselves must honor them too.
//
// # Nested steps
//
// Inngest forbids opening a step inside another step. [InngestStep.Run]
// auto-collapses: called within a live step body it executes the function
// inline (with a debug log) instead of opening an illegal nested step. This
// removes the "am I already inside a step?" bookkeeping that TypeScript
// history adapters had to do by hand. The collapse cannot help a tool whose
// problem is the framework's wrapper itself — a handler that needs
// WaitForEvent or its own multi-step checkpoints still opts out with
// ManualStep, exactly as in TypeScript.
package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/inngest/inngestgo/step"

	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// RunFn is the unit of durable work: it returns the step's result as raw
// JSON, which Inngest memoizes and replays.
type RunFn func(ctx context.Context) (json.RawMessage, error)

// Step is the durability seam. AgentKit calls it for every side effect that
// must execute exactly once across Inngest replays: model inference, tool
// handlers, history writes, id minting, stream publishes.
type Step interface {
	Run(ctx context.Context, id string, fn RunFn) (json.RawMessage, error)
}

// OptionsStep is implemented by durability wrappers that need explicit run
// options. Ordinary Step implementations do not need to implement it.
type OptionsStep interface {
	RunWithOptions(ctx context.Context, id string, opts RunOptions, fn RunFn) (json.RawMessage, error)
}

// ReplayPolicy declares whether a completed operation must replay its exact
// result or may be executed again when the workflow driver replays.
//
// The zero value is ReplayMemoized. ReplayRecompute is a semantic promise made
// by trusted application code. Oversized-result handling is independent of
// this policy and is decided by the configured Step after serialization.
type ReplayPolicy string

const (
	// ReplayMemoized preserves the completed result across driver replays. Use
	// it for inference, side effects, state mutations and non-repeatable reads.
	ReplayMemoized ReplayPolicy = "memoized"

	// ReplayRecompute executes the operation inline on every driver replay and
	// does not ask Step to memoize the result. It is safe only for reviewed,
	// repeatable, side-effect-free work.
	ReplayRecompute ReplayPolicy = "recompute"
)

// RunOptions configures one typed durable operation.
type RunOptions struct {
	ReplayPolicy ReplayPolicy
}

// Validate rejects policy values that AgentKit does not understand. An empty
// policy is the backward-compatible ReplayMemoized default.
func (o RunOptions) Validate() error {
	switch o.ReplayPolicy {
	case "", ReplayMemoized, ReplayRecompute:
		return nil
	default:
		return fmt.Errorf("agentkit: unknown durable replay policy %q", o.ReplayPolicy)
	}
}

// Inngest returns the default Step. It behaves correctly in all three
// execution contexts without caller-side detection:
//
//   - inside an Inngest function: a real memoized step.Run
//   - inside an existing step body: collapses to inline execution (see the
//     package doc on nested steps)
//   - outside Inngest entirely (tests, scripts): the SDK executes the
//     function directly — no manager, no memoization
//
// This replaces the TypeScript package's getStepTools() detection and the
// `step ? step.run(...) : fn()` branching at every call site.
func Inngest() Step { return InngestStep{} }

// IsControlHijack reports whether a recovered panic value is inngestgo's
// step-suspension sentinel. The sentinel's type lives in an internal SDK
// package, so it cannot be named here; the name-plus-path check is the
// documented shape of it.
//
// Deferred cleanup uses this to tell a step suspension apart from a real
// exit: a ControlHijack unwind means the executor is parking this invocation
// to run a step, NOT that the function finished. Terminal emitters,
// finalizers and billing must not run on that path — they must re-panic and
// wait for the invocation that actually completes. Deferred code that
// recovers a hijack and does not re-panic turns the function into one that
// silently completes instead of suspending (see the package doc).
func IsControlHijack(recovered any) bool {
	if recovered == nil {
		return false
	}
	t := reflect.TypeOf(recovered)
	return t.Name() == "ControlHijack" &&
		strings.HasSuffix(t.PkgPath(), "/internal/sdkrequest")
}

// IsWithinStep reports whether ctx belongs to a live Inngest step body. Step
// wrappers use it to preserve nested-step collapse without importing the SDK.
func IsWithinStep(ctx context.Context) bool { return step.IsWithinStep(ctx) }

// InngestStep implements Step on the Inngest Go SDK. See Inngest.
type InngestStep struct{}

func (InngestStep) Run(ctx context.Context, id string, fn RunFn) (json.RawMessage, error) {
	if step.IsWithinStep(ctx) {
		// Opening a step here would be illegal nesting; run inline. The
		// enclosing step's memoization already covers this execution.
		slog.DebugContext(ctx, "agentkit: nested durable step collapsed to inline execution",
			"step_id", id)
		return fn(ctx)
	}
	return step.Run(ctx, id, fn)
}

// Inline executes immediately with no durability. For tests and for callers
// that explicitly do not want memoization; values still round-trip through
// JSON via [Run] so behavior matches the durable path.
type Inline struct{}

func (Inline) Run(ctx context.Context, id string, fn RunFn) (json.RawMessage, error) {
	return fn(ctx)
}

// Run executes fn under s with a typed result. The result is marshaled with
// AgentKit's parity encoder (jsonutil) and unmarshaled back on every path,
// so T must survive a JSON round-trip — the same constraint Inngest itself
// imposes on memoized values.
func Run[T any](ctx context.Context, s Step, id string, fn func(ctx context.Context) (T, error)) (T, error) {
	return RunWithOptions(ctx, s, id, RunOptions{}, fn)
}

// RunWithOptions executes fn with an explicit replay policy. ReplayRecompute
// deliberately bypasses Step.Run. Every other policy delegates to Step, which
// may apply storage rules after the result has been serialized. Every path
// keeps the same JSON round-trip.
func RunWithOptions[T any](ctx context.Context, s Step, id string, opts RunOptions, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if err := opts.Validate(); err != nil {
		return zero, err
	}
	if s == nil {
		s = Inngest()
	}
	encoded := func(ctx context.Context) (json.RawMessage, error) {
		v, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return jsonutil.Marshal(v)
	}
	var raw json.RawMessage
	var err error
	if opts.ReplayPolicy == ReplayRecompute {
		raw, err = encoded(ctx)
	} else if optionStep, ok := s.(OptionsStep); ok {
		raw, err = optionStep.RunWithOptions(ctx, id, opts, encoded)
	} else {
		raw, err = s.Run(ctx, id, encoded)
	}
	if err != nil {
		return zero, err
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, fmt.Errorf("agentkit: unmarshal durable step %q result into %T: %w", id, out, err)
	}
	return out, nil
}
