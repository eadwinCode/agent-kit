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
	var zero T
	raw, err := s.Run(ctx, id, func(ctx context.Context) (json.RawMessage, error) {
		v, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return jsonutil.Marshal(v)
	})
	if err != nil {
		return zero, err
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, fmt.Errorf("agentkit: unmarshal durable step %q result into %T: %w", id, out, err)
	}
	return out, nil
}
