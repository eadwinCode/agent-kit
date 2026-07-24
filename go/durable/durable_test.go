package durable

import (
	"context"
	"errors"
	"testing"

	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

type payload struct {
	Name  string         `json:"name"`
	Count int            `json:"count"`
	When  *jsonutil.Time `json:"when,omitempty"`
}

// TestRunRoundTripsOnInlinePath verifies decision 2: the non-durable path
// still JSON round-trips, so a type that can't survive serialization fails
// in unit tests, not first in production replay.
func TestRunRoundTripsOnInlinePath(t *testing.T) {
	got, err := Run(context.Background(), Inline{}, "step-1", func(ctx context.Context) (payload, error) {
		return payload{Name: "a<b&c", Count: 2}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "a<b&c" || got.Count != 2 {
		t.Errorf("round-trip mangled value: %+v", got)
	}
}

// TestRunRoundTripCatchesUnserializable: a channel can't marshal; the error
// must surface instead of silently diverging from durable behavior.
func TestRunRoundTripCatchesUnserializable(t *testing.T) {
	_, err := Run(context.Background(), Inline{}, "step-1", func(ctx context.Context) (chan int, error) {
		return make(chan int), nil
	})
	if err == nil {
		t.Fatal("expected marshal error for chan, got nil")
	}
}

func TestRunPropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	_, err := Run(context.Background(), Inline{}, "step-1", func(ctx context.Context) (int, error) {
		return 0, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("want sentinel error, got %v", err)
	}
}

// TestInngestStepOutsideFunction: with no Inngest manager in ctx, the SDK
// executes the function directly — so the default Step is safe everywhere
// and callers never do context detection.
func TestInngestStepOutsideFunction(t *testing.T) {
	got, err := Run(context.Background(), Inngest(), "outside", func(ctx context.Context) (string, error) {
		return "ran inline", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "ran inline" {
		t.Errorf("got %q", got)
	}
}

// The nested-collapse branch (IsWithinStep) requires a live Inngest run to
// exercise — the within-step context key is unexported and only set by the
// SDK's own step body. Covered by the dev-server integration test in
// server_test.go once server.go exists (plan phase 7).
