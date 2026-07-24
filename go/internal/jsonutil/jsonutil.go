// Package jsonutil is the single marshaling path for everything AgentKit
// writes to a wire or a database.
//
// The TypeScript package serializes with JSON.stringify; Go's encoding/json
// differs from it in three ways that would silently break wire parity:
//
//  1. encoding/json escapes '<', '>' and '&' to < etc. by default.
//     JSON.stringify does not, and agent messages are full of code snippets.
//     Marshal disables HTML escaping.
//  2. time.Time marshals as RFC3339Nano, trimming trailing zeros and omitting
//     the fractional second entirely for whole seconds. Date#toISOString()
//     always emits exactly three fractional digits. Use the Time type for any
//     timestamp that crosses the wire.
//  3. map[string]any re-marshals with keys sorted alphabetically, while
//     JavaScript objects preserve insertion order. Content that AgentKit does
//     not interpret (tool inputs, tool-result payloads) must therefore be
//     carried as json.RawMessage end-to-end and never decoded into maps —
//     that rule lives at the call sites; this package just documents it.
package jsonutil

import (
	"bytes"
	"encoding/json"
	"time"
)

// Marshal encodes v like JSON.stringify: no HTML escaping, no trailing
// newline, compact output.
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encoder.Encode appends a newline that Stringify would not.
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), nil
}

// MarshalString is Marshal returning a string.
func MarshalString(v any) (string, error) {
	b, err := Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// isoMillis is Date#toISOString()'s exact shape: UTC, always three
// fractional digits, 'Z' suffix.
const isoMillis = "2006-01-02T15:04:05.000Z"

// Time marshals in Date#toISOString() format so rows written by Go are
// byte-identical to rows written by the TypeScript package. It accepts any
// RFC3339 timestamp when unmarshaling.
type Time struct {
	time.Time
}

// Now returns the current instant as a Time.
func Now() Time {
	return Time{time.Now()}
}

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.UTC().Format(isoMillis) + `"`), nil
}

func (t *Time) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	// time.Parse accepts a fractional second after the seconds field even
	// when the layout has none, so this covers ".000", ".123456789" and none.
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}
	t.Time = parsed
	return nil
}
