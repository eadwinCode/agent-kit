package jsonutil

import (
	"testing"
	"time"
)

// TestMarshalNoHTMLEscaping pins parity trap #1: JSON.stringify does not
// escape <, > or &, and messages carry code snippets constantly.
func TestMarshalNoHTMLEscaping(t *testing.T) {
	got, err := MarshalString(map[string]string{"code": `if (a < b && c > d) { run("x&y") }`})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"code":"if (a < b && c > d) { run(\"x&y\") }"}`
	if got != want {
		t.Errorf("Marshal escaped HTML characters:\n got %s\nwant %s", got, want)
	}
}

// TestTimeISOMillis pins parity trap #2: Date#toISOString() always emits
// exactly three fractional digits; Go's default trims them.
func TestTimeISOMillis(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		// Whole second — Go's RFC3339Nano would emit no fraction at all.
		{time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC), `"2026-07-24T10:00:00.000Z"`},
		// 7ms — RFC3339Nano would emit ".007" only by luck; ".070" would trim.
		{time.Date(2026, 7, 24, 10, 4, 5, 7e6, time.UTC), `"2026-07-24T10:04:05.007Z"`},
		// Sub-millisecond precision truncates to ms like Date does.
		{time.Date(2026, 7, 24, 10, 4, 5, 123456789, time.UTC), `"2026-07-24T10:04:05.123Z"`},
		// Non-UTC input normalizes to Z.
		{time.Date(2026, 7, 24, 12, 0, 0, 0, time.FixedZone("CEST", 2*3600)), `"2026-07-24T10:00:00.000Z"`},
	}
	for _, c := range cases {
		got, err := Marshal(Time{c.in})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Errorf("Time(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestTimeRoundTrip(t *testing.T) {
	for _, s := range []string{
		`"2026-07-24T10:00:00.000Z"`,
		`"2026-07-24T10:00:00Z"`,          // no fraction (Go-written pre-fix rows)
		`"2026-07-24T10:00:00.123456Z"`,   // micros
		`"2026-07-24T12:00:00.000+02:00"`, // offset
	} {
		var tt Time
		if err := tt.UnmarshalJSON([]byte(s)); err != nil {
			t.Errorf("UnmarshalJSON(%s): %v", s, err)
		}
	}
}
