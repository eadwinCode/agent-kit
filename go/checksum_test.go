package agentkit

import "testing"

// TestChecksumParity pins Go's checksum against a golden vector produced by
// the TypeScript package:
//
//	xxh.h64(JSON.stringify([{type:"text",role:"assistant",content:"hello"}]) + "msg_123", 0).toString()
//	=> "11877540297208695414"
//
// If this fails, cespare/xxhash and xxhashjs disagree and strict checksum
// parity is impossible — stop and revisit the plan before porting further.
func TestChecksumParity(t *testing.T) {
	input := `[{"type":"text","role":"assistant","content":"hello"}]msg_123`
	if got, want := checksumOf(input), "11877540297208695414"; got != want {
		t.Fatalf("checksumOf mismatch with xxhashjs golden vector:\n got %s\nwant %s", got, want)
	}
}

// TestChecksumParityUnicode guards the hash over multi-byte input: xxhashjs
// hashes the UTF-8 bytes of the JS string, which must equal Go's UTF-8 bytes.
func TestChecksumParityUnicode(t *testing.T) {
	// Golden values generated with:
	//   node -e 'const x=require("xxhashjs"); console.log(x.h64(process.argv[1],0).toString())' '<input>'
	cases := map[string]string{
		`[{"content":"héllo 🌍 <tag> & çode"}]id-1`: "11683080702117945466",
		``: "17241709254077376921", // empty string, seed 0
	}
	for input, want := range cases {
		if got := checksumOf(input); got != want {
			t.Errorf("checksumOf(%q) = %s, want %s", input, got, want)
		}
	}
}
