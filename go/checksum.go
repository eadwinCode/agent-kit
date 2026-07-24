package agentkit

import (
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// checksumOf mirrors the TypeScript package's AgentResult checksum:
// xxhashjs.h64(input, 0).toString(), which renders the 64-bit hash in
// decimal (verified against xxhashjs — see checksum_test.go's golden vector).
//
// The input is JSON.stringify(output.concat(toolCalls)) + (id ?? ""); callers
// build it with jsonutil.Marshal so the serialization matches JSON.stringify
// byte for byte. createdAt is deliberately excluded — see the TS types.ts
// checksum doc comment (wall-clock re-stamping on Inngest replays made
// logically identical results hash differently).
func checksumOf(input string) string {
	return strconv.FormatUint(xxhash.Sum64String(input), 10)
}
