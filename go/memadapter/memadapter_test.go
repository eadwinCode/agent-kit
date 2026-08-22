package memadapter_test

// The in-memory adapters run the same public conformance suite an
// application adapter runs. If a contract changes, this fails first.

import (
	"testing"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/conformance"
	"github.com/eadwinCode/agent-kit/go/memadapter"
)

func TestJournalConformance(t *testing.T) {
	conformance.VerifyEventJournal(t, func() agentkit.EventJournal {
		return memadapter.NewJournal()
	})
}

func TestStateStoreConformance(t *testing.T) {
	conformance.VerifyStateStore(t, func() agentkit.StateStore {
		return memadapter.NewStateStore()
	})
}

func TestControlStoreConformance(t *testing.T) {
	conformance.VerifyControlStore(t, func() agentkit.ControlStore {
		return memadapter.NewControlStore()
	})
}

func TestApprovalStoreConformance(t *testing.T) {
	conformance.VerifyApprovalStore(t,
		func() agentkit.ApprovalStore { return memadapter.NewApprovalStore() },
		func(store agentkit.ApprovalStore, scope agentkit.SessionScope, requestID string, status agentkit.ApprovalStatus) {
			store.(*memadapter.ApprovalStore).DecideFor(scope, requestID, status, "")
		},
	)
}

func TestHistoryConformance(t *testing.T) {
	conformance.VerifyHistoryConfig(t, func(*testing.T) conformance.HistoryAdapter[historyState] {
		return memadapter.NewHistory[historyState]()
	})
}

// historyState is the typed state the conformance run carries. History
// persistence must not depend on what is in it.
type historyState struct {
	Note string `json:"note,omitempty"`
}
