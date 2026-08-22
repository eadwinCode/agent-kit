package agentkit_test

// A0 negative architecture checks for the Go runtime.
//
// These assert the absence of things, which normal tests never catch: a
// generic runtime stays generic only if something fails when application
// concepts leak into it. Each check names the boundary it defends.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

// tenancyLeakage lists tenancy vocabulary that must never appear in
// AgentKit's contracts.
//
// This is the check that was missing when SessionKey shipped with TeamID and
// ProjectID fields: banning table names and one company's name caught the
// obvious leak and missed the structural one. A contract that names a tenancy
// model forces every consumer without that model — a single-tenant
// deployment, a B2C app, an org/workspace hierarchy — to carry empty fields
// to satisfy a shape AgentKit never reads. Scope is opaque precisely so that
// the application's model stays the application's.
var tenancyLeakage = []string{
	"teamid",
	"team_id",
	"projectid",
	"project_id",
	"orgid",
	"org_id",
	"organizationid",
	"workspaceid",
	"workspace_id",
	"tenantid",
	"tenant_id",
	"accountid",
	"account_id",
}

// applicationLeakage lists identifiers that must never appear in AgentKit's
// public API or its non-test source. They are application vocabulary: the
// moment a generic port knows one of them, adapters stop being swappable.
var applicationLeakage = []string{
	"clevix",
	"ai_runs",
	"ai_run_events",
	"ai_thread_messages",
	"ai_agent_sessions",
	"ai_agent_commands",
	"ai_action_approvals",
	"ai_threads",
	"getlatestthread",
	"landing-page-builder",
}

// packageSourceFiles returns AgentKit's non-test .go files.
func packageSourceFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	roots := []string{".", "durable", "memadapter", "conformance"}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read %s: %v", root, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			files = append(files, filepath.Join(root, name))
		}
	}
	return files
}

func TestNoApplicationVocabularyInTheRuntime(t *testing.T) {
	for _, path := range packageSourceFiles(t) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		lower := strings.ToLower(string(raw))
		for _, banned := range applicationLeakage {
			if strings.Contains(lower, banned) {
				t.Errorf("%s mentions %q. AgentKit owns generic contracts; tenancy, schema, "+
					"endpoints and product policy belong in the application's adapter.", path, banned)
			}
		}
	}
}

func TestNoTenancyModelInTheContracts(t *testing.T) {
	for _, path := range packageSourceFiles(t) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		lower := strings.ToLower(string(raw))
		for _, banned := range tenancyLeakage {
			if strings.Contains(lower, banned) {
				t.Errorf("%s names %q. Tenancy is the application's model: AgentKit scopes by an "+
					"opaque SessionScope it never parses, so consumers with a different model — or "+
					"none — are not forced to carry fields the runtime never reads.", path, banned)
			}
		}
	}
}

// TestScopeIsOpaque pins the shape of the scope token itself. A struct with
// named fields here is how a tenancy model gets back in.
func TestScopeIsOpaque(t *testing.T) {
	var scope agentkit.SessionScope = "anything the application wants"
	if string(scope) != "anything the application wants" {
		t.Fatal("SessionScope must be a plain string the application composes")
	}
	if reflect.TypeOf(scope).Kind() != reflect.String {
		t.Fatalf("SessionScope kind = %v; it must stay an opaque scalar so no field names creep in",
			reflect.TypeOf(scope).Kind())
	}
	// It must also be usable as a map key: adapters index their storage by it.
	index := map[agentkit.SessionScope]int{scope: 1}
	if index[scope] != 1 {
		t.Fatal("SessionScope must be comparable so adapters can key storage by it")
	}
}

// TestPortsDoNotImportApplicationPackages keeps the dependency direction
// pointing one way: adapters depend on AgentKit, never the reverse.
func TestPortsDoNotImportApplicationPackages(t *testing.T) {
	allowedPrefixes := []string{
		"github.com/eadwinCode/agent-kit/go",
		"github.com/google/uuid",
		"github.com/cespare/xxhash",
		"github.com/inngest/inngestgo",
		"github.com/zendev-sh/goai",
		"github.com/modelcontextprotocol",
	}

	fset := token.NewFileSet()
	for _, path := range packageSourceFiles(t) {
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imp := range file.Imports {
			pkg := strings.Trim(imp.Path.Value, `"`)
			if !strings.Contains(pkg, ".") {
				continue // standard library
			}
			ok := false
			for _, prefix := range allowedPrefixes {
				if strings.HasPrefix(pkg, prefix) {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("%s imports %q, which is outside AgentKit's allowed dependency set. "+
					"A runtime contract that depends on an application package is not storage-neutral.", path, pkg)
			}
		}
	}
}

// TestEveryPortMethodTakesContext enforces the contract rule that every port
// method is cancellable and carries request scope. A method without a
// context cannot honor a cancelled run or a request deadline.
func TestEveryPortMethodTakesContext(t *testing.T) {
	// Only the ports the APPLICATION implements and AgentKit calls into.
	// StructuredStream is the emitter AgentKit hands to tools, not a storage
	// port, and its Identity accessor is a pure read.
	ports := map[string]bool{
		"EventJournal":     true,
		"JournalCompactor": true,
		"StateStore":       true,
		"ControlStore":     true,
		"ApprovalStore":    true,
		"StreamSink":       true,
		"Finalizer":        true,
	}

	fset := token.NewFileSet()
	found := map[string]bool{}
	for _, path := range packageSourceFiles(t) {
		if !strings.HasPrefix(path, "ports.go") && filepath.Dir(path) != "." {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || !ports[spec.Name.Name] {
				return true
			}
			iface, ok := spec.Type.(*ast.InterfaceType)
			if !ok {
				return true
			}
			found[spec.Name.Name] = true
			for _, method := range iface.Methods.List {
				fn, ok := method.Type.(*ast.FuncType)
				if !ok || len(method.Names) == 0 {
					continue
				}
				if fn.Params.NumFields() == 0 {
					t.Errorf("%s.%s takes no parameters; every port method must accept a context.Context",
						spec.Name.Name, method.Names[0].Name)
					continue
				}
				first := fn.Params.List[0].Type
				sel, ok := first.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Context" {
					t.Errorf("%s.%s does not take context.Context first; a port method that cannot be "+
						"cancelled cannot honor a cancelled run", spec.Name.Name, method.Names[0].Name)
				}
			}
			return true
		})
	}

	for name := range ports {
		if !found[name] {
			t.Errorf("port %q was not found in the package source; if it was renamed, update this check "+
				"so the contract stays enforced", name)
		}
	}
}

// TestPortsAreNilSafe proves a consumer can adopt one contract at a time.
// An all-or-nothing port set would force an application to implement six
// adapters before it can use any.
func TestPortsAreNilSafe(t *testing.T) {
	if _, err := runNetwork(t, textModel("hi"), nil, nil, nil); err != nil {
		t.Fatalf("a run with no ports at all must still work: %v", err)
	}
}
