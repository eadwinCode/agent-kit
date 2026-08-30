package agentkit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// goai renders a pointer field as a nullable TYPE ARRAY. Gemini's function
// declarations reject array types, and a node that then carries properties or
// required without a resolvable OBJECT type fails the entire request with
// "only allowed for OBJECT type" — one optional nested struct breaks every
// tool call the model tries to make.
func TestSanitizeInputSchemaResolvesNullableObjectArrays(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			// What goai emits for `Config *formConfigInput`.
			"config": map[string]any{
				"type": []any{"object", "null"},
				"properties": map[string]any{
					"url": map[string]any{"type": []any{"string", "null"}},
				},
				"required": []any{"url"},
			},
			"name": map[string]any{"type": "string"},
		},
		"required": []any{"name"},
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}

	out := string(sanitizeInputSchema(raw))
	if strings.Contains(out, `"null"`) || strings.Contains(out, `["object"`) {
		t.Fatalf("nullable array type survived: %s", out)
	}

	var decoded struct {
		Properties struct {
			Config struct {
				Type       string   `json:"type"`
				Properties struct{} `json:"properties"`
				Required   []string `json:"required"`
			} `json:"config"`
			Name struct {
				Type string `json:"type"`
			} `json:"name"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(sanitizeInputSchema(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Properties.Config.Type != "object" {
		t.Fatalf("config type = %q, want object", decoded.Properties.Config.Type)
	}
	if decoded.Properties.Name.Type != "string" {
		t.Fatalf("name type = %q", decoded.Properties.Name.Type)
	}
}

func TestSanitizeInputSchemaDefaultsUntypedPropertyNodesToObject(t *testing.T) {
	out := string(sanitizeInputSchema([]byte(
		`{"type":"object","properties":{"meta":{"properties":{"a":{"type":"string"}},"required":["a"]}}}`,
	)))
	var decoded struct {
		Properties struct {
			Meta struct {
				Type string `json:"type"`
			} `json:"meta"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Properties.Meta.Type != "object" {
		t.Fatalf("meta type = %q, want object", decoded.Properties.Meta.Type)
	}
}

func TestSanitizeInputSchemaPassesPrimitivesThrough(t *testing.T) {
	out := string(sanitizeInputSchema([]byte(`{"type":"string","description":"x"}`)))
	if out != `{"description":"x","type":"string"}` && out != `{"type":"string","description":"x"}` {
		t.Fatalf("primitive schema mutated: %s", out)
	}
	if got := sanitizeInputSchema(nil); len(got) != 0 {
		t.Fatalf("empty schema = %s", got)
	}
}

// End to end through NewTool: the generated schema for a tool taking an
// optional nested struct must carry no nullable array types.
func TestNewToolSanitizesPointerNestedStructSchema(t *testing.T) {
	type inner struct {
		URL *string `json:"url"`
	}
	type input struct {
		Name   string `json:"name"`
		Config *inner `json:"config"`
	}
	tool := NewTool[struct{}, input]("update_thing", "updates a thing",
		func(_ context.Context, _ input, _ ToolOptions[struct{}]) (any, error) {
			return nil, nil
		})
	var schema struct {
		Properties struct {
			Config struct {
				Type string `json:"type"`
			} `json:"config"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties.Config.Type != "object" {
		t.Fatalf("config type = %q, want object; schema=%s",
			schema.Properties.Config.Type, tool.InputSchema)
	}
}
