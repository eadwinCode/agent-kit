package agentkit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type skuInput struct {
	SKU int `json:"sku" jsonschema:"description=The SKU to set"`
	// Optional params are POINTER fields: goai.SchemaFrom emits
	// OpenAI-strict-mode schemas where every property is required and
	// pointers become nullable. omitempty has no schema effect.
	Reason *string `json:"reason,omitempty"`
}

func newSKUTool(opts ...ToolOption[shape]) Tool[shape] {
	return NewTool[shape]("set_sku", "Sets the SKU.",
		func(ctx context.Context, in skuInput, topts ToolOptions[shape]) (any, error) {
			topts.State.Data.SKU = in.SKU
			return map[string]any{"ok": true, "sku": in.SKU}, nil
		}, opts...)
}

func TestNewToolSchemaFromStructTags(t *testing.T) {
	tool := newSKUTool()
	var schema struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			// Type is a string for plain fields, an array for nullable
			// (pointer) fields: ["string","null"].
			Type        json.RawMessage `json:"type"`
			Description string          `json:"description"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
		t.Fatalf("schema not valid JSON: %v\n%s", err, tool.InputSchema)
	}
	if schema.Type != "object" {
		t.Errorf("schema.type = %q", schema.Type)
	}
	sku, ok := schema.Properties["sku"]
	if !ok {
		t.Fatalf("missing sku property: %s", tool.InputSchema)
	}
	if string(sku.Type) != `"integer"` || sku.Description != "The SKU to set" {
		t.Errorf("sku property = %+v", sku)
	}
	if reason := schema.Properties["reason"]; string(reason.Type) != `["string","null"]` {
		t.Errorf("pointer field should be nullable, got type %s", reason.Type)
	}
	// Strict-mode contract: every property is listed as required; optional
	// params are pointers, which become nullable rather than omitted.
	joined := strings.Join(schema.Required, ",")
	if !strings.Contains(joined, "sku") || !strings.Contains(joined, "reason") {
		t.Errorf("strict-mode schema should require all properties, got %v", schema.Required)
	}
	if !strings.Contains(string(tool.InputSchema), "null") {
		t.Errorf("pointer field should be nullable: %s", tool.InputSchema)
	}
}

func TestNewToolDecodesInputAndMutatesState(t *testing.T) {
	tool := newSKUTool()
	st := NewState(StateConfig[shape]{})
	out, err := tool.Handler(context.Background(), json.RawMessage(`{"sku": 7}`), ToolOptions[shape]{State: st})
	if err != nil {
		t.Fatal(err)
	}
	if st.Data.SKU != 7 {
		t.Errorf("state not mutated: %+v", st.Data)
	}
	m, _ := out.(map[string]any)
	if m["sku"] != 7 {
		t.Errorf("handler output = %v", out)
	}
}

func TestNewToolInvalidInputReturnsError(t *testing.T) {
	tool := newSKUTool()
	_, err := tool.Handler(context.Background(), json.RawMessage(`{"sku": "not-a-number"}`), ToolOptions[shape]{State: NewState(StateConfig[shape]{})})
	if err == nil {
		t.Fatal("expected decode error to flow back as the tool error")
	}
	if !strings.Contains(err.Error(), "set_sku") {
		t.Errorf("error should name the tool: %v", err)
	}
}

func TestNewToolEmptyInputRuns(t *testing.T) {
	// Model may send no arguments at all; handler still runs on zero value.
	tool := newSKUTool()
	if _, err := tool.Handler(context.Background(), nil, ToolOptions[shape]{State: NewState(StateConfig[shape]{})}); err != nil {
		t.Fatal(err)
	}
}

func TestToolOptions(t *testing.T) {
	tool := newSKUTool(WithManualStep[shape](), WithStrict[shape]())
	if !tool.ManualStep || !tool.Strict {
		t.Errorf("options not applied: %+v", tool)
	}
	noParams := newSKUTool(WithoutParameters[shape]())
	if noParams.InputSchema != nil {
		t.Error("WithoutParameters must clear the schema")
	}
	custom := json.RawMessage(`{"type":"object","properties":{}}`)
	overridden := newSKUTool(WithInputSchema[shape](custom))
	if string(overridden.InputSchema) != string(custom) {
		t.Error("WithInputSchema must override")
	}
}

func TestMarshalToolResultWrapsData(t *testing.T) {
	raw, err := MarshalToolResult(map[string]any{"applied": true})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"data":{"applied":true}}`
	if string(raw) != want {
		t.Errorf("got %s want %s", raw, want)
	}
}
