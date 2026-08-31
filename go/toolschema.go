package agentkit

import (
	"encoding/json"
)

// sanitizeInputSchema normalizes a generated input schema for providers that
// validate JSON Schema strictly — Gemini chief among them.
//
// goai renders POINTER fields as nullable by emitting the type as an array
// ("type": ["string", "null"]) while KEEPING the field in the parent's
// required list — optionality lives entirely in the nullability. Gemini's
// function declarations reject that twice over: `type` must be a single enum
// value, and a node carrying `properties` or `required` whose type does not
// resolve to OBJECT fails the whole request with "only allowed for OBJECT
// type" — so one optional nested struct silently breaks every tool call the
// model tries to make.
//
// The walk therefore resolves type arrays to their first non-null entry,
// drops a field from its parent's required list when its type was nullable
// (that omission is how optionality is expressed on the strict providers
// agent-kit targets), and defaults nodes that carry properties or required
// to object when no usable type remains. Everything else passes through.
func sanitizeInputSchema(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		// The schema came from json.Marshal; it cannot fail to parse.
		return raw
	}
	sanitizeSchemaNode(node)
	out, err := json.Marshal(node)
	if err != nil {
		return raw
	}
	return out
}

// sanitizeSchemaNode normalizes one schema node in place and reports whether
// the node was nullable — information the PARENT needs, because nullable
// fields must leave the parent's required list to stay optional.
func sanitizeSchemaNode(node map[string]any) (nullable bool) {
	if t, ok := node["type"].([]any); ok {
		first := ""
		for _, entry := range t {
			if s, ok := entry.(string); ok && s != "null" {
				first = s
				break
			}
		}
		nullable = true
		if first == "" {
			delete(node, "type")
		} else {
			node["type"] = first
		}
	}
	if t, ok := node["type"].(string); ok && t != "object" {
		// Scalars cannot legally carry these keywords.
		delete(node, "properties")
		delete(node, "required")
	}
	if _, hasProps := node["properties"]; hasProps {
		if node["type"] == nil {
			node["type"] = "object"
		}
	}
	if _, hasRequired := node["required"]; hasRequired {
		if node["type"] == nil {
			node["type"] = "object"
		}
	}
	props, _ := node["properties"].(map[string]any)
	for name, child := range props {
		childNode, ok := child.(map[string]any)
		if !ok {
			continue
		}
		if sanitizeSchemaNode(childNode) {
			markNotRequired(node, name)
		}
	}
	if items, ok := node["items"].(map[string]any); ok {
		sanitizeSchemaNode(items)
	}
	return nullable
}

// markNotRequired removes one name from a node's required list.
func markNotRequired(node map[string]any, name string) {
	required, _ := node["required"].([]any)
	kept := required[:0]
	for _, entry := range required {
		if s, ok := entry.(string); ok && s == name {
			continue
		}
		kept = append(kept, entry)
	}
	if len(kept) == 0 {
		delete(node, "required")
		return
	}
	node["required"] = kept
}
