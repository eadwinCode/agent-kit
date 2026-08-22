// Package jsonschema is a minimal JSON Schema validator covering the
// subset the frozen contract schemas in contracts/schemas use.
//
// It exists so the schemas can be *executable* without adding a third-party
// dependency to a published module. The contracts are the boundary between
// two runtimes and every application adapter; a schema nothing validates is
// documentation, not a contract.
//
// Supported keywords: type, const, enum, required, properties,
// additionalProperties, items, oneOf, minimum, minLength. Anything else is
// ignored — which is safe here, because the schemas are ours and the
// validator is checked against them by tests.
package jsonschema

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Schema is a parsed schema document.
type Schema struct {
	raw map[string]any
}

// Parse reads a schema document.
func Parse(data []byte) (*Schema, error) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("jsonschema: parse: %w", err)
	}
	return &Schema{raw: raw}, nil
}

// Validate checks one JSON document against the schema and returns every
// violation, deepest path first. An empty slice means valid.
func (s *Schema) Validate(document []byte) []string {
	var value any
	if err := json.Unmarshal(document, &value); err != nil {
		return []string{fmt.Sprintf("$: not valid JSON: %v", err)}
	}
	var errs []string
	validate(s.raw, value, "$", &errs)
	sort.Strings(errs)
	return errs
}

// ValidateValue checks an already-decoded value.
func (s *Schema) ValidateValue(value any) []string {
	var errs []string
	validate(s.raw, value, "$", &errs)
	sort.Strings(errs)
	return errs
}

func validate(schema map[string]any, value any, path string, errs *[]string) {
	if len(schema) == 0 {
		return
	}

	if alternatives, ok := schema["oneOf"].([]any); ok {
		matched := 0
		for _, alt := range alternatives {
			sub, ok := alt.(map[string]any)
			if !ok {
				continue
			}
			var subErrs []string
			validate(sub, value, path, &subErrs)
			if len(subErrs) == 0 {
				matched++
			}
		}
		if matched != 1 {
			*errs = append(*errs, fmt.Sprintf("%s: matched %d of %d oneOf alternatives, want exactly 1", path, matched, len(alternatives)))
			return
		}
	}

	if want, ok := schema["type"].(string); ok && !typeMatches(want, value) {
		*errs = append(*errs, fmt.Sprintf("%s: type is %s, want %s", path, typeName(value), want))
		return
	}

	if want, ok := schema["const"]; ok && !equalJSON(want, value) {
		*errs = append(*errs, fmt.Sprintf("%s: value %v, want const %v", path, value, want))
	}

	if allowed, ok := schema["enum"].([]any); ok {
		found := false
		for _, candidate := range allowed {
			if equalJSON(candidate, value) {
				found = true
				break
			}
		}
		if !found {
			*errs = append(*errs, fmt.Sprintf("%s: value %v is not one of the %d allowed values", path, value, len(allowed)))
		}
	}

	switch typed := value.(type) {
	case map[string]any:
		validateObject(schema, typed, path, errs)
	case []any:
		if items, ok := schema["items"].(map[string]any); ok {
			for i, item := range typed {
				validate(items, item, fmt.Sprintf("%s[%d]", path, i), errs)
			}
		}
	case string:
		if min, ok := numberOf(schema["minLength"]); ok && float64(len(typed)) < min {
			*errs = append(*errs, fmt.Sprintf("%s: length %d, want at least %g", path, len(typed), min))
		}
	case float64:
		if min, ok := numberOf(schema["minimum"]); ok && typed < min {
			*errs = append(*errs, fmt.Sprintf("%s: value %g, want at least %g", path, typed, min))
		}
	}
}

func validateObject(schema map[string]any, object map[string]any, path string, errs *[]string) {
	if required, ok := schema["required"].([]any); ok {
		for _, key := range required {
			name, ok := key.(string)
			if !ok {
				continue
			}
			if _, present := object[name]; !present {
				*errs = append(*errs, fmt.Sprintf("%s: missing required property %q", path, name))
			}
		}
	}

	properties, _ := schema["properties"].(map[string]any)
	for _, name := range sortedKeys(object) {
		child := fmt.Sprintf("%s.%s", path, name)
		if properties != nil {
			if sub, ok := properties[name].(map[string]any); ok {
				validate(sub, object[name], child, errs)
				continue
			}
		}
		if allowed, ok := schema["additionalProperties"].(bool); ok && !allowed {
			*errs = append(*errs, fmt.Sprintf("%s: unexpected property %q", path, name))
		}
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func typeMatches(want string, value any) bool {
	switch want {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	case "number":
		_, ok := value.(float64)
		return ok
	case "integer":
		n, ok := value.(float64)
		return ok && n == math.Trunc(n)
	default:
		return true
	}
}

func typeName(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case float64:
		if v == math.Trunc(v) {
			return "integer"
		}
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}

func numberOf(value any) (float64, bool) {
	n, ok := value.(float64)
	return n, ok
}

func equalJSON(a, b any) bool {
	an, aok := a.(float64)
	bn, bok := b.(float64)
	if aok && bok {
		return an == bn
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return as == bs
	}
	ab, aok := a.(bool)
	bb, bok := b.(bool)
	if aok && bok {
		return ab == bb
	}
	if a == nil && b == nil {
		return true
	}
	left, err1 := json.Marshal(a)
	right, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && strings.EqualFold(string(left), string(right))
}
