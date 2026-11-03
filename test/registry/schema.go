package registry

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// annotation keywords are allowed in a schema and ignored.
var annotation = map[string]bool{
	"$schema": true, "$id": true, "$comment": true, "title": true, "description": true,
	"default": true, "examples": true, "definitions": true,
}

// Schema is a JSON Schema restricted to the keywords the registry schemas use: type, enum, const,
// required, properties, additionalProperties, items, minItems, minLength, pattern, minimum, maximum,
// anyOf, oneOf and $ref into #/definitions. A schema that uses any other keyword is rejected when it
// is loaded, so a keyword can never look enforced while being ignored.
type Schema struct {
	root map[string]any
}

// LoadSchema reads and checks a schema file.
func LoadSchema(path string) (*Schema, error) {
	dir, name := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return LoadSchemaFS(root.FS(), name)
}

// LoadSchemaFS reads and checks a schema file from fsys.
func LoadSchemaFS(fsys fs.FS, path string) (*Schema, error) {
	raw, err := fs.ReadFile(fsys, path)
	if err != nil {
		return nil, err
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	s := &Schema{root: root}
	if err := s.checkKeywords(root, "#"); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

func (s *Schema) checkKeywords(node any, at string) error {
	m, ok := node.(map[string]any)
	if !ok {
		return fmt.Errorf("%s: a schema must be an object", at)
	}
	for key, value := range m {
		switch key {
		case "type", "enum", "const", "required", "pattern", "minItems", "minLength", "minimum", "maximum", "$ref":
		case "properties":
			props, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s/properties: must be an object", at)
			}
			for name, sub := range props {
				if err := s.checkKeywords(sub, at+"/properties/"+name); err != nil {
					return err
				}
			}
		case "definitions":
			defs, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s/definitions: must be an object", at)
			}
			for name, sub := range defs {
				if err := s.checkKeywords(sub, at+"/definitions/"+name); err != nil {
					return err
				}
			}
		case "items", "additionalProperties":
			if _, isBool := value.(bool); isBool && key == "additionalProperties" {
				continue
			}
			if err := s.checkKeywords(value, at+"/"+key); err != nil {
				return err
			}
		case "anyOf", "oneOf":
			list, ok := value.([]any)
			if !ok {
				return fmt.Errorf("%s/%s: must be an array", at, key)
			}
			for i, sub := range list {
				if err := s.checkKeywords(sub, fmt.Sprintf("%s/%s/%d", at, key, i)); err != nil {
					return err
				}
			}
		default:
			if !annotation[key] {
				return fmt.Errorf("%s: unsupported keyword %q", at, key)
			}
		}
	}
	return nil
}

// Validate returns every violation of doc against the schema, each prefixed by its location.
func (s *Schema) Validate(doc any) []string {
	var problems []string
	s.validate(s.root, doc, "$", &problems)
	sort.Strings(problems)
	return problems
}

func (s *Schema) resolve(ref string) (map[string]any, bool) {
	name, ok := strings.CutPrefix(ref, "#/definitions/")
	if !ok {
		return nil, false
	}
	defs, _ := s.root["definitions"].(map[string]any)
	sub, ok := defs[name].(map[string]any)
	return sub, ok
}

func typeOf(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case int, int64, uint64:
		return "integer"
	case float64:
		if x == float64(int64(x)) {
			return "integer"
		}
		return "number"
	}
	return fmt.Sprintf("%T", v)
}

func typeMatches(want, got string) bool {
	return want == got || (want == "number" && got == "integer")
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func (s *Schema) validate(node map[string]any, v any, at string, out *[]string) {
	add := func(format string, args ...any) { *out = append(*out, at+": "+fmt.Sprintf(format, args...)) }
	if ref, ok := node["$ref"].(string); ok {
		sub, found := s.resolve(ref)
		if !found {
			add("unresolved $ref %s", ref)
			return
		}
		s.validate(sub, v, at, out)
	}
	if !checkType(node, v, add) {
		return
	}
	checkEnum(node, v, add)
	switch x := v.(type) {
	case string:
		checkString(node, x, add)
	case []any:
		if n, ok := toFloat(node["minItems"]); ok && float64(len(x)) < n {
			add("has %d item(s), want at least %d", len(x), int(n))
		}
		if items, ok := node["items"].(map[string]any); ok {
			for i, e := range x {
				s.validate(items, e, fmt.Sprintf("%s[%d]", at, i), out)
			}
		}
	case map[string]any:
		s.validateObject(node, x, at, out)
	default:
		checkNumber(node, v, add)
	}
	s.checkAlternatives(node, v, at, add)
}

func checkType(node map[string]any, v any, add func(string, ...any)) bool {
	t, ok := node["type"]
	if !ok {
		return true
	}
	var wants []string
	switch tt := t.(type) {
	case string:
		wants = []string{tt}
	case []any:
		for _, x := range tt {
			wants = append(wants, fmt.Sprint(x))
		}
	}
	got := typeOf(v)
	for _, w := range wants {
		if typeMatches(w, got) {
			return true
		}
	}
	add("is %s, want %s", got, strings.Join(wants, " or "))
	return false
}

func checkEnum(node map[string]any, v any, add func(string, ...any)) {
	if list, ok := node["enum"].([]any); ok {
		found := false
		for _, e := range list {
			found = found || typeOf(e) == typeOf(v) && fmt.Sprintf("%v", e) == fmt.Sprintf("%v", v)
		}
		if !found {
			add("%v is not one of %v", v, list)
		}
	}
	if c, ok := node["const"]; ok && fmt.Sprintf("%v", c) != fmt.Sprintf("%v", v) {
		add("%v is not the constant %v", v, c)
	}
}

func checkString(node map[string]any, x string, add func(string, ...any)) {
	if p, ok := node["pattern"].(string); ok && !regexp.MustCompile(p).MatchString(x) {
		add("%q does not match %s", x, p)
	}
	if n, ok := toFloat(node["minLength"]); ok && float64(len([]rune(x))) < n {
		add("is shorter than %d", int(n))
	}
}

func checkNumber(node map[string]any, v any, add func(string, ...any)) {
	f, ok := toFloat(v)
	if !ok {
		return
	}
	if lo, ok := toFloat(node["minimum"]); ok && f < lo {
		add("%v is below %v", f, lo)
	}
	if hi, ok := toFloat(node["maximum"]); ok && f > hi {
		add("%v is above %v", f, hi)
	}
}

// checkAlternatives applies anyOf (at least one subschema holds) and oneOf (exactly one).
func (s *Schema) checkAlternatives(node map[string]any, v any, at string, add func(string, ...any)) {
	for _, key := range []string{"anyOf", "oneOf"} {
		list, ok := node[key].([]any)
		if !ok {
			continue
		}
		matches := 0
		for _, sub := range list {
			var inner []string
			if m, isMap := sub.(map[string]any); isMap {
				s.validate(m, v, at, &inner)
			}
			if len(inner) == 0 {
				matches++
			}
		}
		if matches == 0 || (key == "oneOf" && matches != 1) {
			add("matches %d of the %d %s alternatives", matches, len(list), key)
		}
	}
}

func (s *Schema) validateObject(node map[string]any, obj map[string]any, at string, out *[]string) {
	if req, ok := node["required"].([]any); ok {
		for _, r := range req {
			if _, present := obj[fmt.Sprint(r)]; !present {
				*out = append(*out, fmt.Sprintf("%s: missing required key %q", at, r))
			}
		}
	}
	props, _ := node["properties"].(map[string]any)
	for key, value := range obj {
		if sub, ok := props[key].(map[string]any); ok {
			s.validate(sub, value, at+"."+key, out)
			continue
		}
		switch extra := node["additionalProperties"].(type) {
		case bool:
			if !extra {
				*out = append(*out, fmt.Sprintf("%s: unknown key %q", at, key))
			}
		case map[string]any:
			s.validate(extra, value, at+"."+key, out)
		}
	}
}
