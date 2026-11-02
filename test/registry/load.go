// Package registry loads the conformance registry's case files strictly: one YAML parser, one JSON
// Schema, no regular-expression reading of YAML. New consumers read a case's fields from here (Python
// tools through the regload command); the older regular-expression readers stay until they are ported.
package registry

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Case is one registry case file.
type Case struct {
	ID   string         `json:"id"`
	Path string         `json:"path"`
	Doc  map[string]any `json:"doc"`
}

// CasesDir and SchemaFile locate the cases and their schema under the registry root.
const (
	CasesDir   = "cases"
	SchemaFile = "schema/case.schema.json"
)

// Parse decodes one YAML document strictly: exactly one document, only string keys, no duplicate keys
// (yaml.v3 rejects them), no unquoted timestamp, and only JSON-representable values.
func Parse(data []byte) (map[string]any, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("more than one YAML document")
	}
	norm, err := normalize(raw, "$")
	if err != nil {
		return nil, err
	}
	doc, ok := norm.(map[string]any)
	if !ok {
		return nil, errors.New("the document is not a mapping")
	}
	return doc, nil
}

func normalize(v any, at string) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			n, err := normalize(e, at+"."+k)
			if err != nil {
				return nil, err
			}
			out[k] = n
		}
		return out, nil
	case map[any]any:
		return nil, fmt.Errorf("%s: a mapping key that is not a string", at)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			n, err := normalize(e, fmt.Sprintf("%s[%d]", at, i))
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	case time.Time:
		return nil, fmt.Errorf("%s: an unquoted timestamp; quote it", at)
	}
	return v, nil
}

// LoadAll reads every case under root/cases, sorted by id, and returns it with the problems found: a
// file that does not parse strictly, an id that is not its path under cases/, a duplicate id, and every
// violation of the schema. A problem in one file does not stop the others being checked.
func LoadAll(root string) ([]Case, []string, error) {
	// Everything is read through an os.Root, so a symlink inside the registry cannot lead the walk out of it.
	reg, err := os.OpenRoot(root)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = reg.Close() }()
	schema, err := LoadSchemaFS(reg.FS(), SchemaFile)
	if err != nil {
		return nil, nil, err
	}
	fsys := reg.FS()
	var cases []Case
	var problems []string
	seen := map[string]string{}
	err = fs.WalkDir(fsys, CasesDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || filepath.Ext(path) != ".yaml" {
			return walkErr
		}
		id := strings.TrimSuffix(strings.TrimPrefix(path, CasesDir+"/"), ".yaml")
		display := filepath.ToSlash(filepath.Join(root, path))
		data, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		doc, err := Parse(data)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", display, err))
			return nil
		}
		for _, p := range schema.Validate(doc) {
			problems = append(problems, fmt.Sprintf("%s: %s", display, p))
		}
		if got, _ := doc["id"].(string); got != id {
			problems = append(problems, fmt.Sprintf("%s: id %q is not the path %q", display, got, id))
		}
		if first, dup := seen[id]; dup {
			problems = append(problems, fmt.Sprintf("%s: duplicate id, first in %s", display, first))
		}
		seen[id] = display
		cases = append(cases, Case{ID: id, Path: display, Doc: doc})
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })
	sort.Strings(problems)
	return cases, problems, nil
}
