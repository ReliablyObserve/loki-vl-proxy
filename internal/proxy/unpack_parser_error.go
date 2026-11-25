package proxy

import (
	"errors"

	"github.com/grafana/jsonparser"
)

// errUnpackNotObject is Loki's __error_details__ of a line an unpack rejects because it opens with no brace.
var errUnpackNotObject = errors.New("expecting json object(6), but it is not")

// unpackLineError is the error Loki's unpack sets on a line, nil when it sets none: an empty line goes on as it is,
// a line that does not open with a brace is no JSON object, and an object fails where reading its members or one of
// its string values does (recorded answers: testdata/unpack_error_lines.json).
func unpackLineError(line string) error {
	if line == "" {
		return nil
	}
	if line[0] != '{' {
		return errUnpackNotObject
	}
	return jsonparser.ObjectEach([]byte(line), func(_, value []byte, kind jsonparser.ValueType, _ int) error {
		if kind == jsonparser.String {
			_, err := jsonparser.ParseString(value)
			return err
		}
		return nil
	})
}

// unwrapPackedLabels adds the labels an unpack reads from a line: the string members of a line a packing agent
// wrote, an object whose top-level `_entry` is a string, and none of any other line.
func unwrapPackedLabels(line string, add func(name, value string, last bool)) {
	if _, kind, _, err := jsonparser.Get([]byte(line), "_entry"); err != nil || kind != jsonparser.String {
		return
	}
	_ = jsonparser.ObjectEach([]byte(line), func(key, value []byte, kind jsonparser.ValueType, _ int) error {
		if kind == jsonparser.String && string(key) != "_entry" {
			add(orderedJSONLabelName(string(key), true), orderedJSONScalar(value, kind), true)
		}
		return nil
	})
}
