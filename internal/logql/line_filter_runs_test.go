package logql

import (
	"strconv"
	"strings"
	"testing"
)

// A chain of line filters is answered from the answers of its filters alone: the first segment (split by a label
// filter, line_format or unpack) that holds an invalid filter is reported, by its last invalid filter, quoting every
// filter of that segment. Each filter alone is held to Loki by TestLineFilterRunValidationMatchesLoki.
func FuzzLineFilterRunMatchesItsFilters(f *testing.F) {
	f.Add([]byte{0, 14, 27, 40}, "(")
	f.Add([]byte{2, 3, 19, 200, 9}, "1.2.3.4")
	f.Add([]byte{5, 77, 1, 130}, "<_><_>")
	f.Add([]byte{8, 99, 150, 7}, "<a>")
	f.Add([]byte{1, 6, 25, 31}, "z")
	f.Fuzz(func(t *testing.T, choice []byte, v string) {
		if len(choice) > 8 {
			choice = choice[:8]
		}
		ops := []string{"|=", "!=", "|~", "!~", "|>", "!>"}
		seps := []struct {
			text string
			ends bool
		}{{"", false}, {"", false}, {`| json`, false}, {`| label_format a="b"`, false}, {`| x="1"`, true},
			{`| line_format "x"`, true}, {`| unpack`, true}}
		operand := func(k int) string {
			switch k % 4 {
			case 0:
				return strconv.Quote(v)
			case 1:
				return "ip(" + strconv.Quote(v) + ")"
			case 2:
				return strconv.Quote(v) + ` or "("`
			}
			return `"["`
		}
		// segments holds the filters of each segment, as written.
		segments := [][]string{nil}
		var q strings.Builder
		q.WriteString(`{app="a"}`)
		for _, c := range choice {
			filter := ops[int(c)%len(ops)] + " " + operand(int(c)/len(ops))
			sep := seps[int(c)/24%len(seps)]
			q.WriteString(" " + filter)
			segments[len(segments)-1] = append(segments[len(segments)-1], filter)
			if sep.text != "" {
				q.WriteString(" " + sep.text)
				if sep.ends {
					segments = append(segments, nil)
				}
			}
		}
		want := ""
		for _, segment := range segments {
			printed := make([]string, len(segment))
			reported := ""
			for i, filter := range segment {
				alone := `{app="a"} ` + filter
				lq, err := ParseLogQuery(alone)
				if err != nil {
					t.Skip() // a filter Loki does not parse
				}
				printed[i] = lq.Pipeline[0].(*LineFilterStage).flatString()
				if err := ParseAndValidate(alone); err != nil {
					prefix := "parse error : stage '" + printed[i] + "' : "
					if !strings.HasPrefix(err.Error(), prefix) {
						t.Fatalf("%s: %v does not quote the filter as %q", alone, err, printed[i])
					}
					reported = strings.TrimPrefix(err.Error(), prefix)
				}
			}
			if reported != "" {
				want = "parse error : stage '" + strings.Join(printed, " ") + "' : " + reported
				break
			}
		}
		got := ""
		if err := ParseAndValidate(q.String()); err != nil {
			got = err.Error()
		}
		if got != want {
			t.Fatalf("%s:\n got  %s\n want %s", q.String(), got, want)
		}
	})
}
