package translator

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestLineFilterLiteralTranslation(t *testing.T) {
	translations := map[string]func(string) (string, error){
		"string": TranslateLogQL,
	}
	for name, translate := range translations {
		for _, literal := range []string{"ip(bad)", "a.b", "[error]", "a|b", "^a$", ".*", "a+b?", "{x}", `C:\logs\`, "line\nnext", `say "hello"`} {
			for _, op := range []string{"|=", "!="} {
				for _, quoted := range []string{strconv.Quote(literal), "`" + literal + "`"} {
					t.Run(name+"/"+op+quoted, func(t *testing.T) {
						out, err := translate(`{app="a"} ` + op + quoted)
						if err != nil {
							t.Fatal(err)
						}
						pattern, err := strconv.Unquote(out[strings.Index(out, "~")+1:])
						if err != nil {
							t.Fatalf("invalid translated pattern %q: %v", out, err)
						}
						re, err := regexp.Compile(pattern)
						if err != nil {
							t.Fatal(err)
						}
						for _, line := range []string{literal, "prefix " + literal + " suffix", "ipbad", "axb", "error", "a", "b", "", `C:logs`} {
							if got, want := re.MatchString(line), strings.Contains(line, literal); got != want {
								t.Fatalf("pattern %q on %q: match %v, want %v", pattern, line, got, want)
							}
						}
						if strings.Contains(out, "NOT ~") != (op == "!=") {
							t.Fatalf("lost negation: %q", out)
						}
					})
				}
			}
		}
		for _, op := range []string{"|~", "!~"} {
			t.Run(name+"/regex/"+op, func(t *testing.T) {
				out, err := translate(`{app="a"} ` + op + ` "a.b"`)
				if err != nil || !strings.Contains(out, `~"a.b"`) {
					t.Fatalf("regex semantics changed: %q, %v", out, err)
				}
			})
		}
	}
}

func TestLineFilterEscapedBackslashDoesNotSwallowNextStage(t *testing.T) {
	out, err := TranslateLogQL(`{app="a"} |= "C:\\" != "[ignore]"`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `~"C:\\\\" NOT ~"\\[ignore\\]"`) {
		t.Fatalf("literal boundary or escaping lost: %s", out)
	}
}
