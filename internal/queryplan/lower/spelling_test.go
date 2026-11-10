package lower

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// A value the string translator copies as the client wrote it (a regexp line filter, an exact matcher on a stream
// field) is lowered only when the lowering's quoting of the value is that spelling: the AST holds the value, so a
// literal written with an escape, a raw control character or backquotes is delegated to the string translator
// under "LogSelect escaped spelling", where the shadow counts it as delegated and the proxy serves the
// translator's text.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesAValueTheTranslatorCopiesAsWritten(t *testing.T) {
	stream := Profile{StreamFields: map[string]bool{"app": true, "env": true}}
	for _, c := range []struct {
		name, q string
		prof    Profile
	}{
		{"hex escape in a regexp filter", `{app="a"} |~ "\x41b"`, Profile{}},
		{"hex escape in a negated regexp filter", `{app="a"} !~ "\x41b"`, Profile{}},
		{"hex escape in a stream filter", `{app="\x41"}`, stream},
		{"U escape in a regexp filter", `{app="a"} |~ "\U00000041b"`, Profile{}},
		{"octal escape in a regexp filter", `{app="a"} |~ "\101b"`, Profile{}},
		{"octal escape in a stream filter", `{app="\101"}`, stream},
		{"escape in the second of two regexp filters", `{app="a"} |~ "b" !~ "\x41"`, Profile{}},
		{"escape among other stages", `{app="a"} |= "x" |~ "\x41" | b="1" |> "<_> y"`, Profile{}},
		{"escape on the second stream field", `{app="a", env="\x41"}`, stream},
		{"escape on a stream field after a plain one", `{env="p", app="\x61"}`, stream},
		{"simple escape the stream filter writes raw", `{app="\t"}`, stream},
		{"NUL in a stream filter", `{app="\x00"}`, stream},
		{"bell in a stream filter", `{app="\a"}`, stream},
		{"backquoted stream filter", "{app=`a`}", stream},
		{"raw control character in a regexp filter", "{app=\"a\"} |~ \"a\tb\"", Profile{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			op, err := Plan(planOf(t, c.q), c.prof)
			var nl *NotLoweredError
			if op != nil || !errors.Is(err, ErrNotLowered) || !errors.As(err, &nl) || nl.Construct != "LogSelect escaped spelling" {
				t.Fatalf("Plan(%s) = %v, %v; want ErrNotLowered naming LogSelect escaped spelling", c.q, op, err)
			}
		})
	}
}

// Where the string translator quotes the value, or copies a spelling the lowering writes alike, the lowering keeps
// the query whatever the escape: it is not delegated, and its text is the one the translator writes.
//
// conformance: architecture-syntax-package-layering
func TestPlanKeepsAValueTheTranslatorQuotes(t *testing.T) {
	stream := Profile{StreamFields: map[string]bool{"app": true}}
	for _, c := range []struct {
		name, q string
		prof    Profile
		want    string
	}{
		{"hex escape in a substring filter", `{app="a"} |= "\x41b"`, Profile{}, `app:="a" ~"Ab"`},
		{"hex escape in a negated substring filter", `{app="a"} != "\x41b"`, Profile{}, `app:="a" NOT ~"Ab"`},
		{"hex escape in an or chain of regexps", `{app="a"} |~ "\x41" or "b"`, Profile{}, `app:="a" ~"(?:A)|(?:b)"`},
		{"hex escape in a negated or chain", `{app="a"} !~ "b" or "\x41"`, Profile{}, `app:="a" NOT ~"(?:b)|(?:A)"`},
		{"hex escape in a label filter", `{app="a"} | b="\x41"`, Profile{}, `app:="a" b:="A"`},
		{"hex escape in a label regexp", `{app="a"} | b=~"\x41"`, Profile{}, `app:="a" b:="A"`},
		{"hex escape in a pattern filter", `{app="a"} |> "<_> \x41 <_>"`, Profile{}, ``},
		{"hex escape in a field filter selector", `{app="\x41"}`, Profile{}, `app:="A"`},
		{"hex escape on a label that is no stream field", `{b="\x41"}`, stream, `b:="A"`},
		{"hex escape in a not-equal matcher", `{app!="\x41"}`, stream, `-app:="A"`},
		{"hex escape in a regexp matcher", `{app=~"\x41.*"}`, stream, ``},
		{"quote and backslash escapes in a regexp filter", `{app="a"} |~ "a\"b\\d+"`, Profile{}, `app:="a" ~"a\"b\\d+"`},
		{"quote and backslash in a stream filter", `{app="a\"b\\c"}`, stream, `{app="a\"b\\c"}`},
		{"tab and newline escapes in a regexp filter", `{app="a"} |~ "a\tb\n"`, Profile{}, `app:="a" ~"a\tb\n"`},
		{"non-ASCII in a regexp filter", `{app="a"} |~ "żółć"`, Profile{}, `app:="a" ~"żółć"`},
		{"non-ASCII in a stream filter", `{app="żółć"}`, stream, `{app="żółć"}`},
		{"backquoted regexp filter", "{app=\"a\"} |~ `a\\d+`", Profile{}, `app:="a" ~"a\\d+"`},
		{"backquoted field filter selector", "{app=`a`}", Profile{}, `app:="a"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			op, err := Plan(planOf(t, c.q), c.prof)
			var nl *NotLoweredError
			if errors.As(err, &nl) && nl.Construct == "LogSelect escaped spelling" {
				t.Fatalf("Plan(%s) was delegated for its spelling", c.q)
			}
			if err != nil {
				return // delegated for another reason: not this test's concern
			}
			if got, ok := op.(physical.VLQuery); !ok || (c.want != "" && got.Text != c.want) {
				t.Fatalf("Plan(%s) = %#v, want %s", c.q, op, c.want)
			}
		})
	}
}

// spell writes v as a double-quoted (or back-quoted) LogQL literal in the spelling mode selects: Go's quoting, every
// byte as \xNN or as octal, every rune as a Unicode escape, the printable text as it is with the rest escaped, the
// text raw, or backquotes. A value that cannot be written the way (backquotes around a backquote) falls back to
// Go's quoting.
func spell(v string, mode uint8) string {
	const bs = `\`
	var b strings.Builder
	switch mode % 7 {
	case 1:
		b.WriteByte('"')
		for i := 0; i < len(v); i++ {
			fmt.Fprintf(&b, bs+"x%02x", v[i])
		}
		b.WriteByte('"')
	case 2:
		b.WriteByte('"')
		for _, r := range v {
			if r < 0x10000 {
				fmt.Fprintf(&b, bs+"u%04x", r)
			} else {
				fmt.Fprintf(&b, bs+"U%08x", r)
			}
		}
		b.WriteByte('"')
	case 3:
		b.WriteByte('"')
		for i := 0; i < len(v); i++ {
			fmt.Fprintf(&b, bs+"%03o", v[i])
		}
		b.WriteByte('"')
	case 4:
		if !strings.Contains(v, "`") {
			return "`" + v + "`"
		}
	case 5:
		b.WriteByte('"')
		for i := 0; i < len(v); i++ {
			switch c := v[i]; {
			case c == '"' || c == '\\':
				b.WriteString(bs + string(c))
			case c >= 0x20 && c < 0x7f:
				b.WriteByte(c)
			default:
				fmt.Fprintf(&b, bs+"x%02x", c)
			}
		}
		b.WriteByte('"')
	case 6:
		b.WriteByte('"')
		for i := 0; i < len(v); i++ {
			if c := v[i]; c == '"' || c == '\\' {
				b.WriteString(bs)
			}
			b.WriteByte(v[i])
		}
		b.WriteByte('"')
	default:
		return strconv.Quote(v)
	}
	return b.String()
}

// FuzzEscapedSpellingLowering holds the lowering to the string translator on a value written in any spelling, in
// each position a filter-only log query holds one, with and without stream fields: both write the same bytes, or
// the lowering leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzEscapedSpellingLowering(f *testing.F) {
	const bs = `\`
	f.Add("Ab", uint8(1), uint8(0))
	f.Add("A", uint8(1), uint8(3))
	f.Add("A\tb", uint8(5), uint8(3))
	f.Add("\x00\x7f", uint8(1), uint8(4))
	f.Add("é日\U0001F600", uint8(2), uint8(1))
	f.Add("Ab", uint8(3), uint8(5))
	f.Add("a.b", uint8(6), uint8(6))
	f.Add("a\tb", uint8(6), uint8(0))
	f.Add("a`b", uint8(4), uint8(3))
	f.Add("x\"y"+bs+"z", uint8(0), uint8(7))
	f.Add("A", uint8(1), uint8(8))
	f.Add("A", uint8(1), uint8(9))
	f.Fuzz(func(t *testing.T, v string, mode, shape uint8) {
		l := spell(v, mode)
		var q string
		switch shape % 10 {
		case 0:
			q = `{app="a"} |~ ` + l
		case 1:
			q = `{app="a"} !~ ` + l
		case 2:
			q = `{app="a"} |= ` + l
		case 3:
			q = `{app=` + l + `}`
		case 4:
			q = `{app=` + l + `, env="x"}`
		case 5:
			q = `{b=` + l + `} |~ ` + l
		case 6:
			q = `{app="a"} |~ ` + l + ` or "z"`
		case 7:
			q = `{app="a"} | b=~` + l
		case 8:
			q = `{app=~` + l + `}`
		default:
			q = `{app!=` + l + `}`
		}
		expr, err := logql.Parse(q)
		if err != nil {
			return
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			return
		}
		queryplan.Resolve(p)
		for name, stream := range map[string]map[string]bool{"no stream fields": nil, "stream fields": {"app": true, "env": true}} {
			legacy, terr := translator.TranslateLogQLWithCapabilities(q, nil, stream, logsql.Capabilities{})
			op, lerr := Plan(p, Profile{StreamFields: stream})
			if errors.Is(lerr, ErrNotLowered) {
				continue
			}
			if terr != nil || lerr != nil {
				if (terr == nil) != (lerr == nil) {
					t.Fatalf("%s: %s: translator error %v, lowering error %v", name, q, terr, lerr)
				}
				continue
			}
			if got := op.(physical.VLQuery).Text; got != legacy {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", name, q, got, legacy)
			}
		}
	})
}
