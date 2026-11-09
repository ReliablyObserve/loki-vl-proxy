package logql

import "strings"

// lokiLexerQueries is the grid of the classes of Loki's lexer that reach a number literal besides the scan of the
// literal itself (numLitQueries): a binary operator after a label filter, the words Loki reads as keywords, the
// characters that are tokens of their own, NUL, and number literals outside label filters (a metric comparison,
// a vector, a quantile, topk, offset, line filters, a stage).
func lokiLexerQueries() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	const pre = `{app="a"} | json | `
	metric := `sum(rate({a="b"}[5m]))`

	// A binary operator after a label filter belongs to the query, whose left operand is the pipeline.
	operators := []string{"+", "-", "*", "/", "%", "^", "==", ">", ">=", "<", "<=", "unless", "and", "or", "!=", "="}
	rights := []string{"", " 3", "3", " x", " (", ` "s"`, " 3m", " 5B", " " + metric, " 3 | json", " 3 and B>1", " bool 3", " on(a) 3", ` {app="b"}`, ` {app="b"} | json`, " -3", " 0x1", " 0x", " 3 + 4"}
	for _, value := range []string{"5", "5m", "5kB", "0x1e", "1e3", `"x"`, "-5", ".5", "1_0", "0b1", "0x", "5x"} {
		for _, op := range operators {
			for _, right := range rights {
				for _, gap := range []string{"", " "} {
					add(pre + "A<" + value + gap + op + right)
					add(pre + "A=" + value + gap + op + right)
				}
			}
		}
	}
	for _, q := range []string{
		`{app="a"} + 3`, `{app="a"} | json + 3`, `{app="a"} |= "x" + 3`, `{app="a"} +`, `{app="a"} + on(a) 3`, `({app="a"}) + 3`,
		`1 + {app="a"}`, `1 + {app="a"} | json | A<5`, metric + ` + {app="a"}`, metric + ` and {app="a"} | json`, `1 and {app="a"}`, `1 and 3`,
		`{app="a"} and 3`, `{app="a"} unless {app="b"}`, `{app="a"} | json | (A<5 + 3)`, `{app="a"} | json | (A<5) + 3`,
		`{app="a"} | json | A<5, B<6 + 3`, `{app="a"} | json | A<5 and B<6 or C>1 * 2`, `{app="a"} | json | A<5 + 3 > bool 4`,
		`{app="a"} | json | A<5 + 3 | json`, `{app="a"} | json | A<5 + 3 |= "x"`,
	} {
		add(q)
	}

	// Words that are keyword tokens for Loki's lexer.
	keywords := []string{"by", "without", "bool", "offset", "on", "ignoring", "group_left", "group_right", "unless", "unwrap", "label_replace", "json", "regexp", "logfmt", "unpack", "pattern", "label_format", "line_format", "ip", "decolorize", "drop", "keep", "variants", "of", "BY", "Of", "rate", "count_over_time", "sum", "topk", "vector", "bytes", "duration", "duration_seconds", "abs", "sort", "avg", "min", "distinct", "foo", "Rate", "bytes_rate", "rate_counter", "quantile_over_time", "approx_topk", "status"}
	for _, k := range keywords {
		for _, t := range []string{"A=%s", "A=1 %s", "A<5%s", "A=1 and %s=1", "A=%s(1)", "A=1 %s(1)=1", "A=1 %s (1)", "(A=1 %s=1)", "A=1 %s by (x)", `A="x" %s`, "A=1, %s>2"} {
			add(pre + strings.ReplaceAll(t, "%s", k))
		}
	}

	// Characters that are tokens of their own, NUL among them.
	chars := []string{":", ";", "@", "$", "\\", "?", "~", "&", "{", "}", "[", "]", "'", "_", "é", "€", "\v", "\f", "\x01", "\x7f", "\u00a0", "\u200b", "\x00", "[5m]", "[", "'a'", "'ab'", "''", "'\\n'"}
	for _, c := range chars {
		for _, t := range []string{"A=1 x%sy=2", "A!=1y x%sf", "A=x%sy", "A=1 x%s", "A=1 %s", "A<5%s", "A<5%sx", "A<5 %s", "A<5x%sy", "(A=1 %s)", `A="x"%s`, `A="x" %s`, "A=1\n%s", "A=1 # c\n%s", "A=1 and %s=1", "A<5 \t %s B>1"} {
			add(pre + strings.ReplaceAll(t, "%s", c))
		}
	}

	// Number literals outside label filters.
	values := []string{"0x1", "0b1", " 0b1", "0o7", "1_0", "1e3", ".5", "5.", "0x1p3", "5x", "5kb", "5B", "5m", "1h30", "07", "00", "1e999", "0x", "1e", "08", "1__0", "5kB", "5.5.5", "0B1", "1.5h", "0X", "0o", "1_", "0b", "-5m", "-0x1"}
	contexts := []string{metric + " > %s", metric + " > bool %s", metric + " + %s", "%s + 1", "1 + %s", "vector(%s)", `quantile_over_time(%s, {app="a"} | unwrap x [5m])`, "topk(%s, " + metric + ")", `rate({app="a"}[5m] offset %s)`, `{app="a"} |= %s`, `{app="a"} != %s`, `{app="a"} |~ %s`, `{app="a"} | %s`, `{app="a"} | json | %s`, metric + " >  %s", metric + " >\n%s", metric + " > # c\n%s", metric + ">%s"}
	for _, ctx := range contexts {
		for _, v := range values {
			add(strings.ReplaceAll(ctx, "%s", v))
		}
	}
	return out
}
