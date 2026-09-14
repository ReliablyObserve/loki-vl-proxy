package proxy

import (
	"context"
	"fmt"
	"strings"
	"text/template"
	"text/template/parse"
)

const maxFormattedLineBytes = 64 << 10
const maxFormattedResponseBytes = 16 << 20

type templateOutput struct {
	strings.Builder
	remaining int
}

func (b *templateOutput) Write(p []byte) (int, error) {
	if len(p) > b.remaining {
		return 0, fmt.Errorf("line_format output limit exceeded")
	}
	b.remaining -= len(p)
	return b.Builder.Write(p)
}

// Bound intermediate formatting too: limiting only Execute's writer is too
// late for printf widths, replacement expansion, or nested function pipelines.
func boundedTemplatePrintf(format string, args ...any) (string, error) {
	if len(format) > maxFormattedLineBytes || !templateArgsWithinBudget(args) {
		return "", fmt.Errorf("line_format printf limit exceeded")
	}
	// Bounds numeric widths/precisions and argument indexes before fmt allocates.
	number := 0
	inDirective := false
	for _, c := range format {
		if !inDirective {
			if c == '%' {
				inDirective = true
			}
			continue
		}
		if c == '%' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			inDirective = false
			number = 0
			continue
		}
		if c >= '0' && c <= '9' {
			number = number*10 + int(c-'0')
			if number > maxFormattedLineBytes {
				return "", fmt.Errorf("line_format printf width limit exceeded")
			}
		} else {
			number = 0
		}
	}
	for _, arg := range args {
		if n, ok := arg.(int); ok && strings.Contains(format, "*") && (n > maxFormattedLineBytes || n < -maxFormattedLineBytes) {
			return "", fmt.Errorf("line_format printf width limit exceeded")
		}
	}
	w := &templateOutput{remaining: maxFormattedLineBytes}
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		return "", err
	}
	return w.String(), nil
}

func templateArgsWithinBudget(args []any) bool {
	if len(args) > 100 {
		return false
	}
	size := 0
	for _, arg := range args {
		switch value := arg.(type) {
		case string:
			size += len(value)
		case map[string]string:
			for k, v := range value {
				size += len(k) + len(v) + 8
				if size > 1<<20 {
					return false
				}
			}
		default:
			size += 64
		}
		if size > 1<<20 {
			return false
		}
	}
	return true
}

func boundedTemplatePrint(newline bool, args ...any) (string, error) {
	if !templateArgsWithinBudget(args) {
		return "", fmt.Errorf("line_format print input limit exceeded")
	}
	w := &templateOutput{remaining: maxFormattedLineBytes}
	var err error
	if newline {
		_, err = fmt.Fprintln(w, args...)
	} else {
		_, err = fmt.Fprint(w, args...)
	}
	return w.String(), err
}

func boundedTemplateEscape(fn func(string) string) func(...any) (string, error) {
	return func(args ...any) (string, error) {
		s, err := boundedTemplatePrint(false, args...)
		if err != nil {
			return "", err
		}
		return boundedTemplateString(fn)(s)
	}
}

func boundedTemplateReplace(s, old, replacement string, n int) (string, error) {
	count := strings.Count(s, old)
	if n >= 0 && n < count {
		count = n
	}
	if growth := len(replacement) - len(old); growth > 0 && count > (maxFormattedLineBytes-len(s))/growth {
		return "", fmt.Errorf("line_format replacement limit exceeded")
	}
	result := strings.Replace(s, old, replacement, n)
	if len(result) > maxFormattedLineBytes {
		return "", fmt.Errorf("line_format replacement limit exceeded")
	}
	return result, nil
}

func boundedTemplateString(fn func(string) string) func(string) (string, error) {
	return func(s string) (string, error) {
		if len(s) > maxFormattedLineBytes {
			return "", fmt.Errorf("line_format function input limit exceeded")
		}
		out := fn(s)
		if len(out) > maxFormattedLineBytes {
			return "", fmt.Errorf("line_format function output limit exceeded")
		}
		return out, nil
	}
}

// Insert a cheap budget check at every executable list entry. In particular an
// empty-output range or recursive template must consume work budget too.
func instrumentTemplateBudget(tmpl *template.Template, ctx context.Context) error {
	remaining := 100000
	check := func() (string, error) {
		remaining--
		if remaining < 0 {
			return "", fmt.Errorf("line_format execution limit exceeded")
		}
		return "", ctx.Err()
	}
	tmpl.Funcs(template.FuncMap{"__proxy_budget": check})
	probe, err := template.New("budget").Funcs(template.FuncMap{"__proxy_budget": check}).Parse("{{__proxy_budget}}")
	if err != nil {
		return err
	}
	nodes := 0
	var walk func(*parse.ListNode, int) error
	walk = func(list *parse.ListNode, depth int) error {
		if list == nil {
			return nil
		}
		if depth > 64 {
			return fmt.Errorf("line_format nesting limit exceeded")
		}
		original := list.Nodes
		list.Nodes = make([]parse.Node, 0, 2*len(original)+1)
		list.Nodes = append(list.Nodes, probe.Tree.Root.Nodes[0].Copy())
		for _, node := range original {
			nodes++
			if nodes > 1024 {
				return fmt.Errorf("line_format syntax limit exceeded")
			}
			var branch *parse.BranchNode
			switch n := node.(type) {
			case *parse.IfNode:
				branch = &n.BranchNode
			case *parse.RangeNode:
				branch = &n.BranchNode
			case *parse.WithNode:
				branch = &n.BranchNode
			}
			if branch != nil {
				if err := walk(branch.List, depth+1); err != nil {
					return err
				}
				if err := walk(branch.ElseList, depth+1); err != nil {
					return err
				}
			}
			list.Nodes = append(list.Nodes, probe.Tree.Root.Nodes[0].Copy(), node)
		}
		return nil
	}
	for _, part := range tmpl.Templates() {
		if err := walk(part.Root, 0); err != nil {
			return err
		}
	}
	return nil
}
