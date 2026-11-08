package logql

import (
	"sort"
	"text/template"
	"text/template/parse"
)

// LineTemplate is a line_format template parsed with the same function set
// validation uses, so it exists exactly when Loki accepts the template.
type LineTemplate struct {
	// Canonical is the parse tree printed back: spacing inside actions,
	// comments and trim markers do not change it, and parsing it again gives
	// the same tree. A template with define or block keeps its text.
	Canonical string
	// Fields are the distinct names read as {{.name}} (or $.name), in first
	// use order. Inside range and with the dot is another value, so the list
	// can name more than the labels the template reads, never fewer.
	Fields []string
	// Funcs are the distinct functions called, __line__ and __timestamp__
	// included, in first use order.
	Funcs []string
	// ReadsDot is true when the template uses the whole label map ({{.}},
	// {{index . "a"}}), so Fields does not list every label it can read.
	ReadsDot bool
	// Simple is true when the template is text and {{.name}} actions only.
	Simple bool
}

// typeLineTemplate parses a line_format template; nil when it does not parse
// (validation reports Loki's error for it).
func typeLineTemplate(text string) *LineTemplate {
	t, err := template.New("line").Option("missingkey=zero").Funcs(lokiTemplateFunctions).Parse(text)
	if err != nil || t.Tree == nil {
		return nil
	}
	lt := &LineTemplate{Canonical: text, Simple: true}
	if len(t.Templates()) == 1 {
		lt.Canonical = t.Tree.Root.String()
	} else {
		lt.Simple = false
	}
	w := templateWalker{lt: lt, fields: map[string]bool{}, funcs: map[string]bool{}}
	for _, n := range t.Tree.Root.Nodes {
		switch x := n.(type) {
		case *parse.TextNode:
		case *parse.ActionNode:
			if !simpleFieldAction(x) {
				lt.Simple = false
			}
		default:
			lt.Simple = false
		}
	}
	w.walk(t.Tree.Root)
	defs := t.Templates()
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name() < defs[j].Name() })
	for _, d := range defs {
		if d != t && d.Tree != nil {
			w.walk(d.Tree.Root)
		}
	}
	return lt
}

// simpleFieldAction reports whether a is {{.name}}: one field, no pipe, no
// declaration.
func simpleFieldAction(a *parse.ActionNode) bool {
	if len(a.Pipe.Decl) != 0 || len(a.Pipe.Cmds) != 1 || len(a.Pipe.Cmds[0].Args) != 1 {
		return false
	}
	f, ok := a.Pipe.Cmds[0].Args[0].(*parse.FieldNode)
	return ok && len(f.Ident) == 1
}

type templateWalker struct {
	lt     *LineTemplate
	fields map[string]bool
	funcs  map[string]bool
}

func (w *templateWalker) field(name string) {
	if !w.fields[name] {
		w.fields[name] = true
		w.lt.Fields = append(w.lt.Fields, name)
	}
}

func (w *templateWalker) walk(n parse.Node) {
	switch x := n.(type) {
	case *parse.ListNode:
		if x == nil {
			return
		}
		for _, c := range x.Nodes {
			w.walk(c)
		}
	case *parse.ActionNode:
		w.walk(x.Pipe)
	case *parse.PipeNode:
		if x == nil {
			return
		}
		for _, c := range x.Cmds {
			w.walk(c)
		}
	case *parse.CommandNode:
		for _, a := range x.Args {
			w.walk(a)
		}
	case *parse.FieldNode:
		w.field(x.Ident[0])
	case *parse.VariableNode:
		// $ is the label map at the top: $.name reads a label.
		if x.Ident[0] == "$" && len(x.Ident) > 1 {
			w.field(x.Ident[1])
		} else if x.Ident[0] == "$" {
			w.lt.ReadsDot = true
		}
	case *parse.DotNode:
		w.lt.ReadsDot = true
	case *parse.ChainNode:
		w.walk(x.Node)
	case *parse.IdentifierNode:
		if !w.funcs[x.Ident] {
			w.funcs[x.Ident] = true
			w.lt.Funcs = append(w.lt.Funcs, x.Ident)
		}
	case *parse.IfNode:
		w.branch(&x.BranchNode)
	case *parse.RangeNode:
		w.branch(&x.BranchNode)
	case *parse.WithNode:
		w.branch(&x.BranchNode)
	case *parse.TemplateNode:
		w.walk(x.Pipe)
	}
}

func (w *templateWalker) branch(b *parse.BranchNode) {
	w.walk(b.Pipe)
	w.walk(b.List)
	w.walk(b.ElseList)
}
