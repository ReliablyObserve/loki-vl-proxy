package proxy

import (
	"context"
	"net/http"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// In -field-mapping-label-mode both, the stream labels of log lines and the
// labels of series carry every listed name of a mapped field (its aliases and
// its -label-style name) with the field's value. The names are added to the
// answer as it leaves the route, after every evaluation step (groupings,
// without, binary matching and label_replace run on one name per field, as in
// the alias mode), so series counts, values and matching stay the alias
// mode's. A grouped answer keeps only the labels its by() or on() names, as
// Loki answers it: a field a grouping or a matching names gets no other name,
// a name a without lists is never added, and no label an answer carries is
// overwritten (a label_replace target keeps its value).

// answersBothNames reports the mode that adds the names.
func (lt *LabelTranslator) answersBothNames() bool {
	return lt != nil && lt.labelMode == FieldMappingLabelModeBoth && len(lt.listing) > 0
}

// addBothNames gives labels, keyed by answer names, every listed name of each
// mapped field whose answer name it carries, except the names skip holds and
// the names labels already has. It reports whether it added one.
func (lt *LabelTranslator) addBothNames(labels map[string]string, skip map[string]bool) bool {
	added := false
	for field, names := range lt.listing {
		if len(names) < 2 {
			continue
		}
		value, ok := labels[lt.vlToLoki[field]]
		if !ok {
			continue
		}
		for _, name := range names {
			if _, has := labels[name]; !has && !skip[name] {
				labels[name] = value
				added = true
			}
		}
	}
	return added
}

// addBothNamesFJ is addBothNames for a label object of a JSON answer.
func (lt *LabelTranslator) addBothNamesFJ(obj *fj.Object, skip map[string]bool, arena *fj.Arena) bool {
	added := false
	for field, names := range lt.listing {
		if len(names) < 2 {
			continue
		}
		v := obj.Get(lt.vlToLoki[field])
		if v == nil || v.Type() != fj.TypeString {
			continue
		}
		for _, name := range names {
			if obj.Get(name) == nil && !skip[name] {
				obj.Set(name, arena.NewStringBytes(v.GetStringBytes()))
				added = true
			}
		}
	}
	return added
}

// bothNamesSkip returns the names no answer of query gains: every listed name
// of a mapped field that a grouping (by, on, group_left, group_right) names,
// and each name a without lists. all is true for a query whose plan has a node
// it does not read (or no plan), which gains no name.
func (p *Proxy) bothNamesSkip(ctx context.Context, query string) (skip map[string]bool, all bool) {
	if query == "" {
		return nil, false
	}
	plan, err := loweringPlan(ctx, logqlpkg.CanonicalizeLogRanges(query))
	if err != nil || plan == nil {
		return nil, true
	}
	lt := p.labelTranslator
	add := func(name string) {
		if skip == nil {
			skip = make(map[string]bool, 4)
		}
		skip[name] = true
	}
	grouped := func(labels []string) {
		for _, label := range labels {
			field, ok := lt.lokiToVL[label]
			if !ok {
				continue
			}
			for _, name := range lt.listing[field] {
				add(name)
			}
		}
	}
	grouping := func(g *queryplan.Grouping) {
		if g == nil {
			return
		}
		if !g.Without {
			grouped(g.Labels)
			return
		}
		for _, label := range g.Labels {
			add(label)
		}
	}
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch n := n.(type) {
		case *queryplan.LogSelect, *queryplan.Scalar, *queryplan.VectorLit:
		case *queryplan.RangeAgg:
			grouping(n.By)
		case *queryplan.VectorAgg:
			grouping(n.By)
			walk(n.In)
		case *queryplan.Rank:
			grouping(n.By)
			walk(n.In)
		case *queryplan.BinOp:
			if n.Match != nil {
				if n.Match.Card == "on" {
					grouped(n.Match.MatchLabels)
				} else {
					for _, label := range n.Match.MatchLabels {
						add(label)
					}
				}
				grouped(n.Match.Include)
			}
			walk(n.L)
			walk(n.R)
		case *queryplan.LabelReplace:
			walk(n.In)
		default:
			all = true
		}
	}
	walk(plan.Root)
	return skip, all
}

// bothNamesParsers and bothNamesArenas are reused across answers.
var (
	bothNamesParsers fj.ParserPool
	bothNamesArenas  fj.ArenaPool
)

// withBothNamesBody adds the names (addBothNames) to every stream and series
// label object of a Loki answer: data.result[].stream, data.result[].metric
// and the /series data[]. An answer it cannot read, or that gains no name, is
// returned as it is.
func (lt *LabelTranslator) withBothNamesBody(body []byte, skip map[string]bool) []byte {
	parser := bothNamesParsers.Get()
	defer bothNamesParsers.Put(parser)
	v, err := parser.ParseBytes(body)
	if err != nil || string(v.GetStringBytes("status")) != "success" {
		return body
	}
	arena := bothNamesArenas.Get()
	defer bothNamesArenas.Put(arena)
	added := false
	objects := v.GetArray("data")
	if objects == nil {
		for _, item := range v.GetArray("data", "result") {
			if o := item.GetObject("stream"); o != nil {
				objects = append(objects, item.Get("stream"))
			} else if o := item.GetObject("metric"); o != nil {
				objects = append(objects, item.Get("metric"))
			}
		}
	}
	for _, o := range objects {
		if obj, err := o.Object(); err == nil && lt.addBothNamesFJ(obj, skip, arena) {
			added = true
		}
	}
	if !added {
		return body
	}
	return v.MarshalTo(make([]byte, 0, len(body)+len(body)/8))
}

// bothNamesLayer adds the names to the answers of a query, query_range or
// series route in the both mode: it reads the plain answer (Accept-Encoding
// removed), adds the names once, outside the cache, so a cached answer gets
// them on every hit, and writes it. The other modes pass through untouched.
func (p *Proxy) bothNamesLayer(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	if endpoint != "query" && endpoint != "query_range" && endpoint != "series" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		lt := p.labelTranslator
		if !lt.answersBothNames() {
			next.ServeHTTP(w, r)
			return
		}
		var skip map[string]bool
		if endpoint != "series" {
			var all bool
			if skip, all = p.bothNamesSkip(r.Context(), r.FormValue("query")); all {
				next.ServeHTTP(w, r)
				return
			}
		}
		inner := r.Clone(r.Context())
		inner.Header.Del("Accept-Encoding")
		bw := &bufferedResponseWriter{header: w.Header()}
		sc := &statusCapture{ResponseWriter: bw, code: http.StatusOK}
		next.ServeHTTP(sc, inner)
		body := bw.body
		if sc.code == http.StatusOK && w.Header().Get("Content-Encoding") == "" {
			if out := lt.withBothNamesBody(body, skip); len(out) != len(body) {
				w.Header().Del("Content-Length")
				body = out
			}
		}
		if sc.code != http.StatusOK {
			w.WriteHeader(sc.code)
		}
		_, _ = w.Write(body)
	}
}
