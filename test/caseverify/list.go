package main

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

// Classification is what the runner does with each case before it sends anything: the cases it sends (the
// default proxy or a variant), those it cannot (by class) and the scenarios that are no request.
type Classification struct {
	Sent          []string            // sent to the default proxy
	SentToVariant map[string][]string // variant -> cases
	Blocked       map[string][]string // class -> cases
	NotExecutable []string
}

// Classify decides every probe's fate without a request: the same decisions Verify makes before it sends.
func Classify(cases []registry.Case, defaultFlags []string, variants []Variant, now func() time.Time) Classification {
	c := Classification{SentToVariant: map[string][]string{}, Blocked: map[string][]string{}}
	for _, p := range Probes(cases) {
		ChooseVariant(&p, defaultFlags, variants)
		switch {
		case p.NotExecutable != "":
			c.NotExecutable = append(c.NotExecutable, p.ID)
		case p.Blocked != "":
			c.Blocked[p.BlockedClass] = append(c.Blocked[p.BlockedClass], p.ID)
		default:
			if _, why := p.Query(now()); why != "" {
				c.Blocked[classPlaceholder] = append(c.Blocked[classPlaceholder], p.ID)
			} else if p.Variant != "" {
				c.SentToVariant[p.Variant] = append(c.SentToVariant[p.Variant], p.ID)
			} else {
				c.Sent = append(c.Sent, p.ID)
			}
		}
	}
	return c
}

func list(cases []registry.Case, defaultFlags []string, variants []Variant, out io.Writer) int {
	c := Classify(cases, defaultFlags, variants, time.Now)
	fmt.Fprintf(out, "sent to the default proxy: %d\n", len(c.Sent))
	names := make([]string, 0, len(c.SentToVariant))
	for v := range c.SentToVariant {
		names = append(names, v)
	}
	sort.Strings(names)
	for _, v := range names {
		fmt.Fprintf(out, "sent to variant %s: %d\n", v, len(c.SentToVariant[v]))
	}
	classes := make([]string, 0, len(c.Blocked))
	total := 0
	for k, ids := range c.Blocked {
		classes = append(classes, k)
		total += len(ids)
	}
	sort.Strings(classes)
	fmt.Fprintf(out, "blocked before sending: %d\n", total)
	for _, k := range classes {
		fmt.Fprintf(out, "  %s: %d\n", k, len(c.Blocked[k]))
		for _, id := range c.Blocked[k] {
			fmt.Fprintf(out, "    %s\n", id)
		}
	}
	fmt.Fprintf(out, "not executable (no request): %d\n", len(c.NotExecutable))
	return 0
}
