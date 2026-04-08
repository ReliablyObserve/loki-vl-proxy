package rulesmigrate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"gopkg.in/yaml.v3"
)

type RuleFile struct {
	Groups []Group `yaml:"groups"`
}

type Group struct {
	Name     string   `yaml:"name"`
	Interval string   `yaml:"interval,omitempty"`
	Type     string   `yaml:"type,omitempty"`
	Headers  []string `yaml:"headers,omitempty"`
	Rules    []Rule   `yaml:"rules"`
}

type Rule struct {
	Alert       string            `yaml:"alert,omitempty"`
	Record      string            `yaml:"record,omitempty"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

type ConvertOptions struct {
	AllowRisky bool
}

type Warning struct {
	Group  string
	Rule   string
	Expr   string
	Reason string
}

type Report struct {
	Warnings []Warning
}

func Convert(input []byte) ([]byte, error) {
	out, _, err := ConvertWithOptions(input, ConvertOptions{AllowRisky: true})
	return out, err
}

func ConvertWithOptions(input []byte, opts ConvertOptions) ([]byte, Report, error) {
	var prom RuleFile
	if err := yaml.Unmarshal(input, &prom); err == nil && len(prom.Groups) > 0 {
		return marshalTranslated(prom, opts)
	}

	var legacy map[string][]Group
	if err := yaml.Unmarshal(input, &legacy); err != nil {
		return nil, Report{}, fmt.Errorf("parse rules YAML: %w", err)
	}
	if len(legacy) == 0 {
		return nil, Report{}, fmt.Errorf("parse rules YAML: no groups found")
	}

	keys := make([]string, 0, len(legacy))
	for k := range legacy {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var merged RuleFile
	for _, k := range keys {
		merged.Groups = append(merged.Groups, legacy[k]...)
	}
	return marshalTranslated(merged, opts)
}

func marshalTranslated(in RuleFile, opts ConvertOptions) ([]byte, Report, error) {
	out := RuleFile{Groups: make([]Group, 0, len(in.Groups))}
	var report Report
	for _, g := range in.Groups {
		translated, warnings, err := translateGroup(g, opts)
		report.Warnings = append(report.Warnings, warnings...)
		if err != nil {
			return nil, report, err
		}
		out.Groups = append(out.Groups, translated)
	}
	data, err := yaml.Marshal(out)
	if err != nil {
		return nil, report, fmt.Errorf("marshal translated rules: %w", err)
	}
	return data, report, nil
}

func translateGroup(g Group, opts ConvertOptions) (Group, []Warning, error) {
	out := g
	out.Type = "vlogs"
	out.Rules = make([]Rule, 0, len(g.Rules))
	var warnings []Warning
	for _, r := range g.Rules {
		ruleWarnings := analyzeRule(g.Name, r)
		if len(ruleWarnings) > 0 && !opts.AllowRisky {
			return Group{}, ruleWarnings, fmt.Errorf("rule %q in group %q requires manual review:\n%s", ruleName(r), g.Name, formatWarnings(ruleWarnings))
		}
		translated, err := translator.TranslateLogQL(r.Expr)
		if err != nil {
			name := r.Alert
			if name == "" {
				name = r.Record
			}
			return Group{}, nil, fmt.Errorf("translate group %q rule %q: %w", g.Name, name, err)
		}
		r.Expr = translated
		out.Rules = append(out.Rules, r)
		warnings = append(warnings, ruleWarnings...)
	}
	return out, warnings, nil
}

func analyzeRule(group string, r Rule) []Warning {
	var warnings []Warning
	name := ruleName(r)
	expr := strings.TrimSpace(r.Expr)

	if strings.Contains(expr, "without(") {
		warnings = append(warnings, Warning{
			Group:  group,
			Rule:   name,
			Expr:   expr,
			Reason: "uses without(), which is implemented by proxy-side post-processing rather than native VictoriaLogs/vmalert execution",
		})
	}
	for _, token := range []string{"on(", "ignoring(", "group_left(", "group_right("} {
		if strings.Contains(expr, token) {
			warnings = append(warnings, Warning{
				Group:  group,
				Rule:   name,
				Expr:   expr,
				Reason: "uses vector-matching semantics that are implemented by proxy-side evaluation, not by direct vmalert/VictoriaLogs execution",
			})
			break
		}
	}
	if hasSubquery(expr) {
		warnings = append(warnings, Warning{
			Group:  group,
			Rule:   name,
			Expr:   expr,
			Reason: "uses subquery [range:step] syntax, which the proxy evaluates above VictoriaLogs but vmalert cannot reproduce natively through translated LogsQL alone",
		})
	}
	if r.Record != "" && strings.Contains(strings.ToLower(expr), "histogram(") {
		warnings = append(warnings, Warning{
			Group:  group,
			Rule:   name,
			Expr:   expr,
			Reason: "uses histogram() in a recording rule; current vmalert/VictoriaLogs support for this path is incomplete",
		})
	}
	return warnings
}

func (r Report) String() string {
	if len(r.Warnings) == 0 {
		return "No migration warnings.\n"
	}
	return "Rules needing manual review:\n" + formatWarnings(r.Warnings)
}

func formatWarnings(warnings []Warning) string {
	var b strings.Builder
	for _, warning := range warnings {
		fmt.Fprintf(&b, "- group %q rule %q: %s\n", warning.Group, warning.Rule, warning.Reason)
	}
	return b.String()
}

func ruleName(r Rule) string {
	if r.Alert != "" {
		return r.Alert
	}
	if r.Record != "" {
		return r.Record
	}
	return "<unnamed>"
}

func hasSubquery(expr string) bool {
	depth := 0
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '[':
			end := strings.IndexByte(expr[i:], ']')
			if end <= 0 {
				continue
			}
			content := expr[i+1 : i+end]
			if strings.Contains(content, ":") {
				return true
			}
		}
	}
	return false
}
