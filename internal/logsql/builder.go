// internal/logsql/builder.go
package logsql

import (
	"fmt"
	"net"
)

// NewQuery assembles a Query from a filter and zero or more pipe stages.
// filter may be nil (produces "*" in the output).
func NewQuery(filter FilterExpr, pipes ...Pipe) *Query {
	return &Query{Filter: filter, Pipes: pipes}
}

// Constructor helpers for common filter expressions.

func And(left, right FilterExpr) FilterExpr { return AndExpr{Left: left, Right: right} }
func Or(left, right FilterExpr) FilterExpr  { return OrExpr{Left: left, Right: right} }
func Not(expr FilterExpr) FilterExpr        { return NotExpr{Expr: expr} }

func FieldExact(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpExact, Value: value}
}
func FieldRegexp(field, pattern string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpRegexp, Value: pattern}
}
func FieldPrefix(field, prefix string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpPrefix, Value: prefix}
}
func FieldSubstring(field, sub string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpSubstring, Value: sub}
}
func FieldAny(field string) FilterExpr   { return FieldFilter{Field: field, Op: FieldOpAny} }
func FieldEmpty(field string) FilterExpr { return FieldFilter{Field: field, Op: FieldOpEmpty} }
func FieldGT(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpGT, Value: value}
}
func FieldGTE(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpGTE, Value: value}
}
func FieldLT(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpLT, Value: value}
}
func FieldLTE(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpLTE, Value: value}
}

// Builder selects the best LogsQL construct for the detected VL version.
type Builder struct{ caps Capabilities }

// NewBuilder returns a Builder configured for the given VL capabilities.
func NewBuilder(caps Capabilities) *Builder { return &Builder{caps: caps} }

// BestTopN returns pipe stages that keep the top-K entries by the named field.
// On all supported versions (v1.40+) this is | sort by (field desc) | limit K.
func (b *Builder) BestTopN(k int, field string) []Pipe {
	return []Pipe{
		PipeSort{By: []SortField{{Field: field, Desc: true}}},
		PipeLimit{N: k},
	}
}

// BestIPv4Range returns a filter that matches field against the CIDR range.
// On v1.45+ it emits the native ipv4_range() field filter.
// On earlier versions it falls back to a regexp approximation.
func (b *Builder) BestIPv4Range(field, cidr string) FilterExpr {
	if b.caps.FieldIPv4Range {
		first, last, ok := cidrToRange(cidr)
		if !ok {
			return FieldFilter{Field: field, Op: FieldOpRegexp, Value: cidrToRegexp(cidr)}
		}
		return FieldFilter{
			Field: field,
			Op:    FieldOpIPv4Range,
			Value: first + ", " + last,
		}
	}
	return FieldFilter{Field: field, Op: FieldOpRegexp, Value: cidrToRegexp(cidr)}
}

// cidrToRange parses a CIDR and returns the first and last IP as strings.
func cidrToRange(cidr string) (first, last string, ok bool) {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", false
	}
	firstIP := network.IP.Mask(network.Mask)
	lastIP := make(net.IP, len(firstIP))
	for i := range firstIP {
		lastIP[i] = firstIP[i] | ^network.Mask[i]
	}
	return firstIP.String(), lastIP.String(), true
}

// cidrToRegexp converts a CIDR to a conservative regexp approximation.
// Only handles /8, /16, /24 accurately; other prefix lengths may over-match.
func cidrToRegexp(cidr string) string {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return ".*"
	}
	ones, _ := network.Mask.Size()
	ip4 := network.IP.To4()
	if ip4 == nil {
		return ".*"
	}
	switch {
	case ones >= 24:
		return fmt.Sprintf(`^%d\.%d\.%d\.`, ip4[0], ip4[1], ip4[2])
	case ones >= 16:
		return fmt.Sprintf(`^%d\.%d\.`, ip4[0], ip4[1])
	case ones >= 8:
		return fmt.Sprintf(`^%d\.`, ip4[0])
	default:
		return ".*"
	}
}
