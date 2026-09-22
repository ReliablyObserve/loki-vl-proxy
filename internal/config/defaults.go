package config

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var proxyConstRe = regexp.MustCompile(`(?m)^\s*(Default[A-Za-z0-9_]*)\s*=\s*([^/\n]+)`)

// proxyConstants reads the exported Default* constants of internal/proxy, the
// values the flags use as their defaults.
func proxyConstants(root string) (map[string]string, error) {
	out := map[string]string{}
	entries, err := os.ReadDir(filepath.Join(root, "internal", "proxy"))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, "internal", "proxy", entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, match := range proxyConstRe.FindAllStringSubmatch(string(data), -1) {
			out[match[1]] = strings.TrimSpace(match[2])
		}
	}
	return out, nil
}

// ResolveDefault turns a flag's default expression into the value an operator
// sees: `proxy.DefaultX` and constant arithmetic become numbers, and durations
// become their Go string form.
func ResolveDefault(expr string, consts map[string]string) string {
	expr = strings.TrimSpace(expr)
	if name, ok := strings.CutPrefix(expr, "proxy."); ok {
		if value, found := consts[name]; found {
			return ResolveDefault(value, consts)
		}
		return expr
	}
	if value, ok := evalDuration(expr); ok {
		return value
	}
	if value, ok := evalInt(expr); ok {
		return value
	}
	return expr
}

func evalInt(expr string) (string, bool) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", false
	}
	value, ok := evalConst(parsed)
	if !ok || value.Kind() != constant.Int {
		return "", false
	}
	return value.ExactString(), true
}

func evalConst(node ast.Expr) (constant.Value, bool) {
	switch n := node.(type) {
	case *ast.BasicLit:
		value := constant.MakeFromLiteral(n.Value, n.Kind, 0)
		return value, value.Kind() != constant.Unknown
	case *ast.ParenExpr:
		return evalConst(n.X)
	case *ast.BinaryExpr:
		left, okLeft := evalConst(n.X)
		right, okRight := evalConst(n.Y)
		if !okLeft || !okRight {
			return nil, false
		}
		if n.Op == token.SHL || n.Op == token.SHR {
			shift, ok := constant.Uint64Val(right)
			if !ok {
				return nil, false
			}
			return constant.Shift(left, n.Op, uint(shift)), true
		}
		return constant.BinaryOp(left, n.Op, right), true
	}
	return nil, false
}

var durationUnits = map[string]time.Duration{
	"time.Nanosecond":  time.Nanosecond,
	"time.Microsecond": time.Microsecond,
	"time.Millisecond": time.Millisecond,
	"time.Second":      time.Second,
	"time.Minute":      time.Minute,
	"time.Hour":        time.Hour,
}

func evalDuration(expr string) (string, bool) {
	for name, unit := range durationUnits {
		if expr == name {
			return unit.String(), true
		}
		factor, found := strings.CutSuffix(strings.TrimSpace(expr), "*"+name)
		if !found {
			continue
		}
		count, ok := evalInt(strings.TrimSpace(strings.TrimSuffix(factor, "*")))
		if !ok {
			continue
		}
		var n int64
		if _, err := fmt.Sscan(count, &n); err != nil {
			continue
		}
		return (time.Duration(n) * unit).String(), true
	}
	return "", false
}
