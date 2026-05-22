package main

import (
	"fmt"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func translate(q string) string {
	result, err := translator.TranslateLogQL(q)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	return result
}

func main() {
	queries := []string{
		// Binary with without in left sub-expression
		`sum without (host) (rate({app="api"}[5m])) / sum by (app) (rate({app="api",level="error"}[5m]))`,
		// Or should this parse as?
		`sum without (host) (count_over_time({app="api"}[5m])) / sum (count_over_time({app="api",level="error"}[5m]))`,
	}

	for _, q := range queries {
		fmt.Printf("QUERY: %q\n", q)
		expr, err := logql.Parse(q)
		if err != nil {
			fmt.Printf("  PARSE ERROR: %v\n", err)
			fmt.Printf("  FULL TRANSLATED: %q\n\n", translate(q))
			continue
		}
		fmt.Printf("  PARSED TYPE: %T\n", expr)

		if binOp, ok := expr.(*logql.BinOpExpr); ok {
			leftStr := binOp.Left.String()
			rightStr := binOp.Right.String()
			fmt.Printf("  LEFT: %q\n  LEFT TRANSLATED: %q\n", leftStr, translate(leftStr))
			fmt.Printf("  RIGHT: %q\n  RIGHT TRANSLATED: %q\n", rightStr, translate(rightStr))
		}

		fmt.Printf("  FULL TRANSLATED: %q\n\n", translate(q))
	}
}
