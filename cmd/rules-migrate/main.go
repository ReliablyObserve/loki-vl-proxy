package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/szibis/Loki-VL-proxy/internal/rulesmigrate"
)

func main() {
	inPath := flag.String("in", "", "Input Loki rule file path (default: stdin)")
	outPath := flag.String("out", "", "Output vmalert rule file path (default: stdout)")
	reportPath := flag.String("report", "", "Optional migration warning report path")
	allowRisky := flag.Bool("allow-risky", false, "Allow risky rules that depend on proxy-only runtime semantics")
	flag.Parse()

	input, err := readInput(*inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read input: %v\n", err)
		os.Exit(1)
	}

	out, report, err := rulesmigrate.ConvertWithOptions(input, rulesmigrate.ConvertOptions{
		AllowRisky: *allowRisky,
	})
	if reportText := report.String(); reportText != "" {
		if err := writeReport(*reportPath, []byte(reportText)); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
		if len(report.Warnings) > 0 {
			fmt.Fprintln(os.Stderr, strings.TrimRight(reportText, "\n"))
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "convert rules: %v\n", err)
		os.Exit(1)
	}

	if err := writeOutput(*outPath, out); err != nil {
		fmt.Fprintf(os.Stderr, "write output: %v\n", err)
		os.Exit(1)
	}
}

func readInput(path string) ([]byte, error) {
	if path == "" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func writeOutput(path string, data []byte) error {
	if path == "" {
		_, err := os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func writeReport(path string, data []byte) error {
	if path == "" {
		return nil
	}
	return os.WriteFile(path, data, 0o644)
}
