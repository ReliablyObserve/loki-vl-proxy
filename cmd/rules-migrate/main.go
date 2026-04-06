package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/szibis/Loki-VL-proxy/internal/rulesmigrate"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("rules-migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	inPath := fs.String("in", "", "Input Loki rule file path (default: stdin)")
	outPath := fs.String("out", "", "Output vmalert rule file path (default: stdout)")
	reportPath := fs.String("report", "", "Optional migration warning report path")
	allowRisky := fs.Bool("allow-risky", false, "Allow risky rules that depend on proxy-only runtime semantics")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	input, err := readInput(*inPath, stdin)
	if err != nil {
		fmt.Fprintf(stderr, "read input: %v\n", err)
		return 1
	}

	out, report, err := rulesmigrate.ConvertWithOptions(input, rulesmigrate.ConvertOptions{
		AllowRisky: *allowRisky,
	})
	if reportText := report.String(); reportText != "" {
		if err := writeReport(*reportPath, []byte(reportText)); err != nil {
			fmt.Fprintf(stderr, "write report: %v\n", err)
			return 1
		}
		if len(report.Warnings) > 0 {
			fmt.Fprintln(stderr, strings.TrimRight(reportText, "\n"))
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "convert rules: %v\n", err)
		return 1
	}

	if err := writeOutput(*outPath, out, stdout); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	return 0
}

func readInput(path string, stdin io.Reader) ([]byte, error) {
	if path == "" {
		return io.ReadAll(stdin)
	}
	return os.ReadFile(path)
}

func writeOutput(path string, data []byte, stdout io.Writer) error {
	if path == "" {
		_, err := stdout.Write(data)
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

func runWithInput(args []string, input string) (int, string, string) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(args, strings.NewReader(input), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}
