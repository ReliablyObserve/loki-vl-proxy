// Command configdoc regenerates the configuration and limits reference
// documents from the flags declared in cmd/proxy/main.go and the registry in
// internal/config.
//
// Run it from the repository root:
//
//	go run ./cmd/configdoc
//
// TestGeneratedDocsAreUpToDate fails when the committed documents differ.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/config"
)

func main() {
	root := flag.String("root", ".", "repository root")
	check := flag.Bool("check", false, "exit non-zero when a document is out of date instead of writing it")
	flag.Parse()

	documents, err := config.Generate(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configdoc:", err)
		os.Exit(1)
	}
	drift := false
	for path, content := range documents {
		full := filepath.Join(*root, path)
		existing, readErr := os.ReadFile(full)
		if readErr == nil && string(existing) == content {
			continue
		}
		if *check {
			fmt.Fprintf(os.Stderr, "configdoc: %s is out of date; run `go run ./cmd/configdoc`\n", path)
			drift = true
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "configdoc:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "configdoc:", err)
			os.Exit(1)
		}
		fmt.Println("wrote", path)
	}
	if drift {
		os.Exit(1)
	}
}
