// Command regload loads the conformance registry's cases through the strict loader.
//
//	regload check [registry-root]   print every problem, exit 1 when there is one
//	regload dump  [registry-root]   print every case as JSON (id, path, doc), exit 1 on a problem
//
// The root defaults to conformance/registry. The Python conformance tools call dump instead of
// parsing YAML themselves.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "check" && os.Args[1] != "dump") {
		fmt.Fprintln(os.Stderr, "usage: regload check|dump [registry-root]")
		os.Exit(2)
	}
	root := "conformance/registry"
	if len(os.Args) > 2 {
		root = os.Args[2]
	}
	cases, problems, err := registry.LoadAll(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "regload:", err)
		os.Exit(2)
	}
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, p)
	}
	if os.Args[1] == "dump" && len(problems) == 0 {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", " ")
		if err := enc.Encode(cases); err != nil {
			fmt.Fprintln(os.Stderr, "regload:", err)
			os.Exit(2)
		}
	}
	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "regload: %d problem(s) in %d case file(s)\n", len(problems), len(cases))
		os.Exit(1)
	}
	if os.Args[1] == "check" {
		fmt.Printf("registry cases: %d files load strictly and match %s\n", len(cases), registry.SchemaFile)
	}
}
