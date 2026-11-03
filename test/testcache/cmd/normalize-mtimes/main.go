// Command normalize-mtimes gives every tracked file of a checkout a modification time derived from its
// content, so Go's test cache can replay results across checkouts. See package testcache.
//
//	normalize-mtimes [repo-root]
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/testcache"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	start := time.Now()
	cmd := exec.Command("git", "-C", root, "ls-files", "-z")
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "normalize-mtimes: git ls-files:", err)
		os.Exit(2)
	}
	var files []string
	for _, name := range bytes.Split(out, []byte{0}) {
		if len(name) > 0 {
			files = append(files, string(name))
		}
	}
	nFiles, nDirs, err := testcache.NormalizeMtimes(root, files)
	if err != nil {
		fmt.Fprintln(os.Stderr, "normalize-mtimes:", err)
		os.Exit(2)
	}
	fmt.Printf("normalize-mtimes: %d files and %d directories in %s\n", nFiles, nDirs, time.Since(start).Round(time.Millisecond))
}
