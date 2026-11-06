// Package oraclerun builds and runs the parse oracles under test/oracle: Go modules of their own that import
// VictoriaLogs' and Loki's parsers. Only tests import this package, so neither oracle nor this runner is ever
// part of the proxy binary; the runner only builds and executes the oracles as separate programs.
package oraclerun

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Env is the environment the oracles build and run with. GOPRIVATE fetches VictoriaLogs straight from its
// repository: the module proxy's tags of github.com/VictoriaMetrics/VictoriaLogs predate the repository split
// and hold VictoriaMetrics' tree.
func Env() []string {
	return append(os.Environ(), "GOPRIVATE=github.com/VictoriaMetrics/VictoriaLogs")
}

// Build compiles the oracle module in dir, with modfile ("" for its go.mod), to a binary in work and returns
// its path.
func Build(dir, modfile, work, name string) (string, error) {
	bin := filepath.Join(work, name)
	args := []string{"build"}
	if modfile != "" {
		args = append(args, "-modfile="+modfile)
	}
	args = append(args, "-o", bin, ".")
	if out, err := goCmd(dir, args...); err != nil {
		return "", fmt.Errorf("build %s: %v\n%s", dir, err, out)
	}
	return bin, nil
}

// SourceDir is where the module cache holds the source of module, as the oracle module in dir pins it.
func SourceDir(dir, modfile, module string) (string, error) {
	args := []string{"list"}
	if modfile != "" {
		args = append(args, "-modfile="+modfile)
	}
	args = append(args, "-m", "-f", "{{if .Replace}}{{.Replace.Dir}}{{else}}{{.Dir}}{{end}}", module)
	out, err := goCmd(dir, args...)
	if err != nil {
		return "", fmt.Errorf("locate %s: %v\n%s", module, err, out)
	}
	return strings.TrimSpace(out), nil
}

// Run executes an oracle binary.
func Run(bin string, args ...string) error {
	cmd := exec.Command(bin, args...)
	cmd.Env = Env()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %v\n%s", filepath.Base(bin), strings.Join(args, " "), err, out)
	}
	return nil
}

// Go runs the go command in dir with the oracles' environment.
func Go(dir string, args ...string) error {
	if out, err := goCmd(dir, args...); err != nil {
		return fmt.Errorf("go %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

func goCmd(dir string, args ...string) (string, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = Env()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Gzip writes src gzipped to dst.
func Gzip(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }() // read only: a close error loses nothing
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	zw := gzip.NewWriter(out)
	if _, err := io.Copy(zw, in); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return out.Close()
}
