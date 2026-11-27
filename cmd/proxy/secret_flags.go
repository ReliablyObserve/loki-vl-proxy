package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// maxSecretFileBytes bounds one -...-file secret read at startup.
const maxSecretFileBytes = 64 << 10

// secretFlag is one secret with the three places the binary reads it from: the
// value flag, the file flag and the environment variable.
type secretFlag struct {
	name  string  // value flag, without the dash
	value *string // the value flag; receives the resolved secret
	file  *string // the -<name>-file flag
	env   string  // environment variable
}

// parseFlagsAndSecrets parses the command line and fills the secrets. It
// returns the flags the operator passed, so "left at default" can be told from
// "set to the default value".
func parseFlagsAndSecrets(fs *flag.FlagSet, args []string, getenv func(string) string, secrets []secretFlag) (map[string]bool, error) {
	if err := fs.Parse(args); err != nil {
		return nil, redactFlagError(err)
	}
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	return explicit, resolveSecretFlags(explicit, getenv, secrets)
}

// resolveSecretFlags fills each secret from the first source that is set: the
// value flag, then the file flag, then the environment variable. A value flag
// and a file flag for one secret are refused, and so is a file that cannot be
// read or holds nothing, so a configured secret is never silently dropped.
// Errors name flags and paths, never a value.
func resolveSecretFlags(explicit map[string]bool, getenv func(string) string, secrets []secretFlag) error {
	for _, s := range secrets {
		fileFlag := s.name + "-file"
		switch {
		case explicit[s.name] && explicit[fileFlag]:
			return fmt.Errorf("-%s and -%s are both set; keep one", s.name, fileFlag)
		case explicit[s.name]:
		case explicit[fileFlag]:
			v, err := readSecretFile(*s.file)
			if err != nil {
				return fmt.Errorf("-%s: %w", fileFlag, err)
			}
			*s.value = v
		default:
			if v := getenv(s.env); v != "" {
				*s.value = v
			}
		}
	}
	return nil
}

// readSecretFile reads one secret, without the line ending editors and shell
// redirects leave at its end.
func readSecretFile(path string) (string, error) {
	f, err := os.Open(path) // #nosec G304 -- operator-supplied path of a mounted secret
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxSecretFileBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxSecretFileBytes {
		return "", fmt.Errorf("%s is larger than %d bytes", path, maxSecretFileBytes)
	}
	v := strings.TrimRight(string(data), "\r\n")
	if v == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return v, nil
}

// redactFlagError drops the value from the one flag parser error that quotes a
// whole argument ("bad flag syntax: ---name=value").
func redactFlagError(err error) error {
	msg := err.Error()
	if !strings.HasPrefix(msg, "bad flag syntax: ") {
		return err
	}
	if name, _, found := strings.Cut(msg, "="); found {
		return errors.New(name + "=<redacted>")
	}
	return err
}
