//go:build !darwin && !(linux && (amd64 || arm64))

package cache

import (
	"errors"
	"os"
)

func writeUncached(*os.File) error     { return errors.New("unsupported platform") }
func dropFromPageCache(*os.File) error { return nil }
