// Package testcache supports running the unit tests of this module through Go's test cache in CI.
//
// Go keys a cached test result on the size, mode and modification time of every file the test
// opened inside the module (and on each entry of every directory it opened). A fresh checkout gives
// every file a new modification time, so a package whose tests read repository files (internal/proxy
// reads the conformance registry, the compatibility matrix and the scripts) would never replay.
// NormalizeMtimes replaces the modification time of each tracked file with one derived from its
// content, and of each directory with a constant: the same content gives the same time on every
// checkout, and any change of content, even one that keeps the size, gives another.
package testcache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// base is the earliest time a normalised file gets; the content adds at most 2^28 seconds (8.5 years)
// and under a second, so every time lies in the past and outside the 2 second window in which Go
// refuses to cache a result that read a file it considers too new.
var base = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// ContentTime returns the modification time that identifies content.
func ContentTime(content []byte) time.Time {
	sum := sha256.Sum256(content)
	seconds := int64(binary.BigEndian.Uint32(sum[0:4]) & (1<<28 - 1))
	nanos := int64(binary.BigEndian.Uint32(sum[4:8]) % 1_000_000_000)
	return base.Add(time.Duration(seconds)*time.Second + time.Duration(nanos))
}

// DirTime is the modification time every directory gets.
func DirTime() time.Time { return base }

// NormalizeMtimes sets the modification time of each file in files (paths relative to root) from its
// content and of root and every directory above those files to DirTime. Entries that are symbolic
// links or not regular files are left alone: Go sees a link's target, which is normalised on its own
// when it is tracked. It returns the number of files and directories changed.
func NormalizeMtimes(root string, files []string) (nFiles, nDirs int, err error) {
	root = filepath.Clean(root)
	dirs := map[string]struct{}{root: {}}
	for _, rel := range files {
		path := filepath.Join(root, rel)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue // in the index, deleted from the work tree
		}
		if err != nil {
			return nFiles, nDirs, err
		}
		for dir := filepath.Dir(path); dir != root && dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
			dirs[dir] = struct{}{}
		}
		if !info.Mode().IsRegular() {
			continue
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nFiles, nDirs, err
		}
		mtime := ContentTime(content)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			return nFiles, nDirs, fmt.Errorf("normalize %s: %w", rel, err)
		}
		nFiles++
	}
	for dir := range dirs {
		if err := os.Chtimes(dir, DirTime(), DirTime()); err != nil {
			return nFiles, nDirs, fmt.Errorf("normalize %s: %w", dir, err)
		}
		nDirs++
	}
	return nFiles, nDirs, nil
}
