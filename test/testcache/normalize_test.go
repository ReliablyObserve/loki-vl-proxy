package testcache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, root, rel, content string) string {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func mtime(t *testing.T, path string) time.Time {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.ModTime()
}

// A second checkout of the same content gets the same times, whatever times the files had.
func TestNormalizeMtimesIsStableAcrossCheckouts(t *testing.T) {
	files := []string{"a.txt", "dir/b.txt", "dir/sub/c.txt"}
	var times [2][]time.Time
	for run := 0; run < 2; run++ {
		root := t.TempDir()
		for _, f := range files {
			p := write(t, root, f, "content of "+f)
			stamp := time.Now().Add(time.Duration(run) * -3 * time.Hour)
			if err := os.Chtimes(p, stamp, stamp); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := NormalizeMtimes(root, files); err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			times[run] = append(times[run], mtime(t, filepath.Join(root, f)))
		}
		for _, d := range []string{"", "dir", "dir/sub"} {
			if got := mtime(t, filepath.Join(root, d)); !got.Equal(DirTime()) {
				t.Errorf("directory %q has mtime %v, want %v", d, got, DirTime())
			}
		}
	}
	for i := range files {
		if !times[0][i].Equal(times[1][i]) {
			t.Errorf("%s: %v then %v for the same content", files[i], times[0][i], times[1][i])
		}
	}
}

// An edit that keeps the size changes the time, which is what makes Go rerun the tests that read the file.
func TestNormalizeMtimesChangesWithContentOfTheSameSize(t *testing.T) {
	root := t.TempDir()
	p := write(t, root, "matrix.json", `{"version": "3.7.9"}`)
	if _, _, err := NormalizeMtimes(root, []string{"matrix.json"}); err != nil {
		t.Fatal(err)
	}
	before := mtime(t, p)
	write(t, root, "matrix.json", `{"version": "3.7.8"}`)
	if _, _, err := NormalizeMtimes(root, []string{"matrix.json"}); err != nil {
		t.Fatal(err)
	}
	if after := mtime(t, p); after.Equal(before) {
		t.Fatalf("same-size edit kept mtime %v", before)
	}
}

// Go refuses to cache a result that read a file younger than 2 seconds, and a time in the future is younger.
func TestContentTimesAreOldAndWithinRange(t *testing.T) {
	for _, content := range []string{"", "a", "b", "the quick brown fox", string(make([]byte, 4096))} {
		got := ContentTime([]byte(content))
		if age := time.Since(got); age < 365*24*time.Hour {
			t.Errorf("ContentTime(%q) = %v is only %v old", content, got, age)
		}
		if got.Before(DirTime()) {
			t.Errorf("ContentTime(%q) = %v is before the base %v", content, got, DirTime())
		}
	}
}

func TestNormalizeMtimesSkipsMissingFilesAndLinks(t *testing.T) {
	root := t.TempDir()
	write(t, root, "real.txt", "x")
	if err := os.Symlink("real.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Skip("symlinks unavailable:", err)
	}
	nFiles, _, err := NormalizeMtimes(root, []string{"real.txt", "link.txt", "deleted.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if nFiles != 1 {
		t.Fatalf("normalised %d files, want 1 (the link and the deleted file are skipped)", nFiles)
	}
}
