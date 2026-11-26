package clientauth

import (
	"context"
	"crypto/sha256"
	"time"
)

// ReloadResult is one reload check that changed something or failed.
type ReloadResult struct {
	Users *Users // the set now in use: the new one, or the last good one on failure
	Err   error  // nil when the new file was loaded
}

// Watcher re-reads the users file and swaps the set when its content changes.
// A file that cannot be read or parsed keeps the last good set; the failure is
// reported once per distinct content or error, so a broken file does not log
// on every tick.
type Watcher struct {
	a           *Authenticator
	path        string
	validTenant func(string) error
	report      func(ReloadResult)

	lastGood   [sha256.Size]byte
	lastFailed [sha256.Size]byte
	failedErr  string
}

// NewWatcher watches path for a, starting from the content a's current users
// were parsed from. report is called after every swap and every new failure.
func NewWatcher(a *Authenticator, path string, validTenant func(string) error, report func(ReloadResult)) *Watcher {
	return &Watcher{a: a, path: path, validTenant: validTenant, report: report, lastGood: a.Users().digest}
}

// Check reads the file once and swaps the set if its content changed and
// parses.
func (w *Watcher) Check() {
	data, err := ReadFile(w.path)
	if err != nil {
		w.failed([sha256.Size]byte{}, err)
		return
	}
	sum := sha256.Sum256(data)
	if sum == w.lastGood {
		w.failedErr = ""
		return
	}
	users, err := Parse(data, w.validTenant)
	if err != nil {
		w.failed(sum, err)
		return
	}
	w.a.Swap(users)
	w.lastGood, w.failedErr = sum, ""
	if w.report != nil {
		w.report(ReloadResult{Users: users})
	}
}

func (w *Watcher) failed(sum [sha256.Size]byte, err error) {
	if w.failedErr == err.Error() && w.lastFailed == sum {
		return
	}
	w.lastFailed, w.failedErr = sum, err.Error()
	if w.report != nil {
		w.report(ReloadResult{Users: w.a.Users(), Err: err})
	}
}

// Run checks the file every interval until ctx is done.
func (w *Watcher) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.Check()
		}
	}
}
