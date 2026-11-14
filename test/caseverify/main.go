// Command caseverify runs every executable registry case against Loki and the proxy and records a verdict
// per case in conformance/registry/generated/verification/cases.json.
//
//	go run ./test/caseverify -loki http://127.0.0.1:13101 -proxy http://127.0.0.1:13100 \
//	    [-vl http://127.0.0.1:19428] [-record answers.jsonl] [-report report.md] [-check]
//	go run ./test/caseverify -replay answers.jsonl      # the same verdicts from recorded answers, no network
//
// A case is executable when its request is an HTTP request (method and path). The runner resolves the
// relative times of its parameters once (now, now-1h; "now" is the start of the run less -settle, at least
// five minutes, aligned down to the case's step, so no window reads data younger than the lag), sends the
// same bytes to both sides, and compares the answers on the facets its compare block names (compare.fields,
// or the mode's default), the HTTP status always included.
//
// No answer is compared on data that is not proven equal: before the comparison the case's stream
// selectors are counted over its window on Loki (sum(count_over_time)) and on VictoriaLogs (-vl, stats
// count()), and every count is recorded, so -replay needs no stack. A difference is asked a second time;
// the verdict is:
//
//	matches         the answers agree on every facet, on equal and non-empty data
//	differs         they disagree twice on equal data; the measured values are recorded
//	data_differs    Loki and VictoriaLogs count other lines for the case's selectors and window
//	empty           both count no line
//	loki_not_ready  Loki's metric path answers nothing while VictoriaLogs holds the lines (a fresh Loki)
//	loki_cached_stale  Loki answered from reused results-cache extents and holds fewer lines or samples than the proxy, on data its own count proved equal
//	not_comparable  Loki answered with warnings, or the proxy answered stale
//	data_unchecked  no count to prove the data (no -vl, another tenant, a recording without counts)
//	unstable        they disagreed once and agreed on the second ask
//	both_fail       Loki and the proxy both answered 5xx to a case that does not compare errors
//	blocked         nothing is proven: a side did not answer, Loki failed, both answers are empty, or the
//	                request needs a precondition, a placeholder time, a write, a stream or Grafana
//	not_executable  a scenario case (scope and entry) with no HTTP request
//
// A fixed case that differs is a regression, unless its owner_kept names the owner-kept deviation cases its
// answers are subject to: then it is reported as differs_owner_kept with them (still a difference, never
// passed, not counted as a regression).
//
// A case whose request names proxy_flags runs against the proxy variant (-variants) that has them, or is
// blocked. Each verdict is set against the registry: a fixed case (or one without a gap) that differs is a
// regression, an open case that matches is a proposed close, a documented, owner-kept or upstream case that
// matches has changed behaviour. The runner never edits a case file; -check exits 1 on a regression.
//
// The command is test support: it reads the registry, talks to the URLs it is given and writes the files
// it is told to write.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/registry"
)

type config struct {
	registry, loki, proxy, vl, replay, record, out, report string
	commit, proxyFlags, lokiVersion, vlVersion, tenant     string
	variants, compose, proxyRevision, proxyImage           string
	list                                                   bool
	requireCurrent, noReuse                                bool
	cases                                                  string
	settle, timeout                                        time.Duration
	check                                                  bool
	now                                                    func() time.Time
}

func main() {
	cfg := config{now: time.Now}
	flag.StringVar(&cfg.registry, "registry", "conformance/registry", "the registry root")
	flag.StringVar(&cfg.loki, "loki", "", "Loki base URL (live run)")
	flag.StringVar(&cfg.proxy, "proxy", "", "proxy base URL (live run)")
	flag.StringVar(&cfg.vl, "vl", "", "VictoriaLogs base URL: its version, and the data precheck's counts")
	flag.StringVar(&cfg.proxyRevision, "proxy-revision", "", "the commit the proxy under test was built from (its startup log's revision)")
	flag.StringVar(&cfg.proxyImage, "proxy-image", "", "the proxy image's id or digest, as recorded")
	flag.BoolVar(&cfg.noReuse, "no-reuse", false, "do not repeat each case to measure the proxy's cache reuse")
	flag.BoolVar(&cfg.requireCurrent, "require-current-proxy", false, "refuse the run unless -proxy-revision is -commit")
	flag.StringVar(&cfg.variants, "variants", "", `JSON file of the proxy variants: [{"name","url","flags":[...]}]`)
	flag.StringVar(&cfg.compose, "compose", "", "compose file (test/e2e-compat/docker-compose.yml) whose proxy services are the variants, and whose service on -proxy's port gives the default flags; -variants adds to them")
	flag.BoolVar(&cfg.list, "list", false, "send nothing: print the cases that are blocked, by class, and the cases that run, and exit")
	flag.StringVar(&cfg.replay, "replay", "", "recorded answers file: verdicts without sending a request")
	flag.StringVar(&cfg.record, "record", "", "live run: write every answer to this file")
	flag.StringVar(&cfg.out, "out", "conformance/registry/generated/verification/cases.json", "the verdicts file")
	flag.StringVar(&cfg.report, "report", "", "also write the Markdown report here")
	flag.StringVar(&cfg.commit, "commit", os.Getenv("GITHUB_SHA"), "the commit the proxy was built from")
	flag.StringVar(&cfg.proxyFlags, "proxy-flags", "", "the flags the proxy runs with, as recorded")
	flag.StringVar(&cfg.lokiVersion, "loki-version", "", "Loki version; read from its buildinfo when empty")
	flag.StringVar(&cfg.vlVersion, "vl-version", "", "VictoriaLogs version; read from -vl when empty")
	flag.StringVar(&cfg.tenant, "tenant", "0", "X-Scope-OrgID for a case that sets none")
	flag.StringVar(&cfg.cases, "case", "", "comma-separated case ids (default: every case)")
	flag.DurationVar(&cfg.settle, "settle", 5*time.Minute, "the window ends this long before the run starts (at least 5m)")
	flag.DurationVar(&cfg.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.BoolVar(&cfg.check, "check", false, "exit 1 when a fixed case differs from Loki")
	flag.Parse()
	os.Exit(run(cfg, os.Stdout, os.Stderr))
}

func run(cfg config, stdout, stderr io.Writer) int {
	fail := func(err error) int {
		fmt.Fprintln(stderr, "caseverify:", err)
		return 2
	}
	cases, problems, err := registry.LoadAll(cfg.registry)
	if err != nil {
		return fail(err)
	}
	if len(problems) > 0 {
		return fail(fmt.Errorf("the registry does not load strictly (%d problems; go run ./test/registry/cmd/regload check)", len(problems)))
	}
	// A redirect is an answer to compare (a path Loki and the proxy redirect), not a hop to follow.
	client := &http.Client{Timeout: cfg.timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	start := cfg.now().UTC()
	f := File{GeneratedBy: "go run ./test/caseverify", Date: start.Format(time.RFC3339), Commit: cfg.commit,
		ProxyFlags: cfg.proxyFlags, Loki: cfg.lokiVersion, VictoriaLogs: cfg.vlVersion, Cases: map[string]Result{}}
	if cfg.settle < minSettle {
		return fail(fmt.Errorf("-settle %s is under %s: the window would read data still arriving", cfg.settle, minSettle))
	}
	variants, err := loadVariants(cfg.variants)
	if err != nil {
		return fail(err)
	}
	variants, err = withCompose(&cfg, variants, &f)
	if err != nil {
		return fail(err)
	}
	if cfg.list {
		return list(cases, strings.Fields(cfg.proxyFlags), variants, stdout)
	}
	src, cnt, meter, rec, err := sources(cfg, client, variants, &f)
	if err != nil {
		return fail(err)
	}
	for _, v := range []*string{&f.Loki, &f.VictoriaLogs} {
		if *v == "" {
			*v = "unknown"
		}
	}
	now := start.Add(-cfg.settle).Truncate(time.Second)
	f.ProxyBuild = proxyBuild(cfg.proxyRevision, cfg.proxyImage, cfg.commit)
	if rp, ok := src.(*Replay); ok {
		// The recorded run's settled end and proxy build: every window resolves to the bytes it recorded.
		if end := rp.WindowEnd(); end != 0 {
			now = time.Unix(0, end).UTC()
		}
		if rp.header != nil {
			f.ProxyBuild = rp.header.Proxy
		}
	}
	if cfg.requireCurrent && !f.ProxyBuild.Verified {
		return fail(fmt.Errorf("the proxy build %q is not the commit %q the run measures (-require-current-proxy)", f.ProxyBuild.Revision, cfg.commit))
	}
	if rec != nil {
		rec.header = &RunHeader{WindowEnd: now.UnixNano(), Proxy: f.ProxyBuild}
	}
	f.WindowEnd = now.Format(time.RFC3339)
	only := map[string]bool{}
	for _, id := range strings.Split(cfg.cases, ",") {
		if id = strings.TrimSpace(id); id != "" {
			only[id] = true
		}
	}
	for _, p := range Probes(cases) {
		if len(only) > 0 && !only[p.ID] {
			continue
		}
		ChooseVariant(&p, strings.Fields(cfg.proxyFlags), variants)
		f.Cases[p.ID] = Verify(p, src, cnt, meter, now, cfg.tenant)
	}
	Summarize(&f)
	if f.ProxyBuild.Stale {
		// Another build than the commit measured: what differs is not this commit's regression.
		f.Flags[flagStaleRegression], f.Flags[flagRegression] = f.Flags[flagRegression], []string{}
	}
	if err := writeJSON(cfg.out, f); err != nil {
		return fail(err)
	}
	if rec != nil {
		if err := writeWith(cfg.record, rec.Write); err != nil {
			return fail(err)
		}
	}
	report := Report(f)
	if cfg.report != "" {
		if err := writeWith(cfg.report, func(w io.Writer) error { _, err := io.WriteString(w, report); return err }); err != nil {
			return fail(err)
		}
	}
	return summarize(cfg, f, stdout, stderr)
}

// summarize prints the run's counts and, with -check, fails on a regression. A difference an owner-kept deviation
// explains is not a regression and does not fail -check, but it is said every run.
func summarize(cfg config, f File, stdout, stderr io.Writer) int {
	parts := make([]string, 0, len(verdicts))
	for _, v := range verdicts {
		parts = append(parts, fmt.Sprintf("%d %s", f.Counts[v], v))
	}
	fmt.Fprintf(stdout, "caseverify: %d cases (%s): %s; %d regressions, %d differ with an owner-kept deviation, %d proposed closes, %d changed behaviours\n",
		len(f.Cases), f.Mode, strings.Join(parts, ", "),
		len(f.Flags[flagRegression]), len(f.Flags[flagOwnerKept]), len(f.Flags[flagProposedClose]), len(f.Flags[flagBehaviourChanged]))
	// A difference an owner-kept deviation explains is not a regression and does not fail -check, but it is
	// said every run.
	for _, id := range f.Flags[flagOwnerKept] {
		fmt.Fprintf(stderr, "owner-kept: %s differs from Loki, subject to %s\n", id, strings.Join(f.Cases[id].OwnerKept, ", "))
	}
	if cfg.check && len(f.Flags[flagRegression]) > 0 {
		for _, id := range f.Flags[flagRegression] {
			fmt.Fprintf(stderr, "regression: %s differs from Loki\n", id)
		}
		return 1
	}
	return 0
}

// withCompose adds the proxy services of the compose file to the variants and takes the default proxy's flags
// from the service on -proxy's port.
func withCompose(cfg *config, variants []Variant, f *File) ([]Variant, error) {
	if cfg.compose == "" {
		return variants, nil
	}
	composed, defaultFlags, err := VariantsFromCompose(cfg.compose, "127.0.0.1", cfg.proxy)
	if err != nil {
		return nil, fmt.Errorf("-compose: %w", err)
	}
	if cfg.proxyFlags == "" {
		cfg.proxyFlags = strings.Join(defaultFlags, " ")
	}
	f.ProxyFlags = cfg.proxyFlags
	return append(composed, variants...), nil
}

// loadVariants reads the -variants file: every proxy variant with its URL and flags.
func loadVariants(path string) ([]Variant, error) {
	if path == "" {
		return nil, nil
	}
	var variants []Variant
	raw, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(raw, &variants)
	}
	if err != nil {
		return nil, fmt.Errorf("-variants: %w", err)
	}
	for _, v := range variants {
		if v.Name == "" || baseURL(v.URL) != nil {
			return nil, fmt.Errorf("-variants: %q needs a name and an http(s) URL", v.Name)
		}
	}
	return variants, nil
}

// sources are the answers and counts of a run: recorded ones, or the live stack's (recorded when -record).
func sources(cfg config, client *http.Client, variants []Variant, f *File) (Source, Counter, Meter, *Recorder, error) {
	if cfg.replay != "" {
		rp, err := openReplay(cfg.replay)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		f.Mode = "recorded"
		return rp, rp, rp, nil, nil
	}
	if cfg.loki == "" || cfg.proxy == "" {
		return nil, nil, nil, nil, errUsage
	}
	for _, u := range []string{cfg.loki, cfg.proxy} {
		if err := baseURL(u); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	var rec *Recorder
	if cfg.record != "" {
		rec = &Recorder{}
	}
	bases := map[string]string{"loki": cfg.loki, "proxy": cfg.proxy}
	for _, v := range variants {
		bases["proxy@"+v.Name] = v.URL
	}
	f.Mode = "live"
	if f.Loki == "" {
		f.Loki = readVersion(client, strings.TrimRight(cfg.loki, "/")+"/loki/api/v1/status/buildinfo", lokiVersion)
	}
	if f.VictoriaLogs == "" && cfg.vl != "" {
		f.VictoriaLogs = readVersion(client, strings.TrimRight(cfg.vl, "/")+"/metrics", vlVersion)
	}
	var meter Meter
	if !cfg.noReuse {
		meter = &LiveMeter{Bases: bases, Client: client, rec: rec}
	}
	return &Live{Bases: bases, Client: client, rec: rec},
		&LiveCounter{Loki: cfg.loki, VL: cfg.vl, Tenant: cfg.tenant, Client: client, rec: rec}, meter, rec, nil
}

// proxyBuild judges the proxy's build against the commit the run measures: verified when they are the same,
// stale when both are known and differ (the stack runs another build), unverified when either is unknown.
func proxyBuild(revision, image, commit string) ProxyBuild {
	b := ProxyBuild{Revision: revision, Image: image}
	known := func(s string) bool { return len(s) >= 7 && s != "unknown" }
	if known(revision) && known(commit) {
		// A short revision names its commit by prefix.
		same := strings.HasPrefix(commit, revision) || strings.HasPrefix(revision, commit)
		b.Verified, b.Stale = same, !same
	}
	return b
}

// minSettle is the shortest settle lag: data younger than it may still be arriving on one side.
const minSettle = 5 * time.Minute

func writeJSON(path string, v any) error {
	return writeWith(path, func(w io.Writer) error {
		enc := json.NewEncoder(w)
		enc.SetIndent("", " ")
		enc.SetEscapeHTML(false)
		return enc.Encode(v)
	})
}

func writeWith(path string, write func(io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
