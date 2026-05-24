//go:build vl_compat

// Package logsql_test validates logsql query strings against a live VictoriaLogs
// instance. Run with: go test ./internal/logsql/... -tags=vl_compat -run TestVLCompat
package logsql_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// vlAddr returns the VictoriaLogs base URL from env or default.
func vlAddr() string {
	if addr := os.Getenv("VICTORIA_LOGS_ADDR"); addr != "" {
		return addr
	}
	return "http://localhost:9428"
}

// vlCheck sends query q to VL and returns an error if VL rejects it (HTTP 400).
// Returns (false, nil) if VL is not reachable so callers can skip.
func vlCheck(client *http.Client, addr, q string) (reachable bool, err error) {
	endpoint := fmt.Sprintf("%s/select/logsql/query?query=%s&start=now-1h&limit=0",
		addr, url.QueryEscape(q))
	resp, httpErr := client.Get(endpoint)
	if httpErr != nil {
		return false, nil // not reachable
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest {
		var result struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&result)
		return true, fmt.Errorf("VL rejected query: %s", result.Error)
	}
	return true, nil
}

// TestVLCompatFilterNodes validates all filter node String() outputs.
func TestVLCompatFilterNodes(t *testing.T) {
	addr := vlAddr()
	client := &http.Client{Timeout: 5 * time.Second}

	queries := []string{
		// Message filters
		`error`,
		`"hello world"`,
		`err*`,
		`*error*`,
		`="404"`,
		`~"error|warn"`,
		`seq("a","b")`,
		`i("error")`,
		`*`,
		// Field filters — all operators
		`app:="nginx"`,
		`level:~"err.*"`,
		`url:/api*`,
		`url:*login*`,
		`level:""`,
		`level:*`,
		`status:>400`,
		`status:>=500`,
		`latency:<100`,
		`latency:<=200`,
		`latency:range(100,500)`,
		`level:in(error,warn)`,
		`NOT level:="debug"`,
		// Stream and time
		`{app="nginx", env="prod"}`,
		`_time:5m`,
		// Field filter — range operators
		`ip:ipv4_range(192.168.0.1,192.168.0.255)`,
		`ip:ipv6_range(::1,::ffff)`,
		// Logical
		`error AND app:="nginx"`,
		`error OR warn`,
		`NOT debug`,
		`(error OR warn) AND app:="nginx"`,
	}

	checked := false
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			ok, err := vlCheck(client, addr, q)
			if !ok {
				t.Skipf("VictoriaLogs not reachable at %s", addr)
			}
			checked = true
			if err != nil {
				t.Error(err)
			}
		})
	}
	if !checked {
		t.Skip("all subtests skipped — VictoriaLogs not reachable")
	}
}

// TestVLCompatPipeStages validates pipe stage String() outputs.
func TestVLCompatPipeStages(t *testing.T) {
	addr := vlAddr()
	client := &http.Client{Timeout: 5 * time.Second}

	pipes := []logsql.Pipe{
		logsql.PipeUnpackJSON{},
		logsql.PipeUnpackLogfmt{},
		logsql.PipeFilter{Expr: logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}},
		logsql.PipeFields{Labels: []string{"level", "status"}},
		logsql.PipeDelete{Labels: []string{"debug"}},
		logsql.PipeLimit{N: 100},
		logsql.PipeSort{By: []logsql.SortField{{Field: "ts", Desc: true}}},
		logsql.PipeSort{By: []logsql.SortField{{Field: "count", Desc: true}}, Limit: 10},
		logsql.PipeMath{Alias: "pct", Expr: "rate/total*100"},
		logsql.PipeStats{
			By:    []logsql.GroupKey{{Field: "host"}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "cnt"}},
		},
		logsql.PipeStats{
			Funcs: []logsql.StatsFuncAlias{
				{Func: logsql.Sum{Field: "bytes"}, Alias: "total"},
				{Func: logsql.Max{Field: "latency"}, Alias: "p_max"},
			},
		},
		// New pipe stages added for VL upstream parity
		logsql.PipeTop{N: 10, By: []string{"host"}},
		logsql.PipeFirst{N: 5},
		logsql.PipeLast{N: 5, By: []string{"app"}},
		logsql.PipeSample{N: 100},
		logsql.PipeOffset{N: 50},
		logsql.PipeUniq{By: []string{"app", "level"}},
		logsql.PipeUniq{},
		logsql.PipeFieldNames{},
		logsql.PipeDropEmptyFields{},
		logsql.PipeCopy{Pairs: [][2]string{{"host", "node"}}},
	}

	checked := false
	for _, pipe := range pipes {
		q := "* " + pipe.String()
		t.Run(pipe.String(), func(t *testing.T) {
			ok, err := vlCheck(client, addr, q)
			if !ok {
				t.Skipf("VictoriaLogs not reachable at %s", addr)
			}
			checked = true
			if err != nil {
				t.Error(err)
			}
		})
	}
	if !checked {
		t.Skip("all subtests skipped — VictoriaLogs not reachable")
	}
}

// TestVLCompatStatsFunctions validates all 26 stats function String() outputs.
func TestVLCompatStatsFunctions(t *testing.T) {
	addr := vlAddr()
	client := &http.Client{Timeout: 5 * time.Second}

	funcs := []logsql.StatsFunc{
		logsql.Count{},
		logsql.Sum{Field: "bytes"},
		logsql.Min{Field: "latency"},
		logsql.Max{Field: "latency"},
		logsql.Avg{Field: "latency"},
		logsql.Median{Field: "latency"},
		logsql.Quantile{Phi: 0.99, Field: "latency"},
		logsql.Stddev{Field: "latency"},
		logsql.Stdvar{Field: "latency"},
		logsql.Rate{},
		logsql.RateSum{Field: "bytes"},
		logsql.CountUniq{Field: "user_id"},
		logsql.CountUniqHash{Field: "user_id"},
		logsql.UniqValues{Field: "user_id", Limit: 10},
		logsql.FieldMax{Field: "latency"},
		logsql.FieldMin{Field: "latency"},
		logsql.JSONValues{Field: "data"},
		logsql.Any{Field: "user_id"},
		logsql.CountEmpty{Field: "level"},
		logsql.SumLen{Field: "_msg"},
		logsql.Values{Field: "status", Limit: 10},
		logsql.Histogram{Field: "latency"},
		logsql.Last{Field: "_msg"},
		logsql.First{Field: "_msg"},
		logsql.RowAny{Fields: []string{"_msg", "level"}},
		logsql.RowMax{By: "latency", Fields: []string{"_msg", "status"}},
		// New stats functions for VL upstream parity
		logsql.RowMin{By: "latency", Fields: []string{"_msg", "status"}},
		logsql.JSONValuesSorted{Field: "level"},
		logsql.JSONValuesTopK{Field: "status", Limit: 5},
	}

	checked := false
	for _, fn := range funcs {
		fn := fn
		q := logsql.PipeStats{
			Funcs: []logsql.StatsFuncAlias{{Func: fn, Alias: "result"}},
		}
		query := "* " + q.String()
		t.Run(fn.String(), func(t *testing.T) {
			ok, err := vlCheck(client, addr, query)
			if !ok {
				t.Skipf("VictoriaLogs not reachable at %s", addr)
			}
			checked = true
			if err != nil {
				t.Error(err)
			}
		})
	}
	if !checked {
		t.Skip("all subtests skipped — VictoriaLogs not reachable")
	}
}

// TestVLCompatBuilderOutputs validates Builder helper String() outputs.
func TestVLCompatBuilderOutputs(t *testing.T) {
	addr := vlAddr()
	client := &http.Client{Timeout: 5 * time.Second}

	caps45 := logsql.CapabilitiesFor("v1.45.0")
	b := logsql.NewBuilder(caps45)

	queries := []string{
		// BestTopN
		logsql.NewQuery(nil, b.BestTopN(10, "count")...).String(),
		// BestIPv4Range on v1.45+
		"* | filter " + b.BestIPv4Range("client_ip", "192.168.1.0/24").String(),
		// Constructor helpers
		logsql.NewQuery(
			logsql.And(logsql.FieldExact("app", "nginx"), logsql.Word{Value: "error"}),
			logsql.PipeStats{
				By:    []logsql.GroupKey{{Field: "host"}},
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "cnt"}},
			},
		).String(),
	}

	checked := false
	for _, q := range queries {
		q := q
		t.Run(q, func(t *testing.T) {
			ok, err := vlCheck(client, addr, q)
			if !ok {
				t.Skipf("VictoriaLogs not reachable at %s", addr)
			}
			checked = true
			if err != nil {
				t.Error(err)
			}
		})
	}
	if !checked {
		t.Skip("all subtests skipped — VictoriaLogs not reachable")
	}
}
