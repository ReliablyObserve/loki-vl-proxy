package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Loki's delete API (/loki/api/v1/delete, registered with -delete-api-enabled behind its token) executed as
// VictoriaLogs delete tasks. Loki's answers are recorded from a Loki that serves the API (rows r1700 and later of
// internal/lokierr/testdata/loki_error_answers.jsonl):
//
//   - POST and PUT create a request: 204, after Loki's checks in Loki's order (deleteRequestOf). The proxy starts
//     a VictoriaLogs delete task (POST /delete/run_task) with the tenant's headers and a filter of the time range
//     and the rows the same log query reads (deleteFilter). Loki processes a request after its cancellation period;
//     VictoriaLogs runs the task at once.
//   - GET lists the tenant's requests. VictoriaLogs keeps neither a request's LogQL query nor its range (only its
//     own filter) and forgets a task once it ends, so the proxy cannot list them as Loki does: Loki's 500, the
//     construct in the log line.
//   - DELETE cancels a request: 204 without a request_id (Loki's answer), else the tenant's running task of that id
//     is stopped (POST /delete/stop_task). An id the tenant has no task for answers Loki's 404, for an id of
//     another tenant's task too. What a stopped task already deleted stays deleted, as Loki's force=true leaves it.
//
// Any other method is Loki's bodyless 405. VictoriaLogs' answers never reach the client: a refused or failed call is
// Loki's 500, VictoriaLogs' text in the log line only.

// The VictoriaLogs delete task API (VictoriaLogs v1.49 and later, -delete.enable).
const (
	vlDeleteRunTaskPath     = "/delete/run_task"
	vlDeleteStopTaskPath    = "/delete/stop_task"
	vlDeleteActiveTasksPath = "/delete/active_tasks"
	// vlDeleteTasksBodyLimit bounds the active task list the proxy reads.
	vlDeleteTasksBodyLimit = 4 << 20
)

// errDeleteVictoriaLogs marks a VictoriaLogs delete call that did not succeed; the log line names the flag
// VictoriaLogs needs for it.
var errDeleteVictoriaLogs = errors.New("VictoriaLogs delete task API call failed (VictoriaLogs serves it with -delete.enable)")

func (p *Proxy) handleDelete(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if err := decodeRequestForm(w, r); err != nil {
		p.metrics.RecordRequest("delete", p.fail(w, r, err), time.Since(start))
		return
	}
	var err error
	status := http.StatusNoContent
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		err = p.createDelete(r)
	case http.MethodGet:
		err = notServed("delete-list", errors.New("VictoriaLogs keeps no record of a delete request's query and range"))
	case http.MethodDelete:
		err = p.cancelDelete(r)
	default:
		err = lokierr.MethodNotAllowed()
	}
	if err != nil {
		status = p.fail(w, r, err)
	} else {
		// Loki's 204 carries no Content-Type; the shipped middleware presets one.
		w.Header().Del("Content-Type")
		w.WriteHeader(http.StatusNoContent)
	}
	p.metrics.RecordRequest("delete", status, time.Since(start))
}

// deleteRequest is a create request as Loki reads it: start and end in Unix nanoseconds.
type deleteRequest struct {
	query      *logqlpkg.LogQuery
	start, end int64
}

// deleteRequestOf reads and checks a create request in Loki's order: the query (not set, not a log query), the
// start (not set, not unix seconds or RFC3339), the end (now when not set), the start against the end, and for a
// query with a line or label filter its max_interval. Times are compared in Unix nanoseconds, as Loki compares
// them: a time past the year 2262 does not fit and wraps (an end of 9999999999 is before any start).
func deleteRequestOf(form url.Values, now time.Time) (deleteRequest, error) {
	raw := form.Get("query")
	if raw == "" {
		return deleteRequest{}, lokierr.DeleteQueryNotSet()
	}
	expr, err := logqlpkg.Parse(raw)
	if err != nil {
		return deleteRequest{}, lokierr.DeleteQueryInvalid(err)
	}
	lq, ok := expr.(*logqlpkg.LogQuery)
	if !ok {
		return deleteRequest{}, lokierr.DeleteQueryInvalid(errors.New("not a log query"))
	}
	startRaw := form.Get("start")
	if startRaw == "" {
		return deleteRequest{}, lokierr.DeleteStartNotSet()
	}
	start, ok := deleteTime(startRaw)
	if !ok {
		return deleteRequest{}, lokierr.DeleteTimeInvalid("start")
	}
	// A start Loki reads past 2262 wraps below zero; the request would reach back to 1677 and delete a tenant's whole
	// history. What Loki answers for it is not recorded, so the proxy serves no such request.
	if start < 0 || !inNanosRange(startRaw) {
		return deleteRequest{}, notServed("delete-start-out-of-range", errors.New("a start that does not fit in Unix nanoseconds"))
	}
	end := now.UnixNano()
	if endRaw := form.Get("end"); endRaw != "" {
		if end, ok = deleteTime(endRaw); !ok {
			return deleteRequest{}, lokierr.DeleteTimeInvalid("end")
		}
		// Ten digits of seconds past 2262 wrap below every start (Loki's answer, recorded); an RFC3339 end out of
		// range is not recorded, so it is served by no request.
		if !allDigits(endRaw) && !inNanosRange(endRaw) {
			return deleteRequest{}, notServed("delete-end-out-of-range", errors.New("an end that does not fit in Unix nanoseconds"))
		}
	}
	if start >= end {
		return deleteRequest{}, lokierr.DeleteStartNotBeforeEnd()
	}
	if hasDeleteFilter(lq) {
		if raw := form.Get("max_interval"); raw != "" {
			interval, ok := deleteMaxInterval(raw)
			if !ok {
				return deleteRequest{}, lokierr.DeleteMaxIntervalInvalid()
			}
			if span := time.Duration(end - start); interval > span {
				return deleteRequest{}, lokierr.DeleteMaxIntervalTooLong(span)
			}
		}
	}
	return deleteRequest{query: lq, start: start, end: end}, nil
}

// deleteTime reads a delete request's time in Unix nanoseconds: ten digits of unix seconds, or RFC3339 (fractions
// and offsets allowed). Loki refuses a number of any other length (1, 0, milliseconds, a leading zero), a sign, a
// fraction, spaces and a date without its T.
func deleteTime(raw string) (int64, bool) {
	if len(raw) == 10 && allDigits(raw) {
		sec, err := strconv.ParseInt(raw, 10, 64)
		return sec * int64(time.Second), err == nil // wraps past 2262, as Loki's time does
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	return t.UnixNano(), err == nil
}

// inNanosRange reports whether an RFC3339 time fits in Unix nanoseconds (1678 to 2262); a time of digits, Loki's
// unix seconds, is checked by its caller.
func inNanosRange(raw string) bool {
	if allDigits(raw) {
		sec, err := strconv.ParseInt(raw, 10, 64)
		return err == nil && sec <= math.MaxInt64/int64(time.Second)
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	return err == nil && !t.Before(minNanosTime) && !t.After(maxNanosTime)
}

// The times a Unix nanosecond count holds.
var (
	minNanosTime = time.Unix(0, math.MinInt64)
	maxNanosTime = time.Unix(0, math.MaxInt64)
)

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// deleteMaxInterval reads max_interval: one or more whole numbers of hours, minutes or seconds (90s, 1h30m), above
// zero. Loki refuses another unit (1d, 1ms), a sign and a bare number.
func deleteMaxInterval(raw string) (time.Duration, bool) {
	var total time.Duration
	for i := 0; i < len(raw); {
		j := i
		for j < len(raw) && raw[j] >= '0' && raw[j] <= '9' {
			j++
		}
		if j == i || j == len(raw) {
			return 0, false
		}
		n, err := strconv.ParseInt(raw[i:j], 10, 32)
		if err != nil {
			return 0, false
		}
		unit := time.Second
		switch raw[j] {
		case 'h':
			unit = time.Hour
		case 'm':
			unit = time.Minute
			if j+1 < len(raw) && raw[j+1] == 's' {
				return 0, false // ms
			}
		case 's':
		default:
			return 0, false
		}
		total += time.Duration(n) * unit
		i = j + 1
	}
	return total, total > 0
}

// hasDeleteFilter reports a query with a line or label filter: Loki splits such a request by max_interval and
// checks max_interval only for it.
func hasDeleteFilter(lq *logqlpkg.LogQuery) bool {
	for _, st := range lq.Pipeline {
		switch st.(type) {
		case *logqlpkg.LineFilterStage, *logqlpkg.LabelFilterStage:
			return true
		}
	}
	return false
}

// createDelete starts the VictoriaLogs delete task of a create request.
func (p *Proxy) createDelete(r *http.Request) error {
	req, err := deleteRequestOf(r.Form, time.Now())
	if err != nil {
		return err
	}
	r = p.withRequestScope(r)
	if err := p.deleteTenantServed(r); err != nil {
		return err
	}
	filter, err := p.deleteFilter(r.Context(), req)
	if err != nil {
		return err
	}
	tenant := r.Header.Get("X-Scope-OrgID")
	p.log.Warn("DELETE request", "tenant", tenant, "query", r.FormValue("query"),
		"start", time.Unix(0, req.start).UTC().Format(time.RFC3339Nano), "end", time.Unix(0, req.end).UTC().Format(time.RFC3339Nano), "client", r.RemoteAddr)
	body, err := p.vlDeleteCall(r.Context(), http.MethodPost, vlDeleteRunTaskPath, url.Values{"filter": {filter}})
	if err != nil {
		return err
	}
	var task struct {
		TaskID string `json:"task_id"`
	}
	_ = json.Unmarshal(body, &task)
	p.log.Warn("DELETE started", "tenant", tenant, "task_id", task.TaskID)
	return nil
}

// deleteTenantServed refuses what the proxy cannot scope a delete to: tenant-label routing, where VictoriaLogs'
// delete task has no stream filter parameter to hold the tenant (a follow-up), and the global tenant bypass.
func (p *Proxy) deleteTenantServed(r *http.Request) error {
	if p.routingForContext(r.Context()).label != "" {
		return notServed("delete-tenant-label", errors.New("a VictoriaLogs delete task takes no tenant stream filter"))
	}
	routing := p.routingForContext(r.Context())
	if _, mapped := routing.tenants["*"]; getOrgID(r.Context()) == "*" && !mapped {
		return notServed("delete-global-tenant", errors.New("a delete is never run over every tenant"))
	}
	return nil
}

// deleteFilter is the VictoriaLogs filter a create request deletes: its time range and the rows the same log query
// reads, from the query plan with the read path's lowering profile (lower.PlanLogFilter). A query whose lowering
// needs a pipe (a parser, a formatting stage, a filter on a label a parser derives) is one Loki accepts and
// VictoriaLogs cannot delete exactly: Loki's 500, the construct in the log line.
func (p *Proxy) deleteFilter(ctx context.Context, req deleteRequest) (string, error) {
	if err := lineFilterRegexpsCompile(req.query); err != nil {
		return "", notServed("delete-query", err)
	}
	normalized := req.query.String()
	streamLabels := p.plainStreamLabelNames(ctx, normalized)
	p.configMu.RLock()
	labelFn := translator.WithStreamLabels(p.labelTranslator.ToVL, streamLabels)
	streamFields := p.streamFieldsMap
	p.configMu.RUnlock()
	caps := p.logsqlCapabilities()
	caps.NoDetectedLevelBodyScan = !p.detectedLevelBodyScan
	plan, err := logLoweringPlan(ctx, logqlpkg.CanonicalizeLogRanges(normalized))
	if err != nil {
		return "", notServed("delete-query", err)
	}
	f, err := lower.PlanLogFilter(plan, loweringProfile(labelFn, streamFields, caps, false))
	if err != nil {
		return "", notServed("delete-query", err)
	}
	return logsql.GuardReservedFieldCase(deleteFilterExpr(f, req.start, req.end).String()), nil
}

// lineFilterRegexpsCompile reports a line filter regexp that does not compile: Loki accepts such a delete (it reads
// the query without building its pipeline), and VictoriaLogs cannot be sent it.
func lineFilterRegexpsCompile(lq *logqlpkg.LogQuery) error {
	for _, st := range lq.Pipeline {
		lf, ok := st.(*logqlpkg.LineFilterStage)
		if !ok || (lf.Op != logqlpkg.LineFilterMatchRe && lf.Op != logqlpkg.LineFilterExcludeRe) {
			continue
		}
		values := []string{lf.Value}
		for _, alt := range lf.Or {
			values = append(values, alt.Value)
		}
		for _, v := range values {
			if _, err := regexp.Compile(v); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteFilterExpr is the filter of the rows of f between start and end (Unix nanoseconds), both included.
func deleteFilterExpr(f logsql.FilterExpr, start, end int64) logsql.FilterExpr {
	window := logsql.TimeFilter{Range: "[" + time.Unix(0, start).UTC().Format(time.RFC3339Nano) + ", " + time.Unix(0, end).UTC().Format(time.RFC3339Nano) + "]"}
	if f == nil {
		return window
	}
	return logsql.AllOf{window, logsql.Group{Expr: f}}
}

// cancelDelete stops the tenant's running delete task of request_id.
func (p *Proxy) cancelDelete(r *http.Request) error {
	id := r.FormValue("request_id")
	if id == "" {
		return nil // Loki answers a cancel without an id with 204
	}
	if !allDigits(id) || len(id) > 20 {
		return lokierr.DeleteRequestNotFound()
	}
	r = p.withRequestScope(r)
	if err := p.deleteTenantServed(r); err != nil {
		return err
	}
	owned, err := p.ownsDeleteTask(r, id)
	if err != nil {
		return err
	}
	if !owned {
		return lokierr.DeleteRequestNotFound()
	}
	p.log.Warn("DELETE cancel", "tenant", r.Header.Get("X-Scope-OrgID"), "task_id", id, "client", r.RemoteAddr)
	_, err = p.vlDeleteCall(r.Context(), http.MethodPost, vlDeleteStopTaskPath, url.Values{"task_id": {id}})
	return err
}

// vlDeleteTask is a VictoriaLogs delete task as /delete/active_tasks lists it.
type vlDeleteTask struct {
	TaskID    string `json:"task_id"`
	TenantIDs []struct {
		AccountID uint32 `json:"account_id"`
		ProjectID uint32 `json:"project_id"`
	} `json:"tenant_ids"`
}

// ownsDeleteTask reports whether VictoriaLogs runs a delete task of id for the request's tenant alone: the
// task's tenant is the one tenant whose AccountID and ProjectID the request carries to VictoriaLogs.
// /delete/stop_task takes no tenant, so this is the only tenant check of a cancel.
func (p *Proxy) ownsDeleteTask(r *http.Request, id string) (bool, error) {
	probe := (&http.Request{Header: make(http.Header)}).WithContext(r.Context())
	p.setResolvedTenantHeaders(probe, false)
	account, project := probe.Header.Get("AccountID"), probe.Header.Get("ProjectID")
	if account == "" {
		account = "0"
	}
	if project == "" {
		project = "0"
	}
	body, err := p.vlDeleteCall(r.Context(), http.MethodGet, vlDeleteActiveTasksPath, nil)
	if err != nil {
		return false, err
	}
	var tasks []vlDeleteTask
	if err := json.Unmarshal(body, &tasks); err != nil {
		return false, lokierr.BackendFailed(fmt.Errorf("reading VictoriaLogs' delete tasks: %w", err))
	}
	for _, t := range tasks {
		if t.TaskID != id {
			continue
		}
		return len(t.TenantIDs) == 1 && strconv.FormatUint(uint64(t.TenantIDs[0].AccountID), 10) == account &&
			strconv.FormatUint(uint64(t.TenantIDs[0].ProjectID), 10) == project, nil
	}
	return false, nil
}

// vlDeleteCall sends one call of VictoriaLogs' delete task API with the request's tenant headers and returns its
// body (at most vlDeleteTasksBodyLimit bytes). A failed or refused call is Loki's 500.
func (p *Proxy) vlDeleteCall(ctx context.Context, method, path string, params url.Values) ([]byte, error) {
	var (
		resp *http.Response
		err  error
	)
	if method == http.MethodGet {
		resp, err = p.vlGet(ctx, path, params)
	} else {
		resp, err = p.vlPost(ctx, path, params)
	}
	if err != nil {
		return nil, lokierr.BackendFailed(fmt.Errorf("%w: %w", errDeleteVictoriaLogs, err))
	}
	defer func() { _ = resp.Body.Close() }()
	// The answers are small (a task id, a status, the running tasks): read them without a preallocated buffer.
	body, err := io.ReadAll(io.LimitReader(resp.Body, vlDeleteTasksBodyLimit+1))
	if err == nil && len(body) > vlDeleteTasksBodyLimit {
		err = fmt.Errorf("%s answered more than %d bytes", path, vlDeleteTasksBodyLimit)
	}
	if err != nil {
		return nil, lokierr.BackendFailed(fmt.Errorf("%w: %w", errDeleteVictoriaLogs, err))
	}
	if resp.StatusCode/100 != 2 {
		return nil, lokierr.BackendFailed(fmt.Errorf("%w: %s answered %d: %s", errDeleteVictoriaLogs, path, resp.StatusCode,
			strings.TrimSpace(string(body[:min(len(body), maxUpstreamErrorBodyBytes)]))))
	}
	return body, nil
}
