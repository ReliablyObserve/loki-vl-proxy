package lokierr

import "time"

// Loki's answers to its delete API (/loki/api/v1/delete), recorded from a Loki that serves it (rows r1700 and
// later of testdata/loki_error_answers.jsonl). A create request is checked in this order: the query (not set, then
// not a log query), the start (not set, then not unix seconds or RFC3339), the end, the start against the end, and
// for a query with a filter the max_interval.
const (
	textNoOrgID                 = "no org id"
	textDeleteQueryNotSet       = "query not set"
	textDeleteQueryInvalid      = "invalid query expression"
	textDeleteStartNotSet       = "start time not set"
	textDeleteTimeInvalidSuffix = " time: require unix seconds or RFC3339 format"
	textDeleteStartNotBeforeEnd = "start time can't be greater than or equal to end time"
	textDeleteEndInFuture       = "deletes in the future are not allowed"
	textDeleteMaxInterval       = "invalid max_interval: valid time units are 's', 'm', 'h'"
	textDeleteMaxIntervalLong   = "max_interval can't be greater than the interval to be deleted ("
	textDeleteNotFound          = "could not find delete request with given id"
)

// NoOrgID is a request without the credential its route needs; cause says what was missing, for the log only.
func NoOrgID(cause error) *Error { return newError(MsgNoOrgID, textNoOrgID, cause) }

// DeleteQueryNotSet is a delete request without a query.
func DeleteQueryNotSet() *Error { return newError(MsgDeleteQueryNotSet, textDeleteQueryNotSet, nil) }

// DeleteQueryInvalid is a delete request whose query is not a log query.
func DeleteQueryInvalid(cause error) *Error {
	return newError(MsgDeleteQueryInvalid, textDeleteQueryInvalid, cause)
}

// DeleteStartNotSet is a delete request without a start.
func DeleteStartNotSet() *Error { return newError(MsgDeleteStartNotSet, textDeleteStartNotSet, nil) }

// DeleteTimeInvalid is a delete request whose start or end (param) is neither unix seconds nor RFC3339.
func DeleteTimeInvalid(param string) *Error {
	return newError(MsgDeleteTimeInvalid, "invalid "+param+textDeleteTimeInvalidSuffix, nil)
}

// DeleteStartNotBeforeEnd is a delete request whose start is not before its end.
func DeleteStartNotBeforeEnd() *Error {
	return newError(MsgDeleteStartNotBeforeEnd, textDeleteStartNotBeforeEnd, nil)
}

// DeleteEndInFuture is a delete request whose end is after now.
func DeleteEndInFuture() *Error { return newError(MsgDeleteEndInFuture, textDeleteEndInFuture, nil) }

// DeleteMaxIntervalInvalid is a delete request with a filter whose max_interval is not a positive duration of
// seconds, minutes and hours.
func DeleteMaxIntervalInvalid() *Error {
	return newError(MsgDeleteMaxIntervalInvalid, textDeleteMaxInterval, nil)
}

// DeleteMaxIntervalTooLong is a delete request whose max_interval is longer than the interval it deletes,
// interval as Go prints a duration (1h0m0s).
func DeleteMaxIntervalTooLong(interval time.Duration) *Error {
	return newError(MsgDeleteMaxIntervalTooLong, textDeleteMaxIntervalLong+interval.String()+")", nil)
}

// DeleteRequestNotFound is a cancel of a delete request the tenant does not have.
func DeleteRequestNotFound() *Error {
	return newError(MsgDeleteRequestNotFound, textDeleteNotFound, nil)
}
