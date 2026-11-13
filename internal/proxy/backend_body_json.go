package proxy

import "errors"

// errVLBodyNotJSON marks a VictoriaLogs 200 answer whose body is not the
// endpoint's JSON: a row of a line-per-row body that does not parse, or a
// stats or hits body that is not one JSON document. It is a failed backend
// call, so the client gets Loki's 500 and nothing of the body; a body that
// holds no rows at all is an empty result and never this error.
var errVLBodyNotJSON = errors.New("VictoriaLogs response is not valid JSON")
