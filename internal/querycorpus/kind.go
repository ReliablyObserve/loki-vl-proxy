// Package querycorpus records the queries that tests parse and translate, so
// the query corpus is built from the cases tests really run. Recording is
// compiled in only with the corpuscapture build tag; in every other build
// Record is an empty function.
package querycorpus

// Kind names the language and entry point a recorded query went through.
type Kind string

const (
	// Client is a query a client sent to a proxy route.
	Client         Kind = "client"
	LogQL          Kind = "logql"
	LogQLTranslate Kind = "logql-translate"
	LogsQL         Kind = "logsql"
)
