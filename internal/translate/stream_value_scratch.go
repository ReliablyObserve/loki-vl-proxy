package translate

import (
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// A log query that filters on a stream label after a json or logfmt parser reads the stream's value, not the
// parsed key of the same name that unpack_json writes over the stored field (Loki exposes the parsed value as
// name_extracted and keeps the stream label). The string translator copies the stored value aside before the
// first parser, fills the copy from the parsed key where the stream has none, filters on the copy and deletes the
// copies at the end.

// StreamValueScratch prefixes the scratch field of each stream label name the pipeline reads after a parser.
const StreamValueScratch = "__lxsv_"

// StreamValueCopies are the `| copy name as __lxsv_name` pipes ahead of the first parser, one per name.
func StreamValueCopies(names []string) []logsql.Pipe {
	pipes := make([]logsql.Pipe, len(names))
	for i, n := range names {
		pipes[i] = logsql.PipeCopy{Pairs: [][2]string{{n, StreamValueScratch + n}}}
	}
	return pipes
}

// StreamValueFills are the pipes behind a parser that give each scratch field the parsed value of any original
// key its name sanitizes from, while the entry has no stored value: one `format if` per name and key.
func StreamValueFills(names []string) []logsql.Pipe {
	var pipes []logsql.Pipe
	for _, n := range names {
		sv := StreamValueScratch + n
		for _, key := range ParsedKeyVariants(n) {
			pipes = append(pipes, logsql.PipeFormat{
				Template:    "<" + key + ">",
				ResultField: sv,
				If: logsql.AllOf{
					logsql.NotExpr{Expr: logsql.FieldFilter{Field: sv, Op: logsql.FieldOpAny}},
					logsql.FieldFilter{Field: strconv.Quote(key), Op: logsql.FieldOpAny},
				},
			})
		}
	}
	return pipes
}

// StreamValueDelete is the `| delete __lxsv_*` pipe that ends the query.
func StreamValueDelete() logsql.Pipe {
	return logsql.PipeDelete{Labels: []string{StreamValueScratch + "*"}}
}
