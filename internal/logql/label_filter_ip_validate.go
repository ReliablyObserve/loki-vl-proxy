package logql

import (
	"fmt"
	"strconv"
)

// ipLabelFilterError is the rejection Loki answers for a label filter stage that is one ip() comparison
// (`| addr=ip("...")`, `| addr!=ip("...")`) whose pattern is not an address, a CIDR prefix or an ordered range of
// one family, or "". Recorded with the Loki oracle (test/oracle/loki pipeline, Loki's exported pipeline build) and
// on the stack for log and metric queries: the stage is quoted as Loki prints it (ip in lower case, no spaces, the
// pattern quoted), and a stage that joins several filters (`a="1", addr=ip("zzz")`, and, or) is not rejected.
func ipLabelFilterError(s *LabelFilterStage) string {
	e := s.Expr
	if e == nil || !e.IsLeaf() || e.Kind != LabelFilterValueIP || validIPPattern(e.Value) {
		return ""
	}
	return stageError("| "+e.Name+e.Cmp+"ip("+strconv.Quote(e.Value)+")", fmt.Errorf("ip: invalid pattern: %q", e.Value))
}
