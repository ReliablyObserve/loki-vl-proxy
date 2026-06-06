package logql

import "testing"

// TestMarkerMethods invokes every Go-interface marker method on the AST types
// in this package. The methods are unexported empty stubs that exist only to
// satisfy interface type discrimination — coverage treats each as a statement,
// so they have to be called from inside the package. There's no behaviour to
// assert; the test passes as long as it compiles and the calls panic-free.
func TestMarkerMethods(t *testing.T) {
	// Expr implementors.
	(&StreamSelector{}).expr()
	(&LogQuery{}).expr()
	(&RangeAggregation{}).expr()
	(&VectorAggregation{}).expr()
	(&BinOpExpr{}).expr()
	(&LiteralExpr{}).expr()
	(&OpaqueMetricExpr{}).expr()

	// Stage implementors.
	(&StreamSelector{}).stage()
	(&LineFilterStage{}).stage()
	(&ParserStage{}).stage()
	(&LabelFilterStage{}).stage()
	(&DropStage{}).stage()
	(&KeepStage{}).stage()
	(&DecolorizeStage{}).stage()
	(&UnwrapStage{}).stage()
	(&LineFormatStage{}).stage()
	(&LabelFormatStage{}).stage()
}
