package queryplan

import "slices"

// The series identity of a metric over a pipeline. Loki puts every sample in the series of the labels its line
// carries after the last stage, so the samples of every line that ends with the same labels are one series, whatever
// stream they come from. The stages that decide those labels:
//
//   - a keep without a matcher deletes every label it does not name, stream labels and structured metadata included,
//     and never the error labels (__error__, __error_details__, __preserve_error__);
//   - a drop without a matcher deletes the labels it names;
//   - a parser adds the keys it extracts from each line;
//   - the unwrap stage takes its label out of the sample;
//   - a label_format, and a keep or a drop with a matcher, change the labels by their values.
//
// SeriesLabels reads them from the typed stages alone, in Loki's terms (labels, not VictoriaLogs fields): the
// lowering maps the result to the fields VictoriaLogs groups by. It is computed on demand, not by Resolve, so a plan
// that does not ask pays nothing.

// errorLabels are the labels a keep never removes.
var errorLabels = [...]string{"__error__", "__error_details__", "__preserve_error__"}

// SeriesLabels is what the stages leave of a sample's labels (see SampleLabels).
func SeriesLabels(stages []Stage) SampleLabels {
	var out SampleLabels
	for _, st := range stages {
		switch st := st.(type) {
		case *Keep:
			if len(st.Matchers) > 0 {
				out = SampleLabels{Opaque: true}
				continue
			}
			out = SampleLabels{Kept: true, Names: withoutErrorLabels(st.Labels)}
		case *Drop:
			if len(st.Matchers) > 0 {
				out.Opaque = true
				continue
			}
			for _, l := range st.Labels {
				out.Names = without(out.Names, l)
				if !slices.Contains(out.Dropped, l) {
					out.Dropped = append(out.Dropped, l)
				}
			}
		case *Parse:
			out.Dynamic = true
		case *LabelFormat:
			out.Opaque = true
		case *Unwrap:
			if out.Kept && !out.Dynamic && !slices.Contains(out.Names, st.Label) {
				// The keep deleted the label the unwrap reads: every sample is a conversion error, which the plan does
				// not model.
				out.Opaque = true
			}
			out.Names = without(out.Names, st.Label)
			out.Unwrapped = st.Label
		}
	}
	return out
}

// without is names less name. The keep's own list is never written to: the last name is cut off with the capacity
// cut too (an append then copies), any other is removed from a copy.
func without(names []string, name string) []string {
	switch i := slices.Index(names, name); {
	case i < 0:
		return names
	case i == len(names)-1:
		return names[:i:i]
	default:
		return slices.Delete(slices.Clone(names), i, i+1)
	}
}

// withoutErrorLabels is labels without the error labels, shared with labels when it holds none.
func withoutErrorLabels(labels []string) []string {
	for i, l := range labels {
		if slices.Contains(errorLabels[:], l) {
			kept := slices.Clone(labels[:i])
			for _, l := range labels[i+1:] {
				if !slices.Contains(errorLabels[:], l) {
					kept = append(kept, l)
				}
			}
			return kept
		}
	}
	return labels
}

// KeptIdentity reports labels that decide the series of every sample: a keep without a matcher names them, no
// parser after it adds a key, and no stage after it changes the labels by their values. The labels are the keep's
// less the error labels, the ones a later drop removes and the unwrapped one; none (an empty slice) is one series
// without labels. ok is false when the stream, or the parsers, still tell the series apart.
func (o SampleLabels) KeptIdentity() (labels []string, ok bool) {
	if !o.Kept || o.Dynamic || o.Opaque {
		return nil, false
	}
	return o.Names, true
}

// SampleLabels are the labels a sample carries after the last stage of a pipeline: Loki's series identity of a
// metric over it (SeriesLabels). A type of its own, not PipelineSem's OutLabels, so the resolver's pipeline
// attributes keep their size.
type SampleLabels struct {
	// Kept says a keep stage without a matcher deleted every label it does not name, the stream labels included.
	Kept bool
	// Names are, when Kept, the labels that keep leaves, in its order: without the error labels (a keep never
	// removes them), the labels a later drop removes and the unwrapped label.
	Names []string
	// Dynamic says a parser stage after the last keep (or in a pipeline without one) adds the keys it extracts
	// from each line.
	Dynamic bool
	// Dropped are the labels a drop stage without a matcher removes after the last keep.
	Dropped []string
	// Opaque says a stage changes the labels by their values (a label_format, a keep or a drop with a matcher), or
	// the keep deleted the label the unwrap reads: no identity is claimed.
	Opaque bool
	// Unwrapped is the label the unwrap stage takes out of the sample ("" without one).
	Unwrapped string
}
