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

// SeriesLabels is what the stages leave of a sample's labels (see OutLabels).
func SeriesLabels(stages []Stage) OutLabels {
	var out OutLabels
	for _, st := range stages {
		switch st := st.(type) {
		case *Keep:
			if len(st.Matchers) > 0 {
				out = OutLabels{Opaque: true}
				continue
			}
			out = OutLabels{Kept: true, Names: withoutErrorLabels(st.Labels)}
		case *Drop:
			if len(st.Matchers) > 0 {
				out.Opaque = true
				continue
			}
			for _, l := range st.Labels {
				if i := slices.Index(out.Names, l); i >= 0 {
					out.Names = slices.Delete(slices.Clone(out.Names), i, i+1)
				}
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
			if i := slices.Index(out.Names, st.Label); i >= 0 {
				out.Names = slices.Delete(slices.Clone(out.Names), i, i+1)
			}
			out.Unwrapped = st.Label
		}
	}
	return out
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
func (o OutLabels) KeptIdentity() (labels []string, ok bool) {
	if !o.Kept || o.Dynamic || o.Opaque {
		return nil, false
	}
	return o.Names, true
}
