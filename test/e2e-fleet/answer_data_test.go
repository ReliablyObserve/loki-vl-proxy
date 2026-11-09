//go:build e2e

package e2e_fleet

// answerData is the data object of a decoded query answer without its stats: Loki's stats describe the request
// that produced the answer (its timing, its cache use), so two answers with the same data differ there.
func answerData(answer map[string]interface{}) interface{} {
	data, ok := answer["data"].(map[string]interface{})
	if !ok {
		return answer["data"]
	}
	out := make(map[string]interface{}, len(data))
	for k, v := range data {
		if k != "stats" {
			out[k] = v
		}
	}
	return out
}
