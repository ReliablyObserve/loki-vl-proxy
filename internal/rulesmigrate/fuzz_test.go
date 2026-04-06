package rulesmigrate

import "testing"

func FuzzConvertWithOptions(f *testing.F) {
	seeds := [][]byte{
		[]byte("groups:\n  - name: api\n    rules:\n      - alert: HighErrorRate\n        expr: sum by (app) (rate({app=\"api\"} |= \"error\" [5m]))\n"),
		[]byte("compat.rules:\n  - name: api\n    rules:\n      - record: api_requests_total:rate5m\n        expr: sum by (app) (rate({app=\"api\"}[5m]))\n"),
		[]byte("groups:\n  - name: risky\n    rules:\n      - alert: Join\n        expr: rate({app=\"api\"}[5m]) / on(app) group_left(team) rate({app=\"api\"}[5m])\n"),
		[]byte("not: [valid"),
		[]byte(""),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		_, _, _ = ConvertWithOptions(input, ConvertOptions{})
		_, _, _ = ConvertWithOptions(input, ConvertOptions{AllowRisky: true})
	})
}
