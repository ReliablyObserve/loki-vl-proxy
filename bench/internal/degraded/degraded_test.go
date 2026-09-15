package degraded

import (
	"net/http"
	"strings"
	"testing"
)

func TestFromHeaders(t *testing.T) {
	h := http.Header{}
	if got := FromHeaders(h); len(got) != 0 {
		t.Fatalf("clean headers reported %v", got)
	}
	for _, name := range Headers {
		h := http.Header{}
		h.Set(name, "1")
		if got := FromHeaders(h); len(got) != 1 || !strings.Contains(got[0], name) {
			t.Errorf("%s: got %v", name, got)
		}
	}
}

func TestFromBody(t *testing.T) {
	cases := map[string]int{
		`{"status":"success","data":{"resultType":"matrix","result":[]}}`:                                   0,
		`{"status":"success","warnings":[],"data":{}}`:                                                      0,
		`{"status":"success","warnings":null,"data":{}}`:                                                    0,
		`{"status":"success","warnings":["partial results"],"data":{}}`:                                     1,
		`{"status":"success","data":{"result":[{"stream":{"a":"1"},"values":[["1","\"warnings\":[1]"]]}]}}`: 0,
		`not json`: 0,
		``:         0,
	}
	for body, want := range cases {
		if got := FromBody([]byte(body)); len(got) != want {
			t.Errorf("%s: got %v, want %d reasons", body, got, want)
		}
	}
}

func TestScannerAcrossChunks(t *testing.T) {
	cases := map[string]bool{
		`{"status":"success","warnings":["x"],"data":{}}`:                               true,
		`{"status":"success","warnings" : [ "x" ],"data":{}}`:                           true,
		`{"data":["warnings","x"]}`:                                                     false,
		`{"data":{"result":[{"metric":{"k":"warnings"}}]}}`:                             false,
		`{"status":"success","warnings": [ "x" ],"data":{}}`:                            true,
		`{"status":"success","warnings":[],"data":{}}`:                                  false,
		`{"status":"success","warnings":[ ],"data":{}}`:                                 false,
		`{"data":{"result":[{"values":[["1","{\"warnings\":[\"no\"]}"]]}]}}`:            false,
		`{"data":{"result":[{"metric":{"warnings":"x"}}]}}`:                             false,
		`{"data":[` + strings.Repeat(`"abcdefgh",`, 5000) + `"z"],"warnings":["late"]}`: true,
		`{"status":"success","data":{},"warnings":["at the end"]}`:                      true,
	}
	for body, want := range cases {
		for _, size := range []int{1, 2, 3, 7, 13, 32 * 1024} {
			s := &Scanner{}
			for i := 0; i < len(body); i += size {
				end := i + size
				if end > len(body) {
					end = len(body)
				}
				if _, err := s.Write([]byte(body[i:end])); err != nil {
					t.Fatal(err)
				}
			}
			if s.Warnings != want {
				t.Errorf("chunk %d, body %.60q: warnings=%v want %v", size, body, s.Warnings, want)
			}
			if size == 1 && body[0] == '{' && (len(FromBody([]byte(body))) > 0) != want {
				t.Errorf("FromBody disagrees with the scanner on %.60q", body)
			}
		}
	}
}
