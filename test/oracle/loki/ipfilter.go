package main

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/grafana/loki/v3/pkg/logql/log"
)

// The ipfilter command answers, for each input, whether Loki's ip() line filter keeps the line: the verdict of
// log.NewIPLineFilter(pattern, |= or !=).Filter(line), read through Loki's exported log package only. The proxy's
// tests compare its ip() line filter regexp with these answers.

// IPFilterInput is one ip() line filter and one line.
type IPFilterInput struct {
	ID      string `json:"id"`
	Pattern string `json:"pattern"`
	Negate  bool   `json:"negate"`
	Line    string `json:"line"`
}

// IPFilterVerdict is Loki's answer for one input; Error is set when Loki rejects the pattern.
type IPFilterVerdict struct {
	ID    string `json:"id"`
	Keep  bool   `json:"keep"`
	Error string `json:"error,omitempty"`
}

func ipfilter(in, out string) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer f.Close()
	o, err := os.Create(out)
	if err != nil {
		return err
	}
	defer o.Close()
	w := bufio.NewWriter(o)
	enc := json.NewEncoder(w)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var x IPFilterInput
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return err
		}
		ty := log.LineMatchEqual
		if x.Negate {
			ty = log.LineMatchNotEqual
		}
		v := IPFilterVerdict{ID: x.ID}
		if filter, err := log.NewIPLineFilter(x.Pattern, ty); err != nil {
			v.Error = err.Error()
		} else {
			v.Keep = filter.Filter([]byte(x.Line))
		}
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return w.Flush()
}
