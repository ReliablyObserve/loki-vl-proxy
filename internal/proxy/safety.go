package proxy

import (
	"bytes"
	"errors"
	"io"
	"math"
)

var errBodyTooLarge = errors.New("body exceeds configured limit")

func safeAddCap(parts ...int) int {
	total := 0
	for _, part := range parts {
		if part <= 0 {
			continue
		}
		if total > math.MaxInt-part {
			return math.MaxInt
		}
		total += part
	}
	return total
}

func readBodyLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	cap := limit + 1
	if cap > 128*1024 {
		cap = 128 * 1024
	}
	buf := bytes.NewBuffer(make([]byte, 0, cap))
	_, err := buf.ReadFrom(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	body := buf.Bytes()
	if int64(len(body)) > limit {
		return body[:limit], errBodyTooLarge
	}
	return body, nil
}
