package proxy

// lokiParseBytes is ParseBytes from github.com/dustin/go-humanize v1.0.1
// (bytes.go), the version Loki 3.7.x pins for unwrap bytes() (convertBytes).
// It is kept, with two equivalent rewrites the linter asks for (De Morgan,
// strings.ReplaceAll), as the reference the bytes port and gate are checked
// against, so the module itself can move to a newer release: v1.1.0 parses
// integers beyond float64 precision exactly, while Loki's v1.0.1 rounds them
// through a float64 (for example "446744073709551615" is 446744073709551616).
//
// Copyright (c) 2005-2008 Dustin Sallings <dustin@spy.net>
// Licensed under the MIT License (see go-humanize's LICENSE).

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/dustin/go-humanize"
)

var lokiBytesSizeTable = map[string]uint64{
	"b":   humanize.Byte,
	"kib": humanize.KiByte,
	"kb":  humanize.KByte,
	"mib": humanize.MiByte,
	"mb":  humanize.MByte,
	"gib": humanize.GiByte,
	"gb":  humanize.GByte,
	"tib": humanize.TiByte,
	"tb":  humanize.TByte,
	"pib": humanize.PiByte,
	"pb":  humanize.PByte,
	"eib": humanize.EiByte,
	"eb":  humanize.EByte,
	// Without suffix
	"":   humanize.Byte,
	"ki": humanize.KiByte,
	"k":  humanize.KByte,
	"mi": humanize.MiByte,
	"m":  humanize.MByte,
	"gi": humanize.GiByte,
	"g":  humanize.GByte,
	"ti": humanize.TiByte,
	"t":  humanize.TByte,
	"pi": humanize.PiByte,
	"p":  humanize.PByte,
	"ei": humanize.EiByte,
	"e":  humanize.EByte,
}

func lokiParseBytes(s string) (uint64, error) {
	lastDigit := 0
	hasComma := false
	for _, r := range s {
		if !unicode.IsDigit(r) && r != '.' && r != ',' {
			break
		}
		if r == ',' {
			hasComma = true
		}
		lastDigit++
	}

	num := s[:lastDigit]
	if hasComma {
		num = strings.ReplaceAll(num, ",", "")
	}

	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, err
	}

	extra := strings.ToLower(strings.TrimSpace(s[lastDigit:]))
	if m, ok := lokiBytesSizeTable[extra]; ok {
		f *= float64(m)
		if f >= math.MaxUint64 {
			return 0, fmt.Errorf("too large: %v", s)
		}
		return uint64(f), nil
	}

	return 0, fmt.Errorf("unhandled size name: %v", extra)
}
