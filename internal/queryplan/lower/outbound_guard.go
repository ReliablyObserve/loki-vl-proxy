package lower

import (
	"reflect"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// GuardOutbound is op with the outbound guard (logsql.GuardReservedFieldCase) applied to every VictoriaLogs query
// text it holds: what the proxy sends for op. The lowering writes an unwrap of a label named _time, _stream or
// _stream_id in another case in that form already (UnwrapsReservedFieldOtherCase), and the shadows compare it with
// the string translator's operators passed through GuardOutbound. An op whose texts the guard leaves alone is
// returned as it is.
func GuardOutbound(op physical.Op) physical.Op {
	v := reflect.ValueOf(&op).Elem()
	if r, changed := guardOps(v); changed {
		return r.Interface().(physical.Op)
	}
	return op
}

var vlQueryType, vlStatsType = reflect.TypeOf(physical.VLQuery{}), reflect.TypeOf(physical.VLStats{})

// guardOps walks an operator tree and guards the Text of each VLQuery and VLStats in it.
func guardOps(v reflect.Value) (reflect.Value, bool) {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v, false
		}
		r, changed := guardOps(v.Elem())
		if !changed {
			return v, false
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(r)
		return out, true
	case reflect.Struct:
		if v.Type() == vlQueryType || v.Type() == vlStatsType {
			text := v.FieldByName("Text").String()
			guarded := logsql.GuardReservedFieldCase(text)
			if guarded == text {
				return v, false
			}
			out := reflect.New(v.Type()).Elem()
			out.Set(v)
			out.FieldByName("Text").SetString(guarded)
			return out, true
		}
		var out reflect.Value
		for i := 0; i < v.NumField(); i++ {
			if !v.Type().Field(i).IsExported() {
				continue
			}
			r, changed := guardOps(v.Field(i))
			if !changed {
				continue
			}
			if !out.IsValid() {
				out = reflect.New(v.Type()).Elem()
				out.Set(v)
			}
			out.Field(i).Set(r)
		}
		if out.IsValid() {
			// A decoded operator keeps the text it was read from (Binary.Text), which the encoder writes back as
			// it is: it no longer is the operator's text.
			if f := out.FieldByName("Text"); f.IsValid() && f.Kind() == reflect.String && f.CanSet() {
				f.SetString("")
			}
			return out, true
		}
	case reflect.Slice:
		var out reflect.Value
		for i := 0; i < v.Len(); i++ {
			r, changed := guardOps(v.Index(i))
			if !changed {
				continue
			}
			if !out.IsValid() {
				out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
				reflect.Copy(out, v)
			}
			out.Index(i).Set(r)
		}
		if out.IsValid() {
			return out, true
		}
	case reflect.Pointer:
		if v.IsNil() {
			return v, false
		}
		r, changed := guardOps(v.Elem())
		if !changed {
			return v, false
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(r)
		return out, true
	}
	return v, false
}
