package translator

import "testing"

func TestExtractedLabelPipes(t *testing.T) {
	for _, tc := range []struct {
		name, unpack  string
		labels        []string
		stored        map[string]string
		before, after string
	}{
		{"json", "unpack_json", []string{"level_extracted"}, nil,
			``,
			` | unpack_json from _msg fields (level) result_prefix "__lxp0_" | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level` +
				` | format if (__lxp_stream:~"[{,]level=\"" __lxp_level:*) "<__lxp_level>" as level_extracted | delete __lxp*, __lxs*`},
		{"logfmt, two labels, one stored under another spelling", "unpack_logfmt", []string{"level_extracted", "service_version_extracted"}, map[string]string{"service_version": "service.version"},
			` | format if (service.version:*) "<service.version>" as __lxs_service_version`,
			` | unpack_logfmt from _msg fields (level, service_version) result_prefix "__lxp0_" | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level | copy __lxp0_service_version as __lxp_service_version` +
				` | format if (__lxp_stream:~"[{,]level=\"" __lxp_level:*) "<__lxp_level>" as level_extracted` +
				` | format if ((__lxp_stream:~"[{,]service[._]version=\"" or __lxs_service_version:*) __lxp_service_version:*) "<__lxp_service_version>" as service_version_extracted | delete __lxp*, __lxs*`},
		{"service_name is on every stream", "unpack_json", []string{"service_name_extracted"}, nil,
			``,
			` | unpack_json from _msg fields (service_name) result_prefix "__lxp0_" | copy __lxp0_service_name as __lxp_service_name` +
				` | format if (__lxp_service_name:*) "<__lxp_service_name>" as service_name_extracted | delete __lxp*, __lxs*`},
		{"labels that are not name_extracted add nothing", "unpack_json", []string{"level", "_extracted", "a.b_extracted"}, nil, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, after := ExtractedLabelPipes(tc.unpack, tc.labels, tc.stored)
			if before != tc.before || after != tc.after {
				t.Fatalf("pipes\n before: %s\n  after: %s\nwant before: %s\n want after: %s", before, after, tc.before, tc.after)
			}
		})
	}
}
