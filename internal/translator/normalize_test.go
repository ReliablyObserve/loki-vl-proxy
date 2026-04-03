package translator

import "testing"

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty",
			input: "",
			want:  "*",
		},
		{
			name:  "collapse spaces",
			input: `{app="nginx"}  |=  "error"`,
			want:  `{app="nginx"} |= "error"`,
		},
		{
			name:  "sort label matchers",
			input: `{host="h1",app="nginx"}`,
			want:  `{app="nginx",host="h1"}`,
		},
		{
			name:  "already sorted",
			input: `{app="nginx",host="h1"}`,
			want:  `{app="nginx",host="h1"}`,
		},
		{
			name:  "sort with regex",
			input: `{namespace=~"prod.*",app="nginx"}`,
			want:  `{app="nginx",namespace=~"prod.*"}`,
		},
		{
			name:  "no stream selector",
			input: `error`,
			want:  `error`,
		},
		{
			name:  "tabs and newlines collapsed",
			input: "{app=\"nginx\"}\t|=\t\"error\"",
			want:  `{app="nginx"} |= "error"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeQuery(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeQuery(%q)\n  got  = %q\n  want = %q", tt.input, got, tt.want)
			}
		})
	}
}
