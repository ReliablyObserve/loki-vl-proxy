package main

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// VariantsFromCompose reads the proxy variants of a compose file (test/e2e-compat/docker-compose.yml): every
// service whose command has -listen is a proxy; its URL is its published port on host, its name its container
// name without the "e2e-proxy" prefix, its flags the command's flags. The variant whose URL is defaultURL is the
// default proxy: it is not a variant, and its flags are returned as the flags the default proxy runs with.
func VariantsFromCompose(path, host, defaultURL string) (variants []Variant, defaultFlags []string, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var doc struct {
		Services map[string]struct {
			ContainerName string   `yaml:"container_name"`
			Ports         []string `yaml:"ports"`
			Command       any      `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	def, _ := url.Parse(defaultURL)
	names := make([]string, 0, len(doc.Services))
	for name := range doc.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		svc := doc.Services[name]
		// A command is a list for the proxies and a string for the others.
		var command []string
		if list, ok := svc.Command.([]any); ok {
			for _, c := range list {
				command = append(command, fmt.Sprint(c))
			}
		}
		isProxy := false
		for _, c := range command {
			if strings.HasPrefix(c, "-listen=") {
				isProxy = true
			}
		}
		if !isProxy || len(svc.Ports) == 0 {
			continue
		}
		parts := strings.Split(svc.Ports[0], ":")
		if len(parts) < 2 {
			continue
		}
		hostPort := parts[len(parts)-2]
		var flags []string
		for _, c := range command {
			if strings.HasPrefix(c, "-") {
				flags = append(flags, c)
			}
		}
		u := "http://" + host + ":" + hostPort
		if def != nil && def.Port() == hostPort {
			defaultFlags = flags
			continue
		}
		short := strings.TrimPrefix(strings.TrimPrefix(svc.ContainerName, "e2e-proxy"), "-")
		if short == "" {
			short = strings.TrimPrefix(name, "loki-vl-proxy-")
		}
		variants = append(variants, Variant{Name: short, URL: u, Flags: flags})
	}
	return variants, defaultFlags, nil
}
