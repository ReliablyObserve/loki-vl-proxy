package config

import "path/filepath"

// Documents are the generated reference pages, keyed by their path relative to
// the repository root.
const (
	ConfigurationReferencePath = "docs/reference/configuration-reference.md"
	LimitsRegistryPath         = "docs/reference/limits-registry.md"
	ErrorsAndAlertsPath        = "docs/reference/errors-and-alerts.md"
	// LimitsJSONPath is the machine-readable export the conformance registry
	// reads, so limits are inventoried from this registry rather than listed
	// again by hand.
	LimitsJSONPath = "conformance/registry/generated/proxy/limits.json"
)

// MainGoPath is where the flags are declared.
const MainGoPath = "cmd/proxy/main.go"

// Generate renders every reference document from the flags declared in the
// repository at root.
func Generate(root string) (map[string]string, error) {
	flags, err := ParseFlags(filepath.Join(root, MainGoPath))
	if err != nil {
		return nil, err
	}
	limits, err := RenderLimitsRegistry(flags)
	if err != nil {
		return nil, err
	}
	errors, err := RenderErrorsAndAlerts(flags)
	if err != nil {
		return nil, err
	}
	limitsJSON, err := RenderLimitsJSON(flags)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		ConfigurationReferencePath: RenderConfigurationReference(flags),
		LimitsRegistryPath:         limits,
		ErrorsAndAlertsPath:        errors,
		LimitsJSONPath:             limitsJSON,
	}, nil
}
