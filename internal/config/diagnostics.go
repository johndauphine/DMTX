package config

import "fmt"

// ConfigDiagnostic is a deterministic, secret-free configuration fact that
// callers can render before execution. Diagnostics never participate in
// configuration hashes or serialized configuration.
type ConfigDiagnostic struct {
	Severity       string `json:"severity"`
	Code           string `json:"code"`
	Field          string `json:"field"`
	Replacement    string `json:"replacement,omitempty"`
	RemovalVersion string `json:"removal_version,omitempty"`
	Message        string `json:"message"`
}

const (
	ConfigDiagnosticWarning         = "warning"
	ConfigDiagnosticDeprecatedField = "config.deprecated_field"
)

// Diagnostics returns a copy of the deterministic, secret-free diagnostics
// produced while parsing this configuration.
func (value Config) Diagnostics() []ConfigDiagnostic {
	return append([]ConfigDiagnostic(nil), value.diagnostics...)
}

func deprecatedFieldDiagnostic(
	field string,
	replacement string,
	removalVersion string,
) ConfigDiagnostic {
	message := fmt.Sprintf("%s is deprecated; use %s", field, replacement)
	if removalVersion != "" {
		message += "; removal is scheduled for version " + removalVersion
	}
	return ConfigDiagnostic{
		Severity:       ConfigDiagnosticWarning,
		Code:           ConfigDiagnosticDeprecatedField,
		Field:          field,
		Replacement:    replacement,
		RemovalVersion: removalVersion,
		Message:        message,
	}
}
