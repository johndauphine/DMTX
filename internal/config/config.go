// Package config loads DMTX configuration without exposing resolved secrets.
package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Endpoint struct {
	Type      string `yaml:"type"`
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Database  string `yaml:"database"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Schema    string `yaml:"schema"`
	SSLMode   string `yaml:"ssl_mode"`
	TLSCAFile string `yaml:"tls_ca_file"`
}

type Migration struct {
	TargetMode             string                 `yaml:"target_mode"`
	IncludeTables          []string               `yaml:"include_tables"`
	ExcludeTables          []string               `yaml:"exclude_tables"`
	DateUpdatedColumns     []string               `yaml:"date_updated_columns"`
	ConnectionLimit        int                    `yaml:"connection_limit"`
	Workers                int                    `yaml:"workers"`
	ChunkSize              int                    `yaml:"chunk_size"`
	Partitions             int                    `yaml:"partitions"`
	LargeTableThreshold    int64                  `yaml:"large_table_threshold"`
	ReaderParallelism      int                    `yaml:"reader_parallelism"`
	WriterParallelism      int                    `yaml:"writer_parallelism"`
	ReadAhead              int                    `yaml:"read_ahead"`
	UpsertMergeSize        int                    `yaml:"upsert_merge_size"`
	MemoryCeilingBytes     int64                  `yaml:"memory_ceiling_bytes"`
	CheckpointFrequency    int                    `yaml:"checkpoint_frequency"`
	MaxRetries             int                    `yaml:"max_retries"`
	StrictConsistency      bool                   `yaml:"strict_consistency"`
	StrictConsistencyScope StrictConsistencyScope `yaml:"strict_consistency_scope"`
	FailOnSchemaDrift      bool                   `yaml:"fail_on_schema_drift"`
	SchemaContract         *SchemaContract        `yaml:"schema_contract,omitempty"`
	// SchemaEvolution is the deprecated spelling of SchemaContract. Parse
	// canonicalizes and clears it so serialized config and hash wire shape use
	// only the preferred name.
	SchemaEvolution       *SchemaContract  `yaml:"schema_evolution,omitempty" json:"-"`
	Validation            ValidationPolicy `yaml:"validation"`
	Preflight             PreflightPolicy  `yaml:"preflight"`
	Deletes               DeletePolicy     `yaml:"deletes"`
	HistoryRetentionDays  int              `yaml:"history_retention_days"`
	Tuning                TuningMode       `yaml:"tuning"`
	RuntimeTuning         bool             `yaml:"runtime_tuning"`
	RuntimeTuningInterval time.Duration    `yaml:"runtime_tuning_interval"`
	// LegacyAIAdjust and LegacyAIAdjustInterval retain the documented
	// compatibility aliases. The preferred fields take precedence.
	LegacyAIAdjust          *bool          `yaml:"ai_adjust,omitempty" json:"-"`
	LegacyAIAdjustInterval  *time.Duration `yaml:"ai_adjust_interval,omitempty" json:"-"`
	AllowPartial            bool           `yaml:"allow_partial"`
	DestructiveAcknowledged bool           `yaml:"-" json:"-"`

	parsed         bool
	explicitFields map[string]struct{}
	parsedBaseline *Migration
}
type Config struct {
	Source    Endpoint  `yaml:"source"`
	Target    Endpoint  `yaml:"target"`
	Migration Migration `yaml:"migration"`

	diagnostics []ConfigDiagnostic
}

// SameEndpoint reports whether source and target resolve to the same physical
// database identity after engine aliases have been canonicalized.
func SameEndpoint(source, target Endpoint) bool {
	if source.Type != target.Type || source.Database == "" || target.Database == "" {
		return false
	}
	if source.Type == "sqlite" {
		return sameSQLiteFile(source.Database, target.Database)
	}
	if source.Database != target.Database {
		return false
	}
	return strings.EqualFold(source.Host, target.Host) && effectivePort(source) == effectivePort(target)
}

func sameSQLiteFile(source, target string) bool {
	sourcePath, sourceErr := CanonicalSQLitePath(source)
	targetPath, targetErr := CanonicalSQLitePath(target)
	if sourceErr != nil || targetErr != nil {
		return filepath.Clean(source) == filepath.Clean(target)
	}
	if sourcePath == targetPath || runtime.GOOS == "windows" && strings.EqualFold(sourcePath, targetPath) {
		return true
	}
	sourceInfo, sourceErr := os.Stat(source)
	targetInfo, targetErr := os.Stat(target)
	return sourceErr == nil && targetErr == nil && os.SameFile(sourceInfo, targetInfo)
}

// CanonicalSQLitePath returns one absolute, symlink-resolved filesystem
// identity for SQLite connection, state, resume, and lease decisions.
func CanonicalSQLitePath(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("SQLite database path is required")
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "file:") {
		return "", fmt.Errorf("SQLite URI database paths are unsupported; use a filesystem path")
	}
	absolute, err := filepath.Abs(filepath.Clean(value))
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return filepath.Clean(resolved), nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	parent, parentErr := filepath.EvalSymlinks(filepath.Dir(absolute))
	if parentErr == nil {
		return filepath.Join(parent, filepath.Base(absolute)), nil
	}
	if !os.IsNotExist(parentErr) {
		return "", parentErr
	}
	return absolute, nil
}

func effectivePort(endpoint Endpoint) int {
	if endpoint.Port != 0 {
		return endpoint.Port
	}
	switch endpoint.Type {
	case "postgres":
		return 5432
	case "mssql":
		return 1433
	case "mysql":
		return 3306
	case "clickhouse":
		return 9440
	default:
		return 0
	}
}

func Parse(data []byte) (Config, error) {
	migrationFields, explicitFields, err := inspectMigrationYAML(data)
	if err != nil {
		return Config{}, fmt.Errorf("parse configuration: %w", err)
	}
	var value Config
	if err := yaml.Unmarshal(data, &value); err != nil {
		return Config{}, fmt.Errorf("parse configuration: %w", err)
	}
	value.Migration.parsed = true
	value.Migration.explicitFields = explicitFields
	schemaContractNode, schemaContractSet := migrationFields["schema_contract"]
	schemaEvolutionNode, schemaEvolutionSet := migrationFields["schema_evolution"]
	if schemaContractSet &&
		(schemaContractNode.Tag == "!!null" || value.Migration.SchemaContract == nil) {
		return Config{}, fmt.Errorf("migration.schema_contract must be a mode or mapping")
	}
	if schemaEvolutionSet &&
		(schemaEvolutionNode.Tag == "!!null" || value.Migration.SchemaEvolution == nil) {
		return Config{}, fmt.Errorf("migration.schema_evolution must be a mode or mapping")
	}
	if schemaContractSet && schemaEvolutionSet {
		return Config{}, fmt.Errorf(
			"migration.schema_contract cannot be combined with deprecated migration.schema_evolution",
		)
	}
	if schemaEvolutionSet {
		value.diagnostics = append(value.diagnostics, deprecatedFieldDiagnostic(
			"migration.schema_evolution",
			"migration.schema_contract",
			"6",
		))
	}
	if _, legacySet := migrationFields["ai_adjust"]; legacySet {
		value.diagnostics = append(value.diagnostics, deprecatedFieldDiagnostic(
			"migration.ai_adjust",
			"migration.runtime_tuning",
			"6",
		))
	}
	if _, legacySet := migrationFields["ai_adjust_interval"]; legacySet {
		value.diagnostics = append(value.diagnostics, deprecatedFieldDiagnostic(
			"migration.ai_adjust_interval",
			"migration.runtime_tuning_interval",
			"6",
		))
	}
	if !value.Migration.fieldWasSet("runtime_tuning") &&
		value.Migration.LegacyAIAdjust != nil {
		value.Migration.RuntimeTuning = *value.Migration.LegacyAIAdjust
		value.Migration.markFieldSet("runtime_tuning")
	}
	if !value.Migration.fieldWasSet("runtime_tuning_interval") &&
		value.Migration.LegacyAIAdjustInterval != nil {
		value.Migration.RuntimeTuningInterval =
			*value.Migration.LegacyAIAdjustInterval
		value.Migration.markFieldSet("runtime_tuning_interval")
	}
	if value.Migration.SchemaContract == nil &&
		value.Migration.SchemaEvolution != nil {
		legacy := *value.Migration.SchemaEvolution
		value.Migration.SchemaContract = &legacy
		value.Migration.markFieldSet("schema_contract")
	}
	value.Migration.SchemaEvolution = nil
	value.Migration.LegacyAIAdjust = nil
	value.Migration.LegacyAIAdjustInterval = nil
	if value.Source.Type == "" {
		value.Source.Type = "mssql"
	}
	if value.Target.Type == "" {
		value.Target.Type = "postgres"
	}
	value.Source.Type, err = CanonicalEngine(value.Source.Type)
	if err != nil {
		return Config{}, fmt.Errorf("source.type: %w", err)
	}
	value.Target.Type, err = CanonicalEngine(value.Target.Type)
	if err != nil {
		return Config{}, fmt.Errorf("target.type: %w", err)
	}
	if value.Source.Type == "sqlite" && value.Source.Database != "" {
		value.Source.Database, err = CanonicalSQLitePath(value.Source.Database)
		if err != nil {
			return Config{}, fmt.Errorf("source.database: %w", err)
		}
	}
	if value.Target.Type == "sqlite" && value.Target.Database != "" {
		value.Target.Database, err = CanonicalSQLitePath(value.Target.Database)
		if err != nil {
			return Config{}, fmt.Errorf("target.database: %w", err)
		}
	}
	if !value.Migration.fieldWasSet("target_mode") {
		value.Migration.TargetMode = "drop_recreate"
	}
	if value.Migration.TargetMode != "drop_recreate" && value.Migration.TargetMode != "upsert" {
		return Config{}, fmt.Errorf("invalid target_mode %q", value.Migration.TargetMode)
	}
	applyTransferDefaults(&value.Migration)
	applyProductionSemanticsDefaults(&value.Migration)
	if err := validateTransferSettings(value.Migration); err != nil {
		return Config{}, err
	}
	if err := validateProductionSemantics(value.Migration); err != nil {
		return Config{}, err
	}
	if err := validatePatterns("include_tables", value.Migration.IncludeTables); err != nil {
		return Config{}, err
	}
	if err := validatePatterns("exclude_tables", value.Migration.ExcludeTables); err != nil {
		return Config{}, err
	}
	value.Migration.captureParsedBaseline()
	return value, nil
}

const (
	DefaultConnectionLimit     = 4
	DefaultWorkers             = 4
	DefaultChunkSize           = 500
	DefaultPartitions          = 1
	DefaultLargeTableThreshold = int64(100_000)
	DefaultReaderParallelism   = 2
	DefaultWriterParallelism   = 2
	DefaultReadAhead           = 2
	DefaultUpsertMergeSize     = 500
	DefaultMemoryCeilingBytes  = int64(64 << 20)
	DefaultCheckpointFrequency = 10
	DefaultMaxRetries          = 3
)

func applyTransferDefaults(migration *Migration) {
	if !migration.fieldWasSet("connection_limit") {
		migration.ConnectionLimit = DefaultConnectionLimit
	}
	if !migration.fieldWasSet("workers") {
		migration.Workers = DefaultWorkers
	}
	if !migration.fieldWasSet("chunk_size") {
		migration.ChunkSize = DefaultChunkSize
	}
	if !migration.fieldWasSet("partitions") {
		migration.Partitions = DefaultPartitions
	}
	if !migration.fieldWasSet("large_table_threshold") {
		migration.LargeTableThreshold = DefaultLargeTableThreshold
	}
	if !migration.fieldWasSet("reader_parallelism") {
		migration.ReaderParallelism = DefaultReaderParallelism
	}
	if !migration.fieldWasSet("writer_parallelism") {
		migration.WriterParallelism = DefaultWriterParallelism
	}
	if !migration.fieldWasSet("read_ahead") {
		migration.ReadAhead = DefaultReadAhead
	}
	if !migration.fieldWasSet("upsert_merge_size") {
		migration.UpsertMergeSize = DefaultUpsertMergeSize
	}
	if !migration.fieldWasSet("memory_ceiling_bytes") {
		migration.MemoryCeilingBytes = DefaultMemoryCeilingBytes
	}
	if !migration.fieldWasSet("checkpoint_frequency") {
		migration.CheckpointFrequency = DefaultCheckpointFrequency
	}
	if !migration.fieldWasSet("max_retries") {
		migration.MaxRetries = DefaultMaxRetries
	}
	if !migration.fieldWasSet("strict_consistency_scope") {
		migration.StrictConsistencyScope = StrictConsistencyTable
	}
	adaptDerivedConcurrency(migration)
}

func adaptDerivedConcurrency(migration *Migration) {
	limit := migration.ConnectionLimit
	if limit < 2 {
		return
	}

	workersRequested := migration.fieldWasSet("workers")
	readersRequested := migration.fieldWasSet("reader_parallelism")
	writersRequested := migration.fieldWasSet("writer_parallelism")
	if !workersRequested && migration.Workers > limit {
		migration.Workers = limit
	}

	concurrencyLimit := limit
	if workersRequested && migration.Workers < concurrencyLimit {
		concurrencyLimit = migration.Workers
	}
	if concurrencyLimit < 2 ||
		migration.ReaderParallelism+migration.WriterParallelism <= concurrencyLimit {
		return
	}

	switch {
	case readersRequested && writersRequested:
		return
	case readersRequested:
		available := concurrencyLimit - migration.ReaderParallelism
		if available >= 1 {
			migration.WriterParallelism = available
		}
	case writersRequested:
		available := concurrencyLimit - migration.WriterParallelism
		if available >= 1 {
			migration.ReaderParallelism = available
		}
	default:
		for migration.ReaderParallelism+migration.WriterParallelism >
			concurrencyLimit {
			if migration.ReaderParallelism >= migration.WriterParallelism &&
				migration.ReaderParallelism > 1 {
				migration.ReaderParallelism--
				continue
			}
			if migration.WriterParallelism > 1 {
				migration.WriterParallelism--
				continue
			}
			break
		}
	}

	if !workersRequested &&
		migration.Workers < migration.ReaderParallelism+migration.WriterParallelism {
		migration.Workers =
			migration.ReaderParallelism + migration.WriterParallelism
	}
}

func validateTransferSettings(migration Migration) error {
	positive := []struct {
		name  string
		value int64
	}{
		{"connection_limit", int64(migration.ConnectionLimit)},
		{"workers", int64(migration.Workers)},
		{"chunk_size", int64(migration.ChunkSize)},
		{"partitions", int64(migration.Partitions)},
		{"large_table_threshold", migration.LargeTableThreshold},
		{"reader_parallelism", int64(migration.ReaderParallelism)},
		{"writer_parallelism", int64(migration.WriterParallelism)},
		{"read_ahead", int64(migration.ReadAhead)},
		{"upsert_merge_size", int64(migration.UpsertMergeSize)},
		{"memory_ceiling_bytes", migration.MemoryCeilingBytes},
	}
	for _, setting := range positive {
		if setting.value <= 0 {
			return fmt.Errorf("migration.%s must be positive", setting.name)
		}
	}
	if migration.CheckpointFrequency < 0 {
		return fmt.Errorf("migration.checkpoint_frequency must not be negative")
	}
	if migration.MaxRetries < 0 {
		return fmt.Errorf("migration.max_retries must not be negative")
	}
	if migration.StrictConsistencyScope != StrictConsistencyTable &&
		migration.StrictConsistencyScope != StrictConsistencyMigration {
		return fmt.Errorf("invalid strict_consistency_scope %q", migration.StrictConsistencyScope)
	}
	if migration.ReaderParallelism+migration.WriterParallelism > migration.ConnectionLimit {
		return fmt.Errorf("migration reader_parallelism plus writer_parallelism exceeds connection_limit")
	}
	if migration.Workers < migration.ReaderParallelism+migration.WriterParallelism {
		return fmt.Errorf("migration workers must cover reader_parallelism plus writer_parallelism")
	}
	return nil
}

// CanonicalEngine normalizes the public engine aliases before they reach
// connection, state, lease, or capability code.
func CanonicalEngine(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "postgres", "postgresql", "pg":
		return "postgres", nil
	case "mssql", "sqlserver", "sql-server":
		return "mssql", nil
	case "mysql", "mariadb", "maria":
		return "mysql", nil
	case "sqlite", "sqlite3", "sqlitedb":
		return "sqlite", nil
	case "clickhouse", "ch":
		return "clickhouse", nil
	default:
		return "", fmt.Errorf("unsupported engine %q", value)
	}
}

// SelectTables applies path-style glob patterns in the source's existing,
// deterministic order. An empty include list selects every table; exclusions
// always take precedence over inclusions.
func SelectTables(names, include, exclude []string) ([]string, error) {
	if err := validatePatterns("include_tables", include); err != nil {
		return nil, err
	}
	if err := validatePatterns("exclude_tables", exclude); err != nil {
		return nil, err
	}

	selected := make([]string, 0, len(names))
	for _, name := range names {
		included, err := matchesAny(name, include)
		if err != nil {
			return nil, err
		}
		if len(include) > 0 && !included {
			continue
		}
		excluded, err := matchesAny(name, exclude)
		if err != nil {
			return nil, err
		}
		if !excluded {
			selected = append(selected, name)
		}
	}
	return selected, nil
}

func validatePatterns(field string, patterns []string) error {
	for _, pattern := range patterns {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid %s glob %q: %w", field, pattern, err)
		}
	}
	return nil
}

func matchesAny(name string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		matched, err := path.Match(pattern, name)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

var template = regexp.MustCompile(`^\$\{(env:|file:)?([^}]+)\}$`)

func ExpandSecret(value string) (string, error) {
	matches := template.FindStringSubmatch(value)
	if matches == nil {
		return value, nil
	}
	switch matches[1] {
	case "file:":
		content, err := os.ReadFile(matches[2])
		if err != nil {
			return "", fmt.Errorf("read secret file: %w", err)
		}
		return strings.TrimSuffix(string(content), "\n"), nil
	default:
		return os.Getenv(matches[2]), nil
	}
}

func Sanitize(value Config) Config {
	value.Source.Password = redact(value.Source.Password)
	value.Target.Password = redact(value.Target.Password)
	return value
}
func redact(value string) string {
	if value == "" {
		return ""
	}
	return "[REDACTED]"
}
