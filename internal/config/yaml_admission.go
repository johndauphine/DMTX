package config

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

type migrationYAMLKind uint8

const (
	migrationYAMLString migrationYAMLKind = iota
	migrationYAMLNonBlank
	migrationYAMLInt
	migrationYAMLBool
	migrationYAMLDuration
	migrationYAMLMapping
	migrationYAMLStringList
	migrationYAMLSchemaContract
	migrationYAMLValidation
	migrationYAMLPreflight
	migrationYAMLDeletes
)

var migrationYAMLFields = map[string]migrationYAMLKind{
	"target_mode":              migrationYAMLNonBlank,
	"include_tables":           migrationYAMLStringList,
	"exclude_tables":           migrationYAMLStringList,
	"date_updated_columns":     migrationYAMLStringList,
	"connection_limit":         migrationYAMLInt,
	"workers":                  migrationYAMLInt,
	"chunk_size":               migrationYAMLInt,
	"partitions":               migrationYAMLInt,
	"large_table_threshold":    migrationYAMLInt,
	"reader_parallelism":       migrationYAMLInt,
	"writer_parallelism":       migrationYAMLInt,
	"read_ahead":               migrationYAMLInt,
	"upsert_merge_size":        migrationYAMLInt,
	"memory_ceiling_bytes":     migrationYAMLInt,
	"checkpoint_frequency":     migrationYAMLInt,
	"max_retries":              migrationYAMLInt,
	"strict_consistency":       migrationYAMLBool,
	"strict_consistency_scope": migrationYAMLNonBlank,
	"fail_on_schema_drift":     migrationYAMLBool,
	"schema_contract":          migrationYAMLSchemaContract,
	"schema_evolution":         migrationYAMLSchemaContract,
	"validation":               migrationYAMLValidation,
	"preflight":                migrationYAMLPreflight,
	"deletes":                  migrationYAMLDeletes,
	"history_retention_days":   migrationYAMLInt,
	"tuning":                   migrationYAMLNonBlank,
	"runtime_tuning":           migrationYAMLBool,
	"runtime_tuning_interval":  migrationYAMLDuration,
	"ai_adjust":                migrationYAMLBool,
	"ai_adjust_interval":       migrationYAMLDuration,
	"allow_partial":            migrationYAMLBool,
}

var validationYAMLFields = map[string]migrationYAMLKind{
	"mode":                      migrationYAMLNonBlank,
	"fail_on_mismatch":          migrationYAMLBool,
	"fail_on_timeout":           migrationYAMLBool,
	"fail_on_estimate_mismatch": migrationYAMLBool,
}

var preflightYAMLFields = map[string]migrationYAMLKind{
	"skip_checks": migrationYAMLStringList,
}

var deletesYAMLFields = map[string]migrationYAMLKind{
	"mode":            migrationYAMLNonBlank,
	"target_behavior": migrationYAMLNonBlank,
	"reconcile":       migrationYAMLMapping,
}

var deleteReconcileYAMLFields = map[string]migrationYAMLKind{
	"schedule":            migrationYAMLNonBlank,
	"interval":            migrationYAMLDuration,
	"batch_size":          migrationYAMLInt,
	"require_primary_key": migrationYAMLBool,
}

var endpointYAMLFields = map[string]struct{}{
	"type":        {},
	"host":        {},
	"port":        {},
	"database":    {},
	"user":        {},
	"password":    {},
	"schema":      {},
	"ssl_mode":    {},
	"tls_ca_file": {},
}

func inspectMigrationYAML(
	data []byte,
) (map[string]*yaml.Node, map[string]struct{}, error) {
	var document yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&document); err != nil {
		if err == io.EOF {
			return nil, nil, fmt.Errorf("configuration must be a mapping")
		}
		return nil, nil, err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf(
			"configuration must contain exactly one YAML document",
		)
	}
	fields := make(map[string]*yaml.Node)
	explicit := make(map[string]struct{})
	if len(document.Content) == 0 {
		return nil, nil, fmt.Errorf("configuration must be a mapping")
	}
	root := document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, nil, fmt.Errorf("configuration must be a mapping")
	}
	seen := make(map[string]struct{}, len(root.Content)/2)
	for index := 0; index < len(root.Content); index += 2 {
		keyNode := root.Content[index]
		if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
			return nil, nil, fmt.Errorf(
				"configuration section names must be strings",
			)
		}
		key := keyNode.Value
		if _, duplicate := seen[key]; duplicate {
			return nil, nil, fmt.Errorf(
				"configuration contains duplicate section %q",
				key,
			)
		}
		seen[key] = struct{}{}
		value := root.Content[index+1]
		switch key {
		case "source", "target":
			if err := inspectEndpointYAML(key, value); err != nil {
				return nil, nil, err
			}
		case "migration":
			if isNullYAML(value) {
				return nil, nil, fmt.Errorf(
					"migration must be a mapping, not null",
				)
			}
			if value.Kind != yaml.MappingNode {
				return nil, nil, fmt.Errorf("migration must be a mapping")
			}
			if err := inspectYAMLMapping(
				"migration",
				value,
				migrationYAMLFields,
				fields,
				explicit,
			); err != nil {
				return nil, nil, err
			}
		case "profile", "ai", "slack":
			if isNullYAML(value) {
				return nil, nil, fmt.Errorf(
					"%s must be a mapping, not null",
					key,
				)
			}
			if value.Kind != yaml.MappingNode {
				return nil, nil, fmt.Errorf("%s must be a mapping", key)
			}
		default:
			return nil, nil, fmt.Errorf(
				"configuration section %q is unsupported",
				key,
			)
		}
	}
	return fields, explicit, nil
}

func inspectEndpointYAML(name string, node *yaml.Node) error {
	if isNullYAML(node) {
		return fmt.Errorf("%s must be a mapping, not null", name)
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", name)
	}
	seen := make(map[string]struct{}, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		keyNode := node.Content[index]
		if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
			return fmt.Errorf("%s field names must be strings", name)
		}
		key := keyNode.Value
		value := node.Content[index+1]
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%s contains duplicate field %q", name, key)
		}
		seen[key] = struct{}{}
		if _, supported := endpointYAMLFields[key]; !supported {
			return fmt.Errorf("%s.%s is unsupported", name, key)
		}
		if isNullYAML(value) {
			return fmt.Errorf("%s.%s must not be null", name, key)
		}
		if value.Kind != yaml.ScalarNode {
			return fmt.Errorf("%s.%s must be a scalar", name, key)
		}
		tag := "!!str"
		if key == "port" {
			tag = "!!int"
		}
		if err := requireYAMLScalarTag(
			name+"."+key,
			value,
			tag,
		); err != nil {
			return err
		}
		if key == "type" && strings.TrimSpace(value.Value) == "" {
			return fmt.Errorf("%s.type must not be blank", name)
		}
	}
	return nil
}

func inspectYAMLMapping(
	prefix string,
	node *yaml.Node,
	supported map[string]migrationYAMLKind,
	topLevel map[string]*yaml.Node,
	explicit map[string]struct{},
) error {
	seen := make(map[string]struct{}, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		keyNode := node.Content[index]
		if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
			return fmt.Errorf("%s field names must be strings", prefix)
		}
		key := keyNode.Value
		value := node.Content[index+1]
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%s contains duplicate field %q", prefix, key)
		}
		seen[key] = struct{}{}
		kind, ok := supported[key]
		if !ok {
			return fmt.Errorf("%s.%s is unsupported", prefix, key)
		}
		field := prefix + "." + key
		path := strings.TrimPrefix(field, "migration.")
		explicit[path] = struct{}{}
		if prefix == "migration" {
			topLevel[key] = value
		}
		if err := inspectYAMLValue(field, value, kind, topLevel, explicit); err != nil {
			return err
		}
	}
	return nil
}

func inspectYAMLValue(
	field string,
	node *yaml.Node,
	kind migrationYAMLKind,
	topLevel map[string]*yaml.Node,
	explicit map[string]struct{},
) error {
	if isNullYAML(node) {
		if kind == migrationYAMLSchemaContract {
			return fmt.Errorf("%s must be a mode or mapping", field)
		}
		return fmt.Errorf("%s must not be null", field)
	}
	if node.Kind == yaml.ScalarNode && strings.TrimSpace(node.Value) == "" {
		return fmt.Errorf("%s must not be blank", field)
	}
	switch kind {
	case migrationYAMLString:
		return requireYAMLScalarTag(field, node, "!!str")
	case migrationYAMLNonBlank:
		if err := requireYAMLScalarTag(field, node, "!!str"); err != nil {
			return err
		}
		if strings.TrimSpace(node.Value) == "" {
			return fmt.Errorf("%s must not be blank", field)
		}
		return nil
	case migrationYAMLInt:
		return requireYAMLScalarTag(field, node, "!!int")
	case migrationYAMLBool:
		return requireYAMLScalarTag(field, node, "!!bool")
	case migrationYAMLDuration:
		if err := requireYAMLScalarTag(field, node, "!!str"); err != nil {
			return err
		}
		if strings.TrimSpace(node.Value) == "" {
			return fmt.Errorf("%s must not be blank", field)
		}
		return nil
	case migrationYAMLMapping:
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("%s must be a mapping", field)
		}
		return nil
	case migrationYAMLStringList:
		if node.Kind != yaml.SequenceNode {
			return fmt.Errorf("%s must be a list", field)
		}
		for index, item := range node.Content {
			if isNullYAML(item) ||
				item.Kind == yaml.ScalarNode &&
					strings.TrimSpace(item.Value) == "" {
				if field == "migration.date_updated_columns" {
					return fmt.Errorf("%s[%d] must not be empty", field, index)
				}
				return fmt.Errorf("%s[%d] must not be blank or null", field, index)
			}
			if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
				return fmt.Errorf("%s[%d] must be a string", field, index)
			}
		}
		return nil
	case migrationYAMLSchemaContract:
		return inspectSchemaContractYAML(field, node)
	case migrationYAMLValidation:
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("%s must be a mapping", field)
		}
		return inspectYAMLMapping(
			field,
			node,
			validationYAMLFields,
			topLevel,
			explicit,
		)
	case migrationYAMLPreflight:
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("%s must be a mapping", field)
		}
		return inspectYAMLMapping(
			field,
			node,
			preflightYAMLFields,
			topLevel,
			explicit,
		)
	case migrationYAMLDeletes:
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("%s must be a mapping", field)
		}
		if err := inspectYAMLMapping(
			field,
			node,
			deletesYAMLFields,
			topLevel,
			explicit,
		); err != nil {
			return err
		}
		for index := 0; index < len(node.Content); index += 2 {
			if node.Content[index].Value != "reconcile" {
				continue
			}
			reconcile := node.Content[index+1]
			if isNullYAML(reconcile) {
				return fmt.Errorf("%s.reconcile must not be null", field)
			}
			if reconcile.Kind != yaml.MappingNode {
				return fmt.Errorf("%s.reconcile must be a mapping", field)
			}
			return inspectYAMLMapping(
				field+".reconcile",
				reconcile,
				deleteReconcileYAMLFields,
				topLevel,
				explicit,
			)
		}
		return nil
	default:
		return fmt.Errorf("%s has unsupported YAML admission kind", field)
	}
}

func inspectSchemaContractYAML(field string, node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag != "!!str" || strings.TrimSpace(node.Value) == "" {
			return fmt.Errorf("%s must not be blank", field)
		}
		return nil
	case yaml.MappingNode:
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			entityNode := node.Content[index]
			if entityNode.Kind != yaml.ScalarNode ||
				entityNode.Tag != "!!str" {
				return fmt.Errorf(
					"%s entity names must be strings",
					field,
				)
			}
			entity := entityNode.Value
			value := node.Content[index+1]
			if _, duplicate := seen[entity]; duplicate {
				return fmt.Errorf(
					"%s contains duplicate entity %q",
					field,
					entity,
				)
			}
			seen[entity] = struct{}{}
			switch entity {
			case "tables", "columns", "data_type":
			default:
				return fmt.Errorf("unknown schema contract entity %q", entity)
			}
			if isNullYAML(value) ||
				value.Kind != yaml.ScalarNode ||
				value.Tag != "!!str" ||
				strings.TrimSpace(value.Value) == "" {
				return fmt.Errorf("%s.%s must not be blank or null", field, entity)
			}
		}
		return nil
	default:
		return fmt.Errorf("%s must be a mode or mapping", field)
	}
}

func isNullYAML(node *yaml.Node) bool {
	return node == nil || node.Tag == "!!null"
}

func requireYAMLScalarTag(
	field string,
	node *yaml.Node,
	tag string,
) error {
	if node.Kind != yaml.ScalarNode || node.Tag != tag {
		expected := map[string]string{
			"!!str":  "string",
			"!!int":  "integer",
			"!!bool": "boolean",
		}[tag]
		if expected == "" {
			expected = strings.TrimPrefix(tag, "!!")
		}
		return fmt.Errorf("%s must be a %s", field, expected)
	}
	return nil
}
