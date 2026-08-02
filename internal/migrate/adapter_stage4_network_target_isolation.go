package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// adapterStage4NetworkReplayIsolationTarget is the target-owned, read-only
// proof that replaying an idempotent Stage 4 upsert page cannot mutate another
// page or any target-only row through an incoming foreign key.
//
// This contract is intentionally separate from ordinary retained-table
// preflight. Network admission repeats the proof immediately before it creates
// durable range state so a target cannot gain a replay capability merely by
// implementing WriteBatch.
type adapterStage4NetworkReplayIsolationTarget interface {
	targetAdapter
	PreflightStage4NetworkReplayIsolation(
		context.Context,
		[]schema.Table,
	) error
}

// preflightStage4NetworkReplayIsolation is the integration point for the
// Stage 4 network admission path. It fails closed when a target has no
// engine-specific proof.
func preflightStage4NetworkReplayIsolation(
	ctx context.Context,
	target targetAdapter,
	tables []schema.Table,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network replay-isolation context is required",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	preflighter, ok := target.(adapterStage4NetworkReplayIsolationTarget)
	if !ok || isNilInterface(preflighter) {
		engine := ""
		if !isNilInterface(target) {
			engine = target.Engine()
		}
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q has no certified network replay-isolation preflight",
				engine,
			),
		)
	}
	if err := preflighter.PreflightStage4NetworkReplayIsolation(
		ctx,
		append([]schema.Table(nil), tables...),
	); err != nil {
		return fmt.Errorf(
			"preflight Stage 4 network replay isolation for target %s: %w",
			preflighter.Engine(),
			err,
		)
	}
	return nil
}

type stage4NetworkReplayTableProfile struct {
	namespace      string
	name           string
	columns        map[string]string
	primaryColumns map[string]struct{}
}

type stage4NetworkIncomingForeignKey struct {
	parentNamespace     string
	parentTable         string
	name                string
	referencedNamespace string
	referencedTable     string
	referencedColumn    string
	updateAction        string
	implicitPrimaryKey  bool
}

type stage4NetworkIdentifierNormalizer func(string) string

func stage4NetworkReplayTableProfiles(
	engine string,
	tables []schema.Table,
	normalize stage4NetworkIdentifierNormalizer,
) (map[string]stage4NetworkReplayTableProfile, error) {
	if normalize == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 %s network identifier normalizer is required",
				engine,
			),
		)
	}
	profiles := make(
		map[string]stage4NetworkReplayTableProfile,
		len(tables),
	)
	for _, table := range tables {
		namespace := normalize(table.Schema)
		name := normalize(table.Name)
		if strings.TrimSpace(table.Name) == "" {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 %s network target table name is empty",
					engine,
				),
			)
		}
		key := adapterSourceTableKey(namespace, name)
		if earlier, duplicate := profiles[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 %s network target tables %q and %q have the same catalog identity",
					engine,
					earlier.name,
					table.Name,
				),
			)
		}
		profile := stage4NetworkReplayTableProfile{
			namespace: namespace,
			name:      table.Name,
			columns: make(
				map[string]string,
				len(table.Columns),
			),
			primaryColumns: make(
				map[string]struct{},
				len(table.Columns),
			),
		}
		for _, column := range table.Columns {
			normalizedColumn := normalize(column.Name)
			if strings.TrimSpace(column.Name) == "" {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 %s network target table %s has an empty column name",
						engine,
						table.Name,
					),
				)
			}
			if earlier, duplicate := profile.columns[normalizedColumn]; duplicate {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 %s network target table %s columns %q and %q have the same catalog identity",
						engine,
						table.Name,
						earlier,
						column.Name,
					),
				)
			}
			profile.columns[normalizedColumn] = column.Name
			if column.PrimaryKey {
				profile.primaryColumns[normalizedColumn] = struct{}{}
			}
		}
		profiles[key] = profile
	}
	return profiles, nil
}

func validateStage4NetworkIncomingForeignKey(
	engine string,
	referenced stage4NetworkReplayTableProfile,
	dependency stage4NetworkIncomingForeignKey,
	normalize stage4NetworkIdentifierNormalizer,
) error {
	if strings.TrimSpace(dependency.parentNamespace) == "" &&
		engine != "SQLite" {
		return stage4NetworkReplayCatalogShapeError(
			engine,
			referenced,
			"incoming foreign-key parent namespace is empty",
		)
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "parent table", value: dependency.parentTable},
		{label: "constraint name", value: dependency.name},
		{
			label: "referenced table",
			value: dependency.referencedTable,
		},
		{label: "update action", value: dependency.updateAction},
	} {
		if strings.TrimSpace(field.value) == "" {
			return stage4NetworkReplayCatalogShapeError(
				engine,
				referenced,
				"incoming foreign-key "+field.label+" is empty",
			)
		}
	}
	referencedKey := adapterSourceTableKey(
		normalize(dependency.referencedNamespace),
		normalize(dependency.referencedTable),
	)
	expectedReferencedKey := adapterSourceTableKey(
		referenced.namespace,
		normalize(referenced.name),
	)
	if referencedKey != expectedReferencedKey {
		return stage4NetworkReplayCatalogShapeError(
			engine,
			referenced,
			fmt.Sprintf(
				"incoming foreign key %s returned referenced identity %s.%s",
				dependency.name,
				dependency.referencedNamespace,
				dependency.referencedTable,
			),
		)
	}
	action, mutatesExternalRows, err := stage4NetworkUpdateAction(
		dependency.updateAction,
	)
	if err != nil {
		return stage4NetworkReplayCatalogShapeError(
			engine,
			referenced,
			fmt.Sprintf(
				"incoming foreign key %s: %v",
				dependency.name,
				err,
			),
		)
	}
	referencedColumnIsPrimary := false
	if dependency.implicitPrimaryKey {
		if len(referenced.primaryColumns) == 0 {
			return stage4NetworkReplayCatalogShapeError(
				engine,
				referenced,
				fmt.Sprintf(
					"incoming foreign key %s uses an implicit primary-key reference but the selected table has no primary key",
					dependency.name,
				),
			)
		}
		referencedColumnIsPrimary = true
	} else {
		if strings.TrimSpace(dependency.referencedColumn) == "" {
			return stage4NetworkReplayCatalogShapeError(
				engine,
				referenced,
				fmt.Sprintf(
					"incoming foreign key %s has an empty referenced column",
					dependency.name,
				),
			)
		}
		columnKey := normalize(dependency.referencedColumn)
		canonicalColumn, exists := referenced.columns[columnKey]
		if !exists {
			return stage4NetworkReplayCatalogShapeError(
				engine,
				referenced,
				fmt.Sprintf(
					"incoming foreign key %s references unknown column %s",
					dependency.name,
					dependency.referencedColumn,
				),
			)
		}
		_, referencedColumnIsPrimary =
			referenced.primaryColumns[columnKey]
		dependency.referencedColumn = canonicalColumn
	}

	if !mutatesExternalRows {
		return nil
	}
	if referencedColumnIsPrimary {
		// Network upsert writers never update primary-key columns. A
		// referential action on an immutable key therefore cannot fire.
		return nil
	}
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"Stage 4 %s network replay for table %s.%s is not isolated: dependent table %s.%s retains foreign key %s with ON UPDATE %s on mutable column %s",
			engine,
			dependency.referencedNamespace,
			dependency.referencedTable,
			dependency.parentNamespace,
			dependency.parentTable,
			dependency.name,
			action,
			dependency.referencedColumn,
		),
	)
}

func stage4NetworkUpdateAction(
	value string,
) (canonical string, mutatesExternalRows bool, err error) {
	canonical = strings.ToUpper(
		strings.Join(strings.Fields(value), " "),
	)
	switch canonical {
	case "NO ACTION", "RESTRICT":
		return canonical, false, nil
	case "CASCADE", "SET NULL", "SET DEFAULT":
		return canonical, true, nil
	default:
		return "", false, fmt.Errorf(
			"unexpected ON UPDATE action %q",
			value,
		)
	}
}

func stage4NetworkReplayCatalogShapeError(
	engine string,
	table stage4NetworkReplayTableProfile,
	detail string,
) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"Stage 4 %s network replay catalog shape for table %s.%s is unsupported: %s",
			engine,
			table.namespace,
			table.name,
			detail,
		),
	)
}

func stage4ExactIdentifier(value string) string {
	return value
}

// SQLite applies ASCII case-insensitive identifier resolution while leaving
// non-ASCII identifier bytes distinct unless a custom collation is involved.
// Catalog identity checks must mirror that rule instead of Unicode folding.
func stage4SQLiteIdentifier(value string) string {
	bytes := []byte(value)
	for index, character := range bytes {
		if character >= 'A' && character <= 'Z' {
			bytes[index] = character + ('a' - 'A')
		}
	}
	return string(bytes)
}
