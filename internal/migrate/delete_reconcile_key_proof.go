package migrate

import (
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

// Delete key identity: fingerprinting the key columns and proving both sides
// agree on the equality that a reconciliation will delete by.

type deleteTableKeyFingerprint struct {
	Schema         string          `json:"schema"`
	Table          string          `json:"table"`
	MySQLCollation string          `json:"mysql_collation"`
	Columns        []schema.Column `json:"columns"`
}

func deleteKeyMetadataFingerprint(
	table schema.Table,
	primaryKey []schema.Column,
) (string, error) {
	payload, err := json.Marshal(deleteTableKeyFingerprint{
		Schema: table.Schema, Table: table.Name,
		MySQLCollation: strings.TrimSpace(table.MySQLCollation),
		Columns:        primaryKey,
	})
	if err != nil {
		return "", fmt.Errorf(
			"encode delete key metadata: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateDeleteKeyEqualityProof(
	proof deleteKeyEqualityProof,
	sourceTable schema.Table,
	targetTable schema.Table,
	sourcePrimaryKey []schema.Column,
	targetPrimaryKey []schema.Column,
) (string, error) {
	if strings.TrimSpace(proof.CanonicalizerID) == "" {
		return "", fmt.Errorf(
			"delete key equality proof has no canonicalizer ID",
		)
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(
		sourceTable,
		sourcePrimaryKey,
	)
	if err != nil {
		return "", err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(
		targetTable,
		targetPrimaryKey,
	)
	if err != nil {
		return "", err
	}
	if proof.SourceFingerprint != sourceFingerprint ||
		proof.TargetFingerprint != targetFingerprint {
		return "", fmt.Errorf(
			"delete key equality proof does not bind the selected source and target primary keys",
		)
	}
	if len(proof.Columns) != len(sourcePrimaryKey) {
		return "", fmt.Errorf(
			"delete key equality proof column width differs",
		)
	}
	for index, columnProof := range proof.Columns {
		sourceKind, err := validationKindForColumn(
			sourcePrimaryKey[index],
		)
		if err != nil {
			return "", err
		}
		targetKind, err := validationKindForColumn(
			targetPrimaryKey[index],
		)
		if err != nil {
			return "", err
		}
		if err := validateDeleteColumnSemantics(
			columnProof,
			sourceKind,
			targetKind,
		); err != nil {
			return "", fmt.Errorf(
				"delete key equality proof column %s: %w",
				sourcePrimaryKey[index].Name,
				err,
			)
		}
	}
	payload, err := json.Marshal(proof)
	if err != nil {
		return "", fmt.Errorf(
			"encode delete key equality proof: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateDeleteColumnSemantics(
	proof deleteKeyColumnProof,
	sourceKind validationValueKind,
	targetKind validationValueKind,
) error {
	sourceTextual := sourceKind == validationText ||
		sourceKind == validationUUID
	targetTextual := targetKind == validationText ||
		targetKind == validationUUID
	textual := sourceTextual || targetTextual
	if sourceKind == validationDynamic ||
		targetKind == validationDynamic {
		return fmt.Errorf("dynamic key equality is unsupported")
	}
	switch proof.Semantics {
	case "integer":
		if sourceKind != validationInteger ||
			targetKind != validationInteger {
			return fmt.Errorf("integer proof does not match metadata")
		}
	case "boolean":
		if (sourceKind != validationBoolean &&
			sourceKind != validationInteger) ||
			(targetKind != validationBoolean &&
				targetKind != validationInteger) {
			return fmt.Errorf("boolean proof does not match metadata")
		}
	case "decimal":
		if (sourceKind != validationDecimal &&
			sourceKind != validationInteger) ||
			(targetKind != validationDecimal &&
				targetKind != validationInteger) {
			return fmt.Errorf("decimal proof does not match metadata")
		}
	case "float_exact":
		if sourceKind != validationFloat ||
			targetKind != validationFloat {
			return fmt.Errorf("float proof does not match metadata")
		}
	case "binary":
		if sourceKind != validationBytes ||
			targetKind != validationBytes {
			return fmt.Errorf("binary proof does not match metadata")
		}
	case "date":
		if sourceKind != validationDate ||
			targetKind != validationDate {
			return fmt.Errorf("date proof does not match metadata")
		}
	case "time":
		if sourceKind != validationTime ||
			targetKind != validationTime {
			return fmt.Errorf("time proof does not match metadata")
		}
	case "timestamp":
		if sourceKind != validationTimestamp ||
			targetKind != validationTimestamp {
			return fmt.Errorf("timestamp proof does not match metadata")
		}
	case "binary_text", "uuid_binary_text":
		if !sourceTextual || !targetTextual {
			return fmt.Errorf("text proof does not match metadata")
		}
		if strings.TrimSpace(proof.CollationEvidence) == "" {
			return fmt.Errorf(
				"text/UUID equality requires explicit binary-collation evidence",
			)
		}
	default:
		return fmt.Errorf(
			"unsupported key equality semantics %q",
			proof.Semantics,
		)
	}
	if textual && proof.Semantics != "binary_text" &&
		proof.Semantics != "uuid_binary_text" {
		return fmt.Errorf(
			"text/UUID key equality lacks binary semantics",
		)
	}
	return nil
}

const deleteKeyEncodingVersion = "dmtx-delete-key-v2"

func canonicalDeleteKey(
	canonicalizer deleteKeyCanonicalizer,
	side deleteKeySide,
	proof deleteKeyEqualityProof,
	values []any,
) ([]byte, []driver.Value, error) {
	if len(values) != len(proof.Columns) {
		return nil, nil, fmt.Errorf(
			"delete key has %d values for %d proof columns",
			len(values),
			len(proof.Columns),
		)
	}
	encoded := make([]byte, 0, len(values)*48)
	encoded = appendFrame(
		encoded,
		"version",
		[]byte(deleteKeyEncodingVersion),
	)
	encoded = appendFrame(
		encoded,
		"columns",
		[]byte(strconv.Itoa(len(values))),
	)
	var parameters []driver.Value
	if side == deleteKeyTargetSide {
		parameters = make([]driver.Value, len(values))
	}
	for index, value := range values {
		if value == nil {
			return nil, nil, fmt.Errorf(
				"delete key column %d is NULL",
				index+1,
			)
		}
		canonical, err := canonicalizer.
			CanonicalizeDeleteKeyValue(side, proof, index, value)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"canonicalize delete key column %d: %w",
				index+1,
				err,
			)
		}
		encoded = appendFrame(
			encoded,
			proof.Columns[index].Semantics,
			canonical.Canonical,
		)
		if side == deleteKeyTargetSide {
			parameter, err := stableDeleteParameter(
				canonical.Parameter,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"delete key column %d: %w",
					index+1,
					err,
				)
			}
			parameters[index] = parameter
		}
	}
	return encoded, parameters, nil
}

func stableDeleteParameter(value driver.Value) (driver.Value, error) {
	if value == nil || !driver.IsValue(value) {
		return nil, fmt.Errorf(
			"canonicalizer returned a non-parameter-safe value",
		)
	}
	if bytesValue, ok := value.([]byte); ok {
		return append([]byte(nil), bytesValue...), nil
	}
	return value, nil
}

type deleteKeySpool struct {
	path string
	db   *sql.DB
}
