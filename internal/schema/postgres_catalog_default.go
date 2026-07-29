package schema

import "fmt"

// ParsePostgresCatalogDefault converts a supported PostgreSQL pg_get_expr
// result into DMTX's structured expression contract. The catalog SQL is parsed
// through the canonical signature grammar and is never retained in the
// returned expression.
func ParsePostgresCatalogDefault(
	column Column,
	pgGetExpr *string,
) (*Expression, error) {
	signature, err := CatalogPostgresDefaultSignature(column, pgGetExpr)
	if err != nil {
		return nil, err
	}
	if !signature.Present {
		return nil, nil
	}

	expression := &Expression{}
	switch signature.Kind {
	case PostgresDefaultBoolean:
		expression.kind = expressionBoolean
		expression.literal = signature.Value
		expression.sql = signature.Value
	case PostgresDefaultInteger,
		PostgresDefaultBigint,
		PostgresDefaultDoublePrecision,
		PostgresDefaultNumeric:
		expression.kind = expressionNumber
		expression.literal = signature.Value
		expression.sql = signature.Value
	case PostgresDefaultText,
		PostgresDefaultChar,
		PostgresDefaultVarchar:
		expression.kind = expressionString
		expression.literal = signature.Value
		expression.sql = postgresStringLiteral(signature.Value)
	case PostgresDefaultBytea:
		expression.kind = expressionBlob
		expression.literal = signature.Value
		expression.sql = "X'" + signature.Value + "'"
	case PostgresDefaultCurrentTime:
		expression.kind = expressionCurrentTime
		expression.sql = "CURRENT_TIME"
	case PostgresDefaultCurrentDate:
		expression.kind = expressionCurrentDate
		expression.sql = "CURRENT_DATE"
	case PostgresDefaultCurrentTimestamp:
		expression.kind = expressionCurrentTimestamp
		expression.sql = "CURRENT_TIMESTAMP"
	default:
		return nil, postgresDefaultSignaturePolicy(
			"construct PostgreSQL catalog default",
			column,
		)
	}

	roundTrip := column
	roundTrip.Default = expression
	planned, err := PlannedPostgresDefaultSignature(roundTrip)
	if err != nil {
		return nil, err
	}
	if planned != signature {
		return nil, fmt.Errorf(
			"schema policy: PostgreSQL catalog default for column %q does not round-trip",
			column.Name,
		)
	}
	return expression, nil
}
