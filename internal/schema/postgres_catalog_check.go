package schema

// ParsePostgresCatalogCheck converts a supported PostgreSQL pg_get_expr CHECK
// body into DMTX's structured portable expression. It reconstructs canonical
// SQL from the parsed AST and never retains executable catalog text.
func ParsePostgresCatalogCheck(
	pgGetExpr string,
	columns []Column,
) (Expression, error) {
	root, err := parsePostgresCatalogCheckRoot(pgGetExpr, columns)
	if err != nil {
		return Expression{}, err
	}
	canonical := renderCanonicalPortableCheck(
		root,
		portableCheckPrecedenceLowest,
	)
	expression := Expression{
		sql:  canonical,
		kind: expressionCheck,
	}
	planned, err := parsePlannedPostgresCheck(expression, columns)
	if err != nil {
		return Expression{}, err
	}
	if makePostgresCheckSignature(planned) !=
		makePostgresCheckSignature(root) {
		return Expression{}, postgresCheckSignaturePolicy(
			"reconstructed CHECK does not round-trip through the planner",
		)
	}
	return expression, nil
}
