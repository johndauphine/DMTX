package schema

import "testing"

func TestRenderPortableCheckNumericSubtypeAndIntegerRanges(t *testing.T) {
	columns := []Column{{Name: "i", Type: "integer"}, {Name: "b", Type: "bigint"}, {Name: "n", Type: "numeric"}}
	accepted := []string{
		`i >= -2147483648 AND i <= 2147483647`,
		`b >= -9223372036854775808 AND b <= 9223372036854775807`,
		`i IN (-2147483648, 2147483647)`,
	}
	for _, source := range accepted {
		expression, err := ParseSQLiteCheckExpression(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := RenderSQLiteCheckForPostgres(expression, columns); err != nil {
			t.Fatalf("safe CHECK %q was rejected: %v", source, err)
		}
	}
	rejected := []string{
		`i = n`, `i < 2147483648`, `i >= -2147483649`,
		`i IN (0, 2147483648)`, `b < 9223372036854775808`,
		`b >= -9223372036854775809`,
	}
	for _, source := range rejected {
		expression, err := ParseSQLiteCheckExpression(source)
		if err != nil {
			t.Fatal(err)
		}
		if rendered, err := RenderSQLiteCheckForPostgres(expression, columns); err == nil {
			t.Fatalf("unsafe CHECK %q rendered as %q", source, rendered)
		}
	}
}

func TestPostgresCatalogCheckNumericCastsFailClosed(t *testing.T) {
	columns := []Column{
		{Name: "i", Type: "integer"},
		{Name: "b", Type: "bigint"},
		{Name: "n", Type: "numeric"},
		{Name: "d", Type: "double precision"},
	}
	accepted := []string{
		`i >= 0`, `i >= '-2147483648'::integer`,
		`i <= 2147483647::integer`,
		`b >= '-9223372036854775808'::bigint`,
		`b <= '9223372036854775807'::bigint`,
		`n >= 0::numeric`,
		`i = ANY (ARRAY[1::integer, 2::integer])`,
		`d = 16777217::double precision`,
		`d = ANY (ARRAY[16777217::double precision, 16777218::double precision])`,
	}
	for _, catalog := range accepted {
		if _, err := ParsePostgresCatalogCheck(catalog, columns); err != nil {
			t.Fatalf("safe catalog CHECK %q was rejected: %v", catalog, err)
		}
	}
	rejected := []string{
		`i >= 0::numeric`, `n >= 0::integer`,
		`i < 2147483648::integer`, `i >= '-2147483649'::integer`,
		`b < '9223372036854775808'::bigint`,
		`b >= '-9223372036854775809'::bigint`,
		`i = 16777217::real`,
		`b = '9007199254740993'::double precision`,
		`i = ANY (ARRAY[1::integer, 2.0::numeric])`,
		`i = ANY (ARRAY[16777217::real, 16777218])`,
		`i = ANY (ARRAY[1::integer, NULL::numeric])`,
		`d = 16777217::real`,
		`d = ANY (ARRAY[16777217::real, 16777218::real])`,
	}
	for _, catalog := range rejected {
		if expression, err := ParsePostgresCatalogCheck(catalog, columns); err == nil {
			t.Fatalf("unsafe catalog CHECK %q reconstructed as %q", catalog, expression.CanonicalSQL())
		}
		if signature, err := ParsePostgresCheckSignature(catalog, columns); err == nil {
			t.Fatalf("unsafe catalog CHECK %q retained signature %q", catalog, signature)
		}
	}
}

func TestPortableFloatingAliasesUseDoublePrecisionSemantics(t *testing.T) {
	for _, columnType := range []string{"real", "float4", "float8"} {
		t.Run(columnType, func(t *testing.T) {
			columns := []Column{{Name: "x", Type: columnType}}
			expression, err := ParseSQLiteCheckExpression(`x = 16777217`)
			if err != nil {
				t.Fatal(err)
			}
			rendered, err := RenderSQLiteCheckForPostgres(
				expression,
				columns,
			)
			if err != nil {
				t.Fatalf("render %s CHECK: %v", columnType, err)
			}
			if rendered != `"x" = 16777217` {
				t.Fatalf("rendered %s CHECK = %q", columnType, rendered)
			}
			planned, err := PlannedPostgresCheckSignature(
				expression,
				columns,
			)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := ParsePostgresCheckSignature(
				`x = 16777217::double precision`,
				columns,
			)
			if err != nil {
				t.Fatalf("parse %s double CHECK: %v", columnType, err)
			}
			if actual != planned {
				t.Fatalf("%s signature mismatch: %q != %q", columnType, actual, planned)
			}
			if _, err := ParsePostgresCheckSignature(
				`x = 16777217::real`,
				columns,
			); err == nil {
				t.Fatalf("%s accepted lossy catalog real cast", columnType)
			}
		})
	}
}

func TestPortableInt4AliasIsNumeric(t *testing.T) {
	columns := []Column{{Name: "i", Type: "int4"}}
	expression, err := ParseSQLiteCheckExpression(`i >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	if rendered, err := RenderSQLiteCheckForPostgres(
		expression, columns,
	); err != nil || rendered != `"i" >= 0` {
		t.Fatalf("int4 CHECK = %q, error = %v", rendered, err)
	}
}
