package engine

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestValidateSQLServer2022SourceCatalog(t *testing.T) {
	base := sqlServer2022SourceCatalog{
		productMajorVersion: 16,
		engineEdition:       3,
		productVersion:      "16.0.4250.1",
		edition:             "Developer Edition (64-bit)",
		databaseName:        "dmtx_source",
		compatibilityLevel:  160,
		state:               "ONLINE",
		userAccess:          "MULTI_USER",
		containment:         "NONE",
	}
	if err := validateSQLServer2022SourceCatalog(base); err != nil {
		t.Fatalf("valid SQL Server 2022 catalog: %v", err)
	}
	tests := map[string]func(*sqlServer2022SourceCatalog){
		"future major": func(value *sqlServer2022SourceCatalog) {
			value.productMajorVersion = 17
			value.productVersion = "17.0.1000.1"
		},
		"Azure edition": func(value *sqlServer2022SourceCatalog) {
			value.engineEdition = 5
		},
		"old compatibility": func(value *sqlServer2022SourceCatalog) {
			value.compatibilityLevel = 150
		},
		"read only": func(value *sqlServer2022SourceCatalog) {
			value.readOnly = true
		},
		"snapshot clone": func(value *sqlServer2022SourceCatalog) {
			value.sourceDatabaseID.Valid = true
			value.sourceDatabaseID.Int64 = 7
		},
		"replication": func(value *sqlServer2022SourceCatalog) {
			value.published = true
		},
		"change data capture": func(value *sqlServer2022SourceCatalog) {
			value.changeDataCapture = true
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			if err := validateSQLServer2022SourceCatalog(value); !errors.As(
				err,
				&policy,
			) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestValidateSQLServer2022MigrationSnapshotCatalog(t *testing.T) {
	base := sqlServer2022SourceCatalog{
		productMajorVersion: 16,
		engineEdition:       3,
		productVersion:      "16.0.4250.1",
		edition:             "Developer Edition (64-bit)",
		databaseName:        "dmtx_snapshot",
		compatibilityLevel:  160,
		state:               "ONLINE",
		userAccess:          "MULTI_USER",
		containment:         "NONE",
		readOnly:            true,
		sourceDatabaseID:    sql.NullInt64{Int64: 7, Valid: true},
	}
	if err := validateSQLServer2022MigrationSnapshotCatalog(base); err != nil {
		t.Fatalf("valid SQL Server migration snapshot catalog: %v", err)
	}
	if err := validateSQLServer2022SourceCatalog(base); err == nil {
		t.Fatal("ordinary SQL Server source admission accepted a database snapshot")
	}
	for name, mutate := range map[string]func(*sqlServer2022SourceCatalog){
		"writable": func(value *sqlServer2022SourceCatalog) {
			value.readOnly = false
		},
		"missing source database identity": func(value *sqlServer2022SourceCatalog) {
			value.sourceDatabaseID = sql.NullInt64{}
		},
		"invalid source database identity": func(value *sqlServer2022SourceCatalog) {
			value.sourceDatabaseID.Int64 = 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			if err := validateSQLServer2022MigrationSnapshotCatalog(value); !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestValidSQLServerSourceIdentifierRejectsLossySurrogateDecode(
	t *testing.T,
) {
	if validSQLServerSourceIdentifier("safe\uFFFDname") {
		t.Fatal("replacement rune was accepted at the sysname boundary")
	}
}

func TestValidateSQLServerSourceTableCatalogFailsClosed(t *testing.T) {
	base := sqlServerSourceTableCatalog{
		objectID:        42,
		typeDescription: "USER_TABLE",
		durability:      "SCHEMA_AND_DATA",
		columnCount:     3,
		maxPartition:    1,
	}
	if err := validateSQLServerSourceTableCatalog(
		"dbo",
		"events",
		base,
	); err != nil {
		t.Fatalf("valid SQL Server table catalog: %v", err)
	}
	tests := map[string]func(*sqlServerSourceTableCatalog){
		"system table": func(value *sqlServerSourceTableCatalog) {
			value.systemShipped = true
		},
		"temporal": func(value *sqlServerSourceTableCatalog) {
			value.temporalType = 2
		},
		"memory optimized": func(value *sqlServerSourceTableCatalog) {
			value.memoryOptimized = true
		},
		"filestream": func(value *sqlServerSourceTableCatalog) {
			value.fileStreamDataSpaceID.Valid = true
			value.fileStreamDataSpaceID.Int64 = 2
		},
		"replicated": func(value *sqlServerSourceTableCatalog) {
			value.replicated = true
		},
		"graph": func(value *sqlServerSourceTableCatalog) {
			value.edge = true
		},
		"ledger": func(value *sqlServerSourceTableCatalog) {
			value.ledgerType = 2
		},
		"partitioned": func(value *sqlServerSourceTableCatalog) {
			value.maxPartition = 2
		},
		"triggered": func(value *sqlServerSourceTableCatalog) {
			value.triggerCount = 1
		},
		"row security": func(value *sqlServerSourceTableCatalog) {
			value.securityPredicateCount = 1
		},
		"full text": func(value *sqlServerSourceTableCatalog) {
			value.fullTextIndexCount = 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			if err := validateSQLServerSourceTableCatalog(
				"dbo",
				"events",
				value,
			); !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestSQLServerSourceColumnFromCatalogPreservesModifiers(t *testing.T) {
	tests := []struct {
		name    string
		catalog sqlServerSourceColumnCatalog
		want    schema.Column
	}{
		{
			name: "bigint",
			catalog: sqlServerSourceTestColumn(
				"id",
				"bigint",
				8,
				19,
				0,
			),
			want: schema.Column{
				Name: "id",
				Type: "bigint",
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
		},
		{
			name: "decimal",
			catalog: sqlServerSourceTestColumn(
				"amount",
				"decimal",
				9,
				12,
				3,
			),
			want: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 3},
				},
			},
		},
		{
			name: "UTF8 varchar",
			catalog: func() sqlServerSourceColumnCatalog {
				value := sqlServerSourceTestColumn(
					"note",
					"varchar",
					80,
					0,
					0,
				)
				value.ansiPadded = true
				value.collation.Valid = true
				value.collation.String =
					"Latin1_General_100_BIN2_UTF8"
				return value
			}(),
			want: schema.Column{
				Name: "note",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{80},
				},
			},
		},
		{
			name: "datetime2 microseconds",
			catalog: sqlServerSourceTestColumn(
				"occurred_at",
				"datetime2",
				8,
				26,
				6,
			),
			want: schema.Column{
				Name: "occurred_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{6},
				},
			},
		},
		{
			name: "smalldatetime exact minute",
			catalog: sqlServerSourceTestColumn(
				"minute_at",
				"smalldatetime",
				4,
				16,
				0,
			),
			want: schema.Column{
				Name: "minute_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "smalldatetime",
				},
			},
		},
		{
			name: "varbinary max",
			catalog: func() sqlServerSourceColumnCatalog {
				value := sqlServerSourceTestColumn(
					"payload",
					"varbinary",
					-1,
					0,
					0,
				)
				value.ansiPadded = true
				value.nullable = true
				return value
			}(),
			want: schema.Column{
				Name:     "payload",
				Type:     "blob",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "blob",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, identity, err := sqlServerSourceColumnFromCatalog(
				test.catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if identity != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf(
					"column = %#v, identity = %#v; want %#v, nil",
					got,
					identity,
					test.want,
				)
			}
		})
	}
}

func TestSQLServerSourceColumnFromCatalogRejectsUnsafeShapes(t *testing.T) {
	base := sqlServerSourceTestColumn("id", "bigint", 8, 19, 0)
	tests := map[string]func(*sqlServerSourceColumnCatalog){
		"user type": func(value *sqlServerSourceColumnCatalog) {
			value.userDefined = true
		},
		"computed": func(value *sqlServerSourceColumnCatalog) {
			value.computed = true
		},
		"masked": func(value *sqlServerSourceColumnCatalog) {
			value.masked = true
		},
		"encrypted": func(value *sqlServerSourceColumnCatalog) {
			value.encryptionType.Valid = true
			value.encryptionType.Int64 = 1
		},
		"rule bound": func(value *sqlServerSourceColumnCatalog) {
			value.ruleObjectID = 8
		},
		"bad integer width": func(value *sqlServerSourceColumnCatalog) {
			value.maxLength = 4
		},
		"unexpected ANSI padding": func(value *sqlServerSourceColumnCatalog) {
			value.ansiPadded = true
		},
		"datetime2 precision mismatch": func(value *sqlServerSourceColumnCatalog) {
			value.typeName = "datetime2"
			value.maxLength = 8
			value.precision = 25
			value.scale = 6
		},
		"datetime2 nanoseconds": func(value *sqlServerSourceColumnCatalog) {
			value.typeName = "datetime2"
			value.maxLength = 8
			value.precision = 27
			value.scale = 7
		},
		"datetimeoffset loses source offset": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "datetimeoffset"
			value.maxLength = 10
			value.precision = 33
			value.scale = 6
		},
		"char max catalog shape": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "char"
			value.maxLength = -1
			value.precision = 0
			value.ansiPadded = true
			value.collation = sql.NullString{
				String: "Latin1_General_100_BIN2_UTF8",
				Valid:  true,
			}
		},
		"nchar max catalog shape": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "nchar"
			value.maxLength = -1
			value.precision = 0
			value.ansiPadded = true
			value.collation = sql.NullString{
				String: "Latin1_General_100_BIN2",
				Valid:  true,
			}
		},
		"binary max catalog shape": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "binary"
			value.maxLength = -1
			value.precision = 0
			value.ansiPadded = true
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			_, _, err := sqlServerSourceColumnFromCatalog(value)
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}

	// An ordinary collation is certified for transfer. It is refused only for a
	// key, which sqlServerSourceColumnFromCatalog cannot know about - see
	// TestSQLServerSourceKeyCollationsMustOrderTheSameOnBothEngines.
	text := sqlServerSourceTestColumn(
		"label",
		"varchar",
		20,
		0,
		0,
	)
	text.ansiPadded = true
	text.collation.Valid = true
	text.collation.String = "SQL_Latin1_General_CP1_CI_AS"
	if _, _, err := sqlServerSourceColumnFromCatalog(text); err != nil {
		t.Fatalf("ordinary collation was refused for a data column: %v", err)
	}
}

func TestApplySQLServerSourceIdentityRequiresSingleBigintPrimaryKey(
	t *testing.T,
) {
	frontier := int64(41)
	table := schema.Table{
		Schema: "dbo",
		Name:   "accounts",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
	}
	if err := applySQLServerSourceIdentity(
		&table,
		&sqlServerSourceIdentityCatalog{
			column:   "id",
			frontier: &frontier,
		},
	); err != nil {
		t.Fatal(err)
	}
	if table.Identity == nil ||
		table.Identity.Column != "id" ||
		table.Identity.Frontier == nil ||
		*table.Identity.Frontier != 41 {
		t.Fatalf("identity = %#v", table.Identity)
	}

	table.Columns = append(table.Columns, schema.Column{
		Name:               "tenant",
		Type:               "integer",
		PrimaryKey:         true,
		PrimaryKeyPosition: 2,
	})
	if err := applySQLServerSourceIdentity(
		&table,
		&sqlServerSourceIdentityCatalog{column: "id"},
	); err == nil {
		t.Fatal("composite-key SQL Server identity was accepted")
	}
}

func sqlServerSourceTestColumn(
	name string,
	typeName string,
	maxLength int,
	precision int,
	scale int,
) sqlServerSourceColumnCatalog {
	return sqlServerSourceColumnCatalog{
		position:    1,
		name:        name,
		typeSchema:  "sys",
		typeName:    typeName,
		maxLength:   maxLength,
		precision:   precision,
		scale:       scale,
		defaultName: sql.NullString{},
	}
}

// TestSQLServerCertifiedSourceTypes pins what dmtx will read from SQL Server.
//
// dmtx refuses a type it has not certified, which is the right posture for a
// tool that moves production data: an unknown type is a value nobody has proved
// survives the trip. The failure this test exists after was not the posture but
// the size of the certified set. Text was certified only under
// Latin1_General_100_BIN2_UTF8 - a collation nobody has unless they chose it -
// and datetime was not certified at all while datetime2 was. So an ordinary
// StackOverflow table, whose columns carry the SQL_Latin1_General_CP1_CI_AS
// that SQL Server installs by default, could not be read at all, and fourteen
// minutes of armed CI passed over it because dmtx's own fixture stamped the one
// accepted collation onto every column it created.
//
// Certification here means transfer fidelity: the value that leaves the source
// is the value that arrives. It is established by round-tripping boundary
// values through a real engine, not by argument - see the live test named in
// each entry below. Ordering is a separate certification, asked only of keys,
// because that is the only place it changes an answer.
func TestSQLServerCertifiedSourceTypes(t *testing.T) {
	for _, certified := range []struct {
		name      string
		typeName  string
		maxLength int
		precision int
		scale     int
		collation string
		ansiPad   bool
		wantType  string
		wantBase  string
		wantArgs  []int
	}{
		{
			name:      "nvarchar under the default collation",
			typeName:  "nvarchar",
			maxLength: 80, // bytes; 40 characters
			collation: "SQL_Latin1_General_CP1_CI_AS",
			ansiPad:   true,
			wantType:  "text",
			wantBase:  "nvarchar",
			wantArgs:  []int{40},
		},
		{
			name:      "nvarchar(max)",
			typeName:  "nvarchar",
			maxLength: -1,
			collation: "SQL_Latin1_General_CP1_CI_AS",
			ansiPad:   true,
			wantType:  "text",
			wantBase:  "text",
		},
		{
			name:      "nchar under the default collation",
			typeName:  "nchar",
			maxLength: 20, // bytes; 10 characters
			collation: "SQL_Latin1_General_CP1_CI_AS",
			ansiPad:   true,
			wantType:  "text",
			wantBase:  "nchar",
			wantArgs:  []int{10},
		},
		{
			name:      "varchar under the default collation",
			typeName:  "varchar",
			maxLength: 50,
			collation: "SQL_Latin1_General_CP1_CI_AS",
			ansiPad:   true,
			wantType:  "text",
			wantBase:  "varchar",
			wantArgs:  []int{50},
		},
		{
			name:      "datetime",
			typeName:  "datetime",
			maxLength: 8,
			precision: 23,
			scale:     3,
			wantType:  "datetime",
			wantBase:  "timestamp",
			wantArgs:  []int{3},
		},
	} {
		t.Run(certified.name, func(t *testing.T) {
			value := sqlServerSourceTestColumn(
				"c",
				certified.typeName,
				certified.maxLength,
				certified.precision,
				certified.scale,
			)
			value.ansiPadded = certified.ansiPad
			if certified.collation != "" {
				value.collation = sql.NullString{
					String: certified.collation,
					Valid:  true,
				}
			}
			column, _, err := sqlServerSourceColumnFromCatalog(value)
			if err != nil {
				t.Fatalf("certified type was refused: %v", err)
			}
			if column.Type != certified.wantType {
				t.Errorf("portable type = %q, want %q", column.Type, certified.wantType)
			}
			if column.DeclaredType == nil {
				t.Fatal("no declared type")
			}
			if column.DeclaredType.Base != certified.wantBase {
				t.Errorf(
					"declared base = %q, want %q",
					column.DeclaredType.Base,
					certified.wantBase,
				)
			}
			if len(column.DeclaredType.Arguments) != len(certified.wantArgs) {
				t.Fatalf(
					"declared arguments = %v, want %v",
					column.DeclaredType.Arguments,
					certified.wantArgs,
				)
			}
			for index, want := range certified.wantArgs {
				if column.DeclaredType.Arguments[index] != want {
					t.Fatalf(
						"declared arguments = %v, want %v",
						column.DeclaredType.Arguments,
						certified.wantArgs,
					)
				}
			}
		})
	}
}

// TestNationalTextLengthIsCharactersNotBytes is worth its own test because
// getting it wrong is silent.
//
// sys.columns.max_length is bytes, and the national types store two bytes per
// character. Reading it straight through would declare nvarchar(40) as
// nvarchar(80) in the target - a schema that still loads, still validates, and
// is wrong in a way no row count would show.
func TestNationalTextLengthIsCharactersNotBytes(t *testing.T) {
	value := sqlServerSourceTestColumn("c", "nvarchar", 80, 0, 0)
	value.ansiPadded = true
	value.collation = sql.NullString{String: "SQL_Latin1_General_CP1_CI_AS", Valid: true}
	column, _, err := sqlServerSourceColumnFromCatalog(value)
	if err != nil {
		t.Fatal(err)
	}
	if column.DeclaredType.Arguments[0] != 40 {
		t.Fatalf("nvarchar length = %d characters, want 40", column.DeclaredType.Arguments[0])
	}

	// An odd byte length cannot be a whole number of UTF-16 units, so the
	// catalog is not describing what it claims to.
	odd := sqlServerSourceTestColumn("c", "nvarchar", 81, 0, 0)
	odd.ansiPadded = true
	odd.collation = sql.NullString{String: "SQL_Latin1_General_CP1_CI_AS", Valid: true}
	if _, _, err := sqlServerSourceColumnFromCatalog(odd); err == nil {
		t.Fatal("an odd byte length was accepted for a national type")
	}
}

// TestSQLServerSourceKeyCollationsMustOrderTheSameOnBothEngines pins the
// certification that did NOT move.
//
// Ordering is asked only of key columns, and asking it there is not a
// formality. A paged read orders by the key; under a case-insensitive collation
// SQL Server says 'a' = 'A' and PostgreSQL does not, so a chunk boundary
// computed on one engine does not mean the same thing on the other and rows are
// skipped or repeated at the seam. That is silent corruption proportional to
// table size, which is the kind speed is not worth trading for - and unlike a
// per-value check it costs nothing at run time, being read once from the
// catalog at discovery.
func TestSQLServerSourceKeyCollationsMustOrderTheSameOnBothEngines(t *testing.T) {
	keyed := func(base string) schema.Table {
		return schema.Table{
			Schema: "dbo",
			Name:   "Tags",
			Columns: []schema.Column{{
				Name:               "TagName",
				Type:               "text",
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base:      base,
					Arguments: []int{40},
				},
			}},
		}
	}

	// Each of these was measured against SQL Server 2022 rather than argued
	// from the collation's name, and two of the three refusals are near misses
	// that read as safe.
	for _, refused := range []struct {
		reason    string
		base      string
		collation string
	}{
		{
			reason:    "case-insensitive: 'a' = 'A' here and not in PostgreSQL",
			base:      "varchar",
			collation: "SQL_Latin1_General_CP1_CI_AS",
		},
		{
			// Binary, and still wrong: it orders by CP1252 bytes, so
			// [EUR, y-diaeresis] where UTF-8 gives the reverse.
			reason:    "narrow _BIN2 is a codepage ordering",
			base:      "varchar",
			collation: "Latin1_General_100_BIN2",
		},
		{
			// UTF-16 code-unit order agrees with PostgreSQL across the BMP and
			// stops above it, because surrogates sit at D800-DFFF while the
			// characters they encode live at U+10000 up.
			reason:    "national types order by UTF-16 code unit",
			base:      "nvarchar",
			collation: "Latin1_General_100_BIN2",
		},
		{
			// SQL Server's _UTF8 collations change the encoding of char and
			// varchar only, so this does not make an nvarchar key portable.
			reason:    "a _UTF8 collation does not re-encode a national type",
			base:      "nvarchar",
			collation: "Latin1_General_100_BIN2_UTF8",
		},
	} {
		if err := checkSQLServerSourceKeyCollations(
			keyed(refused.base),
			map[string]string{"TagName": refused.collation},
		); err == nil {
			t.Errorf("accepted a text key that %s", refused.reason)
		}
	}

	// The one spelling whose ordering was measured to agree.
	for _, accepted := range []string{
		"Latin1_General_100_BIN2_UTF8",
		// Any locale: BIN2 ignores the prefix and _UTF8 fixes the encoding.
		"Japanese_XJIS_140_BIN2_UTF8",
	} {
		if err := checkSQLServerSourceKeyCollations(
			keyed("varchar"),
			map[string]string{"TagName": accepted},
		); err != nil {
			t.Errorf("refused %s, whose ordering matches: %v", accepted, err)
		}
	}

	// The same column as data rather than key is not asked the question at all.
	data := keyed("nvarchar")
	data.Columns = []schema.Column{{
		Name:         "TagName",
		Type:         "text",
		DeclaredType: &schema.DeclaredType{Base: "nvarchar", Arguments: []int{40}},
	}}
	if err := checkSQLServerSourceKeyCollations(
		data,
		map[string]string{"TagName": "SQL_Latin1_General_CP1_CI_AS"},
	); err != nil {
		t.Fatalf("an ordinary collation was refused for a data column: %v", err)
	}
}

// TestRefusedTextKeySaysWhatWouldWork guards the message, not the refusal.
//
// It used to say a text key "needs a binary collation". Latin1_General_100_BIN2
// is a binary collation and is refused, so an operator read that, looked at
// their column, and had nowhere to go. The two failure modes have different
// remedies and neither is guessable from the collation's name, so the message
// has to name the one that applies.
func TestRefusedTextKeySaysWhatWouldWork(t *testing.T) {
	keyed := func(base string) schema.Table {
		return schema.Table{
			Schema: "dbo",
			Name:   "Tags",
			Columns: []schema.Column{{
				Name:               "TagName",
				Type:               "text",
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base:      base,
					Arguments: []int{40},
				},
			}},
		}
	}

	narrow := checkSQLServerSourceKeyCollations(
		keyed("varchar"),
		map[string]string{"TagName": "Latin1_General_100_BIN2"},
	)
	if narrow == nil {
		t.Fatal("a narrow _BIN2 key was accepted")
	}
	// The spelling that would work has to appear, or the operator is left
	// guessing which of SQL Server's collations qualifies.
	if !strings.Contains(narrow.Error(), "_BIN2_UTF8") {
		t.Errorf("refusal does not name the collation that works: %v", narrow)
	}
	// And it must not describe the requirement as "binary", which is what the
	// refused collation already is.
	if strings.Contains(narrow.Error(), "needs a binary collation") {
		t.Errorf("refusal still calls the requirement binary: %v", narrow)
	}

	national := checkSQLServerSourceKeyCollations(
		keyed("nvarchar"),
		map[string]string{"TagName": "Latin1_General_100_BIN2_UTF8"},
	)
	if national == nil {
		t.Fatal("a national text key was accepted")
	}
	// This one has no collation that would fix it, and saying so is the whole
	// point: otherwise the operator tries _BIN2_UTF8, which the other branch
	// recommends, and is refused again for a reason nothing explained.
	if !strings.Contains(national.Error(), "whatever its collation") {
		t.Errorf("national refusal implies a collation would fix it: %v", national)
	}
	if !strings.Contains(national.Error(), "UTF-16") {
		t.Errorf("national refusal does not say why: %v", national)
	}
}
