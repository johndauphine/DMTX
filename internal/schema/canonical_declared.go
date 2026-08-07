package schema

import "fmt"

// Canonical types, projected into a target's catalog shape.
//
// RenderCanonical writes DDL. This writes what dmtx carries between the
// engines: the portable Column.Type a target assigns, and the DeclaredType
// that reproduces its catalog. The pairwise projections under internal/migrate
// produce exactly this pair, so this is the function they collapse into.
//
// Kept separate from the DDL renderer on purpose. A target's catalog and its
// CREATE TABLE text are not the same statement of the type - PostgreSQL's
// catalog reports character varying where its DDL says VARCHAR - and a single
// function serving both would have to pick one and lie about the other.

// CanonicalToDeclared gives the portable type and declaration a target should
// carry for this canonical type.
//
// The declaration is nil where a target's catalog adds nothing to the portable
// name. That is not an omission: a PostgreSQL integer is an integer, and
// inventing a declaration for it would put a fact in the target authority that
// a later incremental run would then have to authenticate against a catalog
// that never said it.
func CanonicalToDeclared(
	value CanonicalType,
	target Dialect,
) (string, *DeclaredType, error) {
	if !value.Certified() {
		return "", nil, &PolicyError{
			Operation: "project canonical type",
			Type:      string(value.Kind),
			Target:    string(target),
		}
	}

	if target == SQLServer {
		return canonicalToSQLServerDeclared(value)
	}

	switch value.Kind {
	case KindBoolean:
		return "boolean", nil, nil
	case KindSmallInt, KindInteger:
		// The PostgreSQL target widens a small integer to integer, and that is
		// compatibility rather than indifference. It has been writing integer
		// for SQL Server's tinyint and smallint since before this package
		// existed, and target authority is authenticated against the target's
		// catalog on later incremental runs - so a dmtx that started declaring
		// smallint would disagree with every target an older dmtx created.
		//
		// The SQL Server target does honour the width, because nothing has been
		// written there yet that says otherwise.
		return "integer", nil, nil
	case KindBigInt:
		return "bigint", nil, nil
	case KindReal:
		// PostgreSQL's catalog round-trips real as a declaration of its own,
		// so the projection keeps it rather than leaving the target authority
		// to be reconstructed from the portable name alone.
		return "real", &DeclaredType{Base: "real"}, nil
	case KindDouble:
		return "double precision", nil, nil
	case KindNumeric:
		declared := &DeclaredType{Base: "numeric"}
		if value.Precision != nil {
			precision := *value.Precision
			declared.Precision = &precision
			scale := int64(0)
			if value.Scale != nil {
				scale = *value.Scale
			}
			declared.Scale = &scale
			declared.Arguments = []int{int(precision), int(scale)}
		}
		return "numeric", declared, nil
	case KindText:
		return canonicalTextDeclared(value)
	case KindBinary, KindBlob:
		// The portable name a target assigns is the target's own, not a
		// neutral one: PostgreSQL's catalog says bytea where SQLite says blob,
		// and target authority is authenticated against that catalog.
		//
		// Fixed and varying collapse here because PostgreSQL has no fixed-width
		// binary type. The padding bytes are part of the stored value, so they
		// arrive; what is lost is the target's ability to re-declare the width,
		// which is a schema fact rather than a data one.
		if target == Postgres {
			return "bytea", nil, nil
		}
		return "blob", nil, nil
	case KindDate:
		return "date", nil, nil
	case KindTime:
		return canonicalTemporalDeclared(value, "time", target)
	case KindDateTime:
		return canonicalTemporalDeclared(value, "timestamp", target)
	case KindUUID:
		return "uuid", nil, nil
	case KindJSON:
		return "json", nil, nil
	default:
		return "", nil, &PolicyError{
			Operation: "project canonical type",
			Type:      string(value.Kind),
			Target:    string(target),
		}
	}
}

// canonicalTextDeclared keeps a bound bounded and an absence absent.
//
// A bounded source column becomes a bounded target column of the same length in
// CHARACTERS - the converters have already done whatever byte arithmetic their
// engine required, which is the step that was missing when an nvarchar(40)
// reached a target as varchar(80).
//
// Unbounded stays unbounded, and it is spelled text rather than varchar with no
// argument, because a PostgreSQL catalog round trip reports those differently
// and target authority is authenticated against that catalog on later runs.
func canonicalTextDeclared(value CanonicalType) (string, *DeclaredType, error) {
	if value.Length == nil {
		return "text", nil, nil
	}
	// Arguments only, with Length left nil. That is what the pairwise
	// projection wrote and therefore what target authority has been
	// authenticated against on incremental runs - populating a second field
	// carrying the same number would be a change to the recorded shape, which
	// a later run compares against a catalog that never mentioned it.
	length := *value.Length
	return "varchar", &DeclaredType{
		Base:      "varchar",
		Arguments: []int{int(length)},
	}, nil
}

// canonicalTemporalDeclared keeps fractional-second precision, including a
// precision of zero, which is not the same as none.
func canonicalTemporalDeclared(
	value CanonicalType,
	base string,
	target Dialect,
) (string, *DeclaredType, error) {
	declared := &DeclaredType{Base: base}
	if value.FractionalSecondPrecision != nil {
		digits := *value.FractionalSecondPrecision
		// The target's own ceiling, not the source's. SQL Server's datetime2
		// carries seven fractional digits and PostgreSQL's timestamp carries
		// six, so a seven-digit source is not something to render smaller - it
		// is something this route cannot carry, and rendering it as six would
		// truncate every value while producing a schema that loads.
		if digits < 0 || digits > temporalDigitCeiling(target) {
			return "", nil, &PolicyError{
				Operation: "project canonical temporal precision",
				Type:      base,
				Target:    string(target),
			}
		}
		// Arguments only, matching the shape the pairwise projection recorded -
		// the same reason canonicalTextDeclared leaves Length nil.
		declared.Arguments = []int{int(digits)}
	}
	return base, declared, nil
}

// temporalDigitCeiling is the most fractional-second digits a target can hold.
func temporalDigitCeiling(target Dialect) int64 {
	switch target {
	case SQLServer:
		return 7
	default:
		// PostgreSQL, MySQL and SQLite all stop at six.
		return 6
	}
}

// canonicalToSQLServerDeclared is the SQL Server target's own vocabulary.
//
// A target records the names its catalog reports, and SQL Server's differ from
// PostgreSQL's in both directions: it declares int where PostgreSQL declares
// nothing at all, and decimal where PostgreSQL says numeric. Target authority
// is authenticated against that catalog on later incremental runs, so the
// declaration has to be what SQL Server will say, not a neutral spelling.
func canonicalToSQLServerDeclared(
	value CanonicalType,
) (string, *DeclaredType, error) {
	declared := func(base string, arguments ...int) *DeclaredType {
		return &DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		}
	}
	switch value.Kind {
	case KindBoolean:
		return "boolean", declared("bool"), nil
	case KindSmallInt:
		// SMALLINT, never TINYINT. SQL Server's TINYINT is unsigned 0-255 and
		// every other engine's is signed, so declaring TINYINT here would refuse
		// half the range the source can hold. The portable name stays "integer"
		// because that is what SQL Server discovery assigns to smallint, and the
		// target authority is compared against what discovery will say.
		return "integer", declared("smallint"), nil
	case KindInteger:
		return "integer", declared("int"), nil
	case KindBigInt:
		return "bigint", declared("bigint"), nil
	case KindReal:
		return "real", declared("real"), nil
	case KindDouble:
		// A declaration, and the comment that used to sit here was wrong on the
		// fact it rested on. It claimed SQL Server's catalog does not report one;
		// mssql_source_discovery.go writes declaration("double precision") for
		// float, and renderSQLServerDeclaredColumn refuses a column that has
		// none - so a PostgreSQL double reaching a SQL Server target could not be
		// created at all. The old pairwise projection wrote this declaration and
		// the swap dropped it.
		//
		// Neither the differential test nor the armed live gate caught it. The
		// test had no double-precision case, and the SO2010 corpus has no
		// floating-point column at all: a corpus proves what it contains, and
		// nothing about what it does not.
		return "double precision", declared("double precision"), nil
	case KindNumeric:
		if value.Precision == nil {
			return "", nil, &PolicyError{
				Operation: "project canonical type",
				Type:      "numeric without precision",
				Target:    string(SQLServer),
			}
		}
		scale := int64(0)
		if value.Scale != nil {
			scale = *value.Scale
		}
		// SQL Server's decimal carries at most 38 digits, and the scale cannot
		// exceed them. Both bounds were in the pairwise projections and both
		// swaps dropped them, so numeric(39,2) was projected happily and then
		// refused by the renderer at CREATE TABLE time - a failure at the last
		// possible moment, naming the declaration rather than the source column
		// it came from.
		//
		// It belongs here rather than in each converter for the same reason the
		// temporal ceiling does: it is a fact about what this TARGET can
		// declare, and a source has no way to know it.
		if *value.Precision < 1 ||
			*value.Precision > SQLServerNumericPrecisionLimit ||
			scale < 0 || scale > *value.Precision {
			return "", nil, &PolicyError{
				Operation: "project canonical numeric precision",
				Type: fmt.Sprintf(
					"decimal(%d,%d) exceeds what SQL Server can declare",
					*value.Precision, scale,
				),
				Target: string(SQLServer),
			}
		}
		return "numeric", declared("decimal", int(*value.Precision), int(scale)), nil
	case KindText:
		return canonicalToSQLServerText(value, declared)
	case KindBinary, KindBlob:
		return canonicalToSQLServerBinary(value, declared)
	case KindDate:
		return "date", declared("date"), nil
	case KindTime:
		if value.FractionalSecondPrecision == nil {
			return "time", declared("time"), nil
		}
		return "time", declared("time", int(*value.FractionalSecondPrecision)), nil
	case KindDateTime:
		if value.FractionalSecondPrecision == nil {
			return "datetime", declared("timestamp"), nil
		}
		return "datetime",
			declared("timestamp", int(*value.FractionalSecondPrecision)), nil
	case KindUUID:
		// Declared, unlike on the PostgreSQL target. SQL Server's catalog
		// reports uniqueidentifier as its own type, so the target authority
		// records it rather than reconstructing it from the portable name.
		return "uuid", declared("uuid"), nil
	case KindJSON:
		// SQL Server has no JSON type; it holds the document as text, which is
		// what the pairwise projection did and is why json and bytea share a
		// target shape here.
		return "blob", declared("blob"), nil
	default:
		return "", nil, &PolicyError{
			Operation: "project canonical type",
			Type:      string(value.Kind),
			Target:    string(SQLServer),
		}
	}
}

// canonicalToSQLServerBinary keeps the width and the padding.
//
// SQL Server's limit is the same 8000 bytes its narrow text families have, and
// past it a byte string has to be VARBINARY(MAX) - which discovery spells
// "blob" with no argument. Both facts are the same shape as the text case above
// and neither is the same NUMBER as it, so they are written out rather than
// shared: the text limit is about a UTF-8 collation and this one is not.
//
// No multiplication happens here. Length is already bytes for a byte string,
// which is the whole reason CanonicalType documents its unit per kind - the
// text path multiplies by four and this one must not, and a single shared
// helper would have had to be told which.
func canonicalToSQLServerBinary(
	value CanonicalType,
	declared func(string, ...int) *DeclaredType,
) (string, *DeclaredType, error) {
	if value.Length == nil {
		return "blob", declared("blob"), nil
	}
	if *value.Length > SQLServerBinaryLengthLimit {
		// Wider than SQL Server can declare, so it becomes VARBINARY(MAX)
		// rather than being truncated - which would refuse values the source
		// holds. Fixed width is lost along with the bound, and it has to be:
		// there is no MAX form of BINARY.
		return "blob", declared("blob"), nil
	}
	base := "varbinary"
	if value.Kind == KindBinary {
		base = "binary"
	}
	return "blob", declared(base, int(*value.Length)), nil
}

// canonicalToSQLServerText multiplies a character length into bytes.
//
// This is the mirror of the halving on the SQL Server SOURCE side, and getting
// the direction wrong is the defect this whole lattice was written after.
//
// A canonical Length is CHARACTERS. SQL Server's varchar(n) under the UTF-8
// collation dmtx writes counts BYTES, and one character can spend four of them,
// so the declared length is multiplied by four. That is a widening: every value
// the source could hold still fits, and no value the source could not hold
// becomes representable, because the source's own bound still governs what
// arrives.
//
// Going the other way the same fact halves - sys.columns.max_length is bytes
// and the national types store two per unit. One fact, two directions, and only
// the direction says which arithmetic applies. That is precisely why it belongs
// here rather than in each projection that happens to need it.
func canonicalToSQLServerText(
	value CanonicalType,
	declared func(string, ...int) *DeclaredType,
) (string, *DeclaredType, error) {
	if value.Length == nil {
		return "text", declared("text"), nil
	}
	bytes := *value.Length * 4
	if bytes > SQLServerNarrowTextLimit {
		// Beyond what varchar can declare, so it becomes unbounded rather than
		// being truncated to the limit - which would refuse values the source
		// holds.
		return "text", declared("text"), nil
	}
	return "text", declared("varchar", int(bytes)), nil
}
