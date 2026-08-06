package schema

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

	switch value.Kind {
	case KindBoolean:
		return "boolean", nil, nil
	case KindInteger:
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
	case KindBlob:
		// The portable name a target assigns is the target's own, not a
		// neutral one: PostgreSQL's catalog says bytea where SQLite says blob,
		// and target authority is authenticated against that catalog.
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
