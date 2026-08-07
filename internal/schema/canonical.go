package schema

// The canonical type: what a value is, independent of which engine spelled it.
//
// dmtx already had two thirds of this and did not route through it. Column.Type
// carries a portable kind ("text", "datetime", "integer") and DeclaredType
// carries the engine's own spelling ("nvarchar", "datetime2") with its
// modifiers. What was missing is a single place that turns one engine's
// declaration into another's, so instead there are eleven pairwise projections
// under internal/migrate/adapter_target_*_schema.go - 6148 lines - each
// deciding the same facts again.
//
// Deciding them again is not a theoretical cost. In one day, four facts each
// needed edits in several places and were wrong in at least one:
//
//   "nvarchar is readable"          4 sites, fixed in one projection, missed in another
//   "nvarchar caps at 4000"         3 sites, wrong in one
//   "MySQL's certified collations"  4 sites, already disagreeing before anyone
//                                   touched them - utf8mb4_bin passed discovery
//                                   and failed both projections
//   "SQL Server's certified collations"  2 sites, source demanding what the
//                                        target writes
//
// N engines with pairwise projection is N² places for one fact to live. A
// canonical intermediate makes it N converters in and N renderers out, and a
// fact has one home.

// Kind is what a value is. Deliberately small.
//
// These are the kinds dmtx's sources actually emit - surveyed from every
// `column.Type = "..."` in internal/engine - rather than a general schema
// tool's enum. dmtx has to carry values and order keys; it does not have to
// represent every type a database can declare, and a kind it cannot certify is
// a kind it should refuse rather than model.
type Kind string

const (
	// KindUnknown is the zero value and is never certified. A type that reaches
	// a renderer as Unknown is a type discovery admitted without classifying,
	// which is a bug rather than a shape to handle.
	KindUnknown Kind = ""

	KindBoolean  Kind = "boolean"
	KindInteger  Kind = "integer"
	KindBigInt   Kind = "bigint"
	KindNumeric  Kind = "numeric"
	KindReal     Kind = "real"
	KindDouble   Kind = "double precision"
	KindText     Kind = "text"
	KindBlob     Kind = "blob"
	KindDate     Kind = "date"
	KindTime     Kind = "time"
	KindDateTime Kind = "datetime"
	KindUUID     Kind = "uuid"
	KindJSON     Kind = "json"
)

// Certification is what dmtx has established about a column, and it is the
// thing SMT's canonical type has no room for.
//
// A schema migration tool renders DDL and never orders a paged read, so it
// needs one question answered: can this type be written on the other side.
// dmtx moves rows, which asks two, and conflating them is what made an ordinary
// StackOverflow table unreadable for months.
type Certification struct {
	// Transfer reports that the value which leaves the source is the value that
	// arrives. Asked of every column. Established by round-tripping boundary
	// values through real engines - emoji through a surrogate pair, CJK,
	// CP1252 high bytes, datetime at .003/.287/.997 - not by argument.
	Transfer bool

	// Ordering reports that equality and ordering mean the same thing on both
	// engines. Asked only of columns that order a paged read or prove a row,
	// because that is the only place the answer changes anything.
	//
	// It is not a formality there. Under a case-insensitive collation SQL Server
	// and MySQL call two different strings equal where PostgreSQL does not, so a
	// chunk boundary computed on one engine does not mean the same thing on the
	// other and rows are skipped or repeated at the seam - silent corruption
	// proportional to table size.
	//
	// Measured, per engine, rather than inferred from a collation's name:
	//
	//	SQL Server  only char/varchar under *_BIN2_UTF8. A narrow _BIN2 orders
	//	            by code page; the national types are UTF-16 and order by
	//	            code unit, which diverges above the BMP.
	//	MySQL       only utf8mb4_bin and utf8mb4_0900_bin.
	Ordering bool

	// Why a certification was withheld, in the words an operator can act on.
	// Empty when both hold.
	Reason string
}

// CanonicalType is a value's shape with no engine attached.
//
// The modifiers are pointers because absent and zero differ: a NUMERIC with no
// precision is not NUMERIC(0), and a timestamp with no fractional precision is
// not timestamp(0). Collapsing those was how a datetime lost its milliseconds.
type CanonicalType struct {
	Kind Kind

	// Length is in CHARACTERS for text and BYTES for blob, and the distinction
	// is not pedantry. SQL Server's nchar/nvarchar declare UTF-16 code units
	// while char/varchar declare bytes; MySQL's sys catalog reports bytes for
	// both. Carrying the wrong unit here declared an nvarchar(40) as
	// varchar(80) in the target - a schema that loads, validates, and is wrong
	// in a way no row count shows.
	Length *int64

	// Precision and Scale apply to numeric.
	Precision *int64
	Scale     *int64

	// FractionalSecondPrecision is digits, for time and datetime.
	FractionalSecondPrecision *int64

	// Certification travels with the type because it is a property of this
	// column on this route, not of the kind. Two text columns in one table can
	// differ: the key must order portably and the body need only arrive.
	Certification Certification
}

// Certified reports whether this type may be carried at all.
//
// Transfer alone. Ordering is asked separately, of the columns that need it,
// because a column that cannot order is still perfectly transferable - and
// refusing it would be the mistake that made Users.AboutMe unreadable.
func (value CanonicalType) Certified() bool {
	return value.Kind != KindUnknown && value.Certification.Transfer
}

// CertifiedAsKey reports whether this type may order a paged read.
func (value CanonicalType) CertifiedAsKey() bool {
	return value.Certified() && value.Certification.Ordering
}
