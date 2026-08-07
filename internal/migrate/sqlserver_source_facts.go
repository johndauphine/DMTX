package migrate

// Facts about SQL Server sources that more than one target projection needs.
//
// This limit lived in the PostgreSQL adapter and was then used by the MySQL one
// as well, which is coupling nobody would expect: editing the PostgreSQL file
// could break MySQL. It is a property of the SOURCE engine rather than of
// either target, so it lives in a file named for that.
//
// It is the fourth home this fact has had. Discovery has one, the retained row
// bound has one, and the canonical type work in internal/schema is where all of
// them are converging - see task #41. Until that lands, one shared copy here
// beats two adapters reaching into each other.

// sqlServerProjectedTextLengthLimit is the largest length each SQL Server text
// family can legally declare.
//
// char and varchar declare bytes and cap at 8000; nchar and nvarchar declare
// UTF-16 code units and cap at 4000, which is the same 8000 bytes of storage.
// Beyond either, SQL Server requires MAX and the column arrives as unbounded
// text instead.
//
// This is the third place the same two numbers appear - discovery refuses an
// over-long declaration in sqlServerTextLengthLimit, and the retained row bound
// refuses one again in sqlServerRetainedTextLengthLimit. Three copies of one
// fact is not a design; it is what a pairwise projection costs, and it is why
// an nvarchar(8000) that cannot exist was refused in two of the three and
// accepted here. Until the canonical type work removes the duplication, the
// copies are at least spelled the same way so a search finds all of them.
func sqlServerProjectedTextLengthLimit(base string) int {
	switch base {
	case "nchar", "nvarchar":
		return 4_000
	default:
		return 8_000
	}
}
