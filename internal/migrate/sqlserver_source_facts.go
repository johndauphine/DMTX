package migrate

import "github.com/johndauphine/dmtx/internal/schema"

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
// The numbers are no longer written here. They are schema.SQLServerTextLengthLimit,
// which is the one home this fact has - the same constants discovery reads and
// the same ones the target vocabulary multiplies by four going the other way.
// Three copies of one fact was what let an nvarchar(8000) that cannot exist be
// refused in two of them and accepted in the third.
//
// The wrapper survives the move because callers in this package pass a base
// string and want an int, and converting at each of them would spread the
// conversion instead of the constant.
func sqlServerProjectedTextLengthLimit(base string) int {
	return int(schema.SQLServerTextLengthLimit(base))
}
