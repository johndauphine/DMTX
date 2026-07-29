package migrate

import "github.com/johndauphine/dmtx/internal/schema"

func clonePostgresProjectionIndexes(source []schema.Index) []schema.Index {
	cloned := append([]schema.Index(nil), source...)
	for index := range cloned {
		cloned[index].Columns = append(
			[]schema.IndexColumn(nil),
			source[index].Columns...,
		)
	}
	return cloned
}

func clonePostgresProjectionForeignKeys(
	source []schema.ForeignKey,
) []schema.ForeignKey {
	cloned := append([]schema.ForeignKey(nil), source...)
	for index := range cloned {
		cloned[index].Columns = append(
			[]string(nil),
			source[index].Columns...,
		)
		cloned[index].ReferencedColumns = append(
			[]string(nil),
			source[index].ReferencedColumns...,
		)
	}
	return cloned
}
