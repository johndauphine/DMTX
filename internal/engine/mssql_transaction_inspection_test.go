package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

var registerSQLServerCatalogConformanceDriver sync.Once

func TestInspectSQLServerTableWithQueryerRevalidatesExactConnection(
	t *testing.T,
) {
	registerSQLServerCatalogConformanceDriver.Do(func() {
		sql.Register(
			"dmtx_mssql_catalog_conformance",
			sqlServerCatalogConformanceDriver{},
		)
	})
	for _, target := range []bool{false, true} {
		name := "source"
		if target {
			name = "target"
		}
		t.Run(name, func(t *testing.T) {
			database, err := sql.Open(
				"dmtx_mssql_catalog_conformance",
				"unsafe-version",
			)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()

			inspect := func(queryer SQLServerCatalogQueryer) error {
				if target {
					_, err := InspectSQLServerTargetTableWithQueryer(
						context.Background(),
						queryer,
						"dbo",
						"events",
					)
					return err
				}
				_, err := InspectSQLServerTableWithQueryer(
					context.Background(),
					queryer,
					"dbo",
					"events",
				)
				return err
			}
			dbError := inspect(database)
			transaction, err := database.BeginTx(
				context.Background(),
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer transaction.Rollback()
			txError := inspect(transaction)
			if dbError == nil || txError == nil {
				t.Fatalf(
					"database error = %v, transaction error = %v",
					dbError,
					txError,
				)
			}
			if dbError.Error() != txError.Error() {
				t.Fatalf(
					"database and transaction validators differ:\nDB: %v\nTX: %v",
					dbError,
					txError,
				)
			}
			if !strings.Contains(dbError.Error(), "catalog version") {
				t.Fatalf(
					"exact-connection verifier error = %v, want catalog version",
					dbError,
				)
			}
		})
	}
}

type sqlServerCatalogConformanceDriver struct{}

func (sqlServerCatalogConformanceDriver) Open(
	name string,
) (driver.Conn, error) {
	return &sqlServerCatalogConformanceConnection{mode: name}, nil
}

type sqlServerCatalogConformanceConnection struct {
	mode string
}

func (*sqlServerCatalogConformanceConnection) Prepare(
	string,
) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}

func (*sqlServerCatalogConformanceConnection) Close() error {
	return nil
}

func (*sqlServerCatalogConformanceConnection) Begin() (
	driver.Tx,
	error,
) {
	return sqlServerCatalogConformanceTransaction{}, nil
}

func (connection *sqlServerCatalogConformanceConnection) QueryContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	if !strings.Contains(query, "SERVERPROPERTY('ProductMajorVersion')") {
		return nil, errors.New(
			"table discovery ran before exact-connection verification",
		)
	}
	productMajor := int64(17)
	productVersion := "17.0.1000.1"
	if connection.mode != "unsafe-version" {
		productMajor = 16
		productVersion = "16.0.4250.1"
	}
	return &sqlServerCatalogConformanceRows{
		columns: []string{
			"product_major_version",
			"engine_edition",
			"product_version",
			"edition",
			"database_name",
			"compatibility_level",
			"state",
			"user_access",
			"containment",
			"read_only",
			"auto_close",
			"auto_shrink",
			"standby",
			"source_database_id",
			"published",
			"subscribed",
			"merge_published",
			"distributor",
			"change_data_capture",
		},
		values: []driver.Value{
			productMajor,
			int64(3),
			productVersion,
			"Developer Edition (64-bit)",
			"dmtx_source",
			int64(160),
			"ONLINE",
			"MULTI_USER",
			"NONE",
			false,
			false,
			false,
			false,
			nil,
			false,
			false,
			false,
			false,
			false,
		},
	}, nil
}

var _ driver.QueryerContext = (*sqlServerCatalogConformanceConnection)(nil)

type sqlServerCatalogConformanceTransaction struct{}

func (sqlServerCatalogConformanceTransaction) Commit() error {
	return nil
}

func (sqlServerCatalogConformanceTransaction) Rollback() error {
	return nil
}

type sqlServerCatalogConformanceRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (rows *sqlServerCatalogConformanceRows) Columns() []string {
	return rows.columns
}

func (*sqlServerCatalogConformanceRows) Close() error {
	return nil
}

func (rows *sqlServerCatalogConformanceRows) Next(
	destinations []driver.Value,
) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	copy(destinations, rows.values)
	return nil
}
