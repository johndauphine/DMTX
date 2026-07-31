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

var registerMySQLCatalogConformanceDriver sync.Once

func TestInspectMySQLTableForFlavorUsesSameDBAndTransactionValidators(
	t *testing.T,
) {
	registerMySQLCatalogConformanceDriver.Do(func() {
		sql.Register(
			"dmtx_mysql_catalog_conformance",
			mysqlCatalogConformanceDriver{},
		)
	})
	for _, mode := range []string{
		"unsafe-version",
		"unexpected-shape",
	} {
		t.Run(mode, func(t *testing.T) {
			database, err := sql.Open(
				"dmtx_mysql_catalog_conformance",
				mode,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()

			transaction, err := database.BeginTx(
				context.Background(),
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer transaction.Rollback()

			dbError := inspectMySQLCatalogConformanceFailure(
				database,
			)
			txError := inspectMySQLCatalogConformanceFailure(
				transaction,
			)
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
			want := "catalog version"
			if mode == "unexpected-shape" {
				want = "expected 1 destination arguments in Scan, not 12"
			}
			if !strings.Contains(dbError.Error(), want) {
				t.Fatalf("error = %v, want %q", dbError, want)
			}
		})
	}
}

func TestVerifyMySQLTargetForFlavorUsesSameDBAndTransactionSessionValidators(
	t *testing.T,
) {
	registerMySQLCatalogConformanceDriver.Do(func() {
		sql.Register(
			"dmtx_mysql_catalog_conformance",
			mysqlCatalogConformanceDriver{},
		)
	})
	for _, mode := range []string{
		"unsafe-target-session",
		"unexpected-target-shape",
	} {
		t.Run(mode, func(t *testing.T) {
			database, err := sql.Open(
				"dmtx_mysql_catalog_conformance",
				mode,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			transaction, err := database.BeginTx(
				context.Background(),
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer transaction.Rollback()

			verify := func(queryer MySQLCatalogQueryer) error {
				return VerifyMySQLTargetForFlavor(
					context.Background(),
					queryer,
					MySQLServerFlavorOracle80,
				)
			}
			dbError := verify(database)
			txError := verify(transaction)
			if dbError == nil || txError == nil ||
				dbError.Error() != txError.Error() {
				t.Fatalf(
					"database error = %v, transaction error = %v",
					dbError,
					txError,
				)
			}
			want := "constraint enforcement"
			if mode == "unexpected-target-shape" {
				want = "expected 1 destination arguments in Scan, not 2"
			}
			if !strings.Contains(dbError.Error(), want) {
				t.Fatalf("error = %v, want %q", dbError, want)
			}
		})
	}
}

func inspectMySQLCatalogConformanceFailure(
	queryer MySQLCatalogQueryer,
) error {
	_, err := InspectMySQLTableForFlavor(
		context.Background(),
		queryer,
		MySQLServerFlavorOracle80,
		"app",
		"events",
	)
	return err
}

type mysqlCatalogConformanceDriver struct{}

func (mysqlCatalogConformanceDriver) Open(
	name string,
) (driver.Conn, error) {
	return &mysqlCatalogConformanceConnection{mode: name}, nil
}

type mysqlCatalogConformanceConnection struct {
	mode string
}

func (*mysqlCatalogConformanceConnection) Prepare(
	string,
) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}

func (*mysqlCatalogConformanceConnection) Close() error {
	return nil
}

func (connection *mysqlCatalogConformanceConnection) Begin() (
	driver.Tx,
	error,
) {
	return mysqlCatalogConformanceTransaction{}, nil
}

func (connection *mysqlCatalogConformanceConnection) QueryContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	if !strings.Contains(query, "@@innodb_page_size") {
		if strings.Contains(
			query,
			"sql_generate_invisible_primary_key",
		) {
			if connection.mode == "unexpected-target-shape" {
				return &mysqlCatalogConformanceRows{
					columns: []string{
						"sql_generate_invisible_primary_key",
					},
					values: []driver.Value{int64(0)},
				}, nil
			}
			return &mysqlCatalogConformanceRows{
				columns: []string{
					"sql_generate_invisible_primary_key",
					"sql_require_primary_key",
				},
				values: []driver.Value{int64(0), int64(0)},
			}, nil
		}
		return nil, errors.New("unexpected catalog query")
	}
	if connection.mode == "unexpected-shape" {
		return &mysqlCatalogConformanceRows{
			columns: []string{"version"},
			values:  []driver.Value{"8.0.46"},
		}, nil
	}
	version := "8.4.0"
	sqlMode := "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE," +
		"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
	foreignKeyChecks := int64(1)
	if connection.mode == "unsafe-target-session" ||
		connection.mode == "unexpected-target-shape" {
		version = "8.0.46"
		sqlMode += ",NO_AUTO_VALUE_ON_ZERO"
	}
	if connection.mode == "unsafe-target-session" {
		foreignKeyChecks = 0
	}
	return &mysqlCatalogConformanceRows{
		columns: []string{
			"version",
			"version_comment",
			"sql_mode",
			"session_time_zone",
			"system_time_zone",
			"auto_increment_increment",
			"auto_increment_offset",
			"lower_case_table_names",
			"explicit_timestamp_defaults",
			"foreign_key_checks",
			"unique_checks",
			"innodb_page_size",
		},
		values: []driver.Value{
			version,
			"MySQL Community Server - GPL",
			sqlMode,
			"+00:00",
			"UTC",
			int64(1),
			int64(1),
			int64(0),
			int64(1),
			foreignKeyChecks,
			int64(1),
			int64(16_384),
		},
	}, nil
}

var _ driver.QueryerContext = (*mysqlCatalogConformanceConnection)(nil)

type mysqlCatalogConformanceTransaction struct{}

func (mysqlCatalogConformanceTransaction) Commit() error {
	return nil
}

func (mysqlCatalogConformanceTransaction) Rollback() error {
	return nil
}

type mysqlCatalogConformanceRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (rows *mysqlCatalogConformanceRows) Columns() []string {
	return rows.columns
}

func (*mysqlCatalogConformanceRows) Close() error {
	return nil
}

func (rows *mysqlCatalogConformanceRows) Next(
	destinations []driver.Value,
) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	copy(destinations, rows.values)
	return nil
}
