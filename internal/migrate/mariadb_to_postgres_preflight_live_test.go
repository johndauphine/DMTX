package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

func TestMariaDBToPostgresRejectsLegacySourceValuesBeforeMutationLive(
	t *testing.T,
) {
	mysqlDSN := os.Getenv("DMTX_TEST_MARIADB_DSN")
	mysqlCA := os.Getenv("DMTX_TEST_MARIADB_CA")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if mysqlDSN == "" || mysqlCA == "" || postgresDSN == "" {
		t.Skip(
			"set DMTX_TEST_MARIADB_DSN, DMTX_TEST_MARIADB_CA, " +
				"and DMTX_TEST_POSTGRES_DSN to run MariaDB " +
				"preflight live tests",
		)
	}
	const mariaDBTLSConfig = "dmtx_mariadb_test"
	registerMySQLCommonFixtureTLSNamed(
		t,
		mysqlCA,
		mariaDBTLSConfig,
	)
	mysqlConfig, err := mysqlDriver.ParseDSN(mysqlDSN)
	if err != nil {
		t.Fatalf("parse MariaDB preflight DSN: %v", err)
	}
	if mysqlConfig.TLSConfig != mariaDBTLSConfig {
		t.Fatal("DMTX_TEST_MARIADB_DSN must use verified TLS")
	}
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL preflight DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	host, rawPort, err := net.SplitHostPort(mysqlConfig.Addr)
	if err != nil {
		t.Fatalf("parse MariaDB preflight address: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse MariaDB preflight port: %v", err)
	}
	sourceEndpoint := config.Endpoint{
		Type:      "mysql",
		Host:      host,
		Port:      port,
		Database:  mysqlConfig.DBName,
		User:      mysqlConfig.User,
		Password:  mysqlConfig.Passwd,
		Schema:    mysqlConfig.DBName,
		SSLMode:   "verify-full",
		TLSCAFile: mysqlCA,
	}
	targetEndpoint := config.Endpoint{
		Type:     "postgres",
		Host:     postgresConfig.Host,
		Port:     int(postgresConfig.Port),
		Database: postgresConfig.Database,
		User:     postgresConfig.User,
		Password: postgresConfig.Password,
		Schema: "dmtx_maria_preflight_" +
			strconv.FormatInt(time.Now().UnixNano(), 36),
		SSLMode: "require",
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open MariaDB preflight fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close MariaDB preflight fixture: %v", err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify MariaDB preflight fixture: %v", err)
	}

	t.Run("zero temporal value", func(t *testing.T) {
		name := mariaDBPreflightLiveTableName("temporal")
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			name,
			`(
				id BIGINT NOT NULL,
				observed_on DATE NULL,
				PRIMARY KEY (id)
			)`,
		)
		insertMariaDBPermissiveSQLMode(
			t,
			ctx,
			sourceDatabase,
			"INSERT INTO "+mySQLIdentifier(name)+
				" (id, observed_on) VALUES (1, '0000-00-00')",
		)
		assertMariaDBPostgresPreflightRejectsBeforeMutation(
			t,
			ctx,
			sourceEndpoint,
			targetEndpoint,
			[]string{name},
			"temporal",
		)
	})

	t.Run("invalid JSON alias value", func(t *testing.T) {
		name := mariaDBPreflightLiveTableName("json")
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			name,
			`(
				id BIGINT NOT NULL,
				document JSON NULL,
				PRIMARY KEY (id)
			)`,
		)
		insertMariaDBChecksDisabled(
			t,
			ctx,
			sourceDatabase,
			"INSERT INTO "+mySQLIdentifier(name)+
				" (id, document) VALUES (1, '{invalid')",
		)
		assertMariaDBPostgresPreflightRejectsBeforeMutation(
			t,
			ctx,
			sourceEndpoint,
			targetEndpoint,
			[]string{name},
			"JSON",
		)
	})

	t.Run("embedded text NUL", func(t *testing.T) {
		name := mariaDBPreflightLiveTableName("nul")
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			name,
			`(
				id BIGINT NOT NULL,
				note VARCHAR(80) NULL,
				PRIMARY KEY (id)
			)`,
		)
		if _, err := sourceDatabase.ExecContext(
			ctx,
			"INSERT INTO "+mySQLIdentifier(name)+
				" (id, note) VALUES (1, CONCAT('left', CHAR(0), 'right'))",
		); err != nil {
			t.Fatalf("insert MariaDB embedded-NUL fixture: %v", err)
		}
		assertMariaDBPostgresPreflightRejectsBeforeMutation(
			t,
			ctx,
			sourceEndpoint,
			targetEndpoint,
			[]string{name},
			"embedded NUL",
		)
	})

	t.Run("historical CHECK violation", func(t *testing.T) {
		name := mariaDBPreflightLiveTableName("check")
		checkName := name + "_positive_ck"
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			name,
			fmt.Sprintf(
				`(
					id BIGINT NOT NULL,
					amount DECIMAL(12,2) NOT NULL,
					PRIMARY KEY (id),
					CONSTRAINT %s CHECK (amount >= 0)
				)`,
				mySQLIdentifier(checkName),
			),
		)
		insertMariaDBChecksDisabled(
			t,
			ctx,
			sourceDatabase,
			"INSERT INTO "+mySQLIdentifier(name)+
				" (id, amount) VALUES (1, -1.00)",
		)
		assertMariaDBPostgresPreflightRejectsBeforeMutation(
			t,
			ctx,
			sourceEndpoint,
			targetEndpoint,
			[]string{name},
			"CHECK "+checkName+" is violated",
		)
	})

	t.Run("historical empty-string CHECK violation", func(t *testing.T) {
		name := mariaDBPreflightLiveTableName("empty_check")
		checkName := name + "_nonempty_ck"
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			name,
			fmt.Sprintf(
				`(
					id BIGINT NOT NULL,
					code VARCHAR(24) NOT NULL,
					PRIMARY KEY (id),
					CONSTRAINT %s CHECK (code <> '')
				)`,
				mySQLIdentifier(checkName),
			),
		)
		insertMariaDBChecksDisabled(
			t,
			ctx,
			sourceDatabase,
			"INSERT INTO "+mySQLIdentifier(name)+
				" (id, code) VALUES (1, '')",
		)
		assertMariaDBPostgresPreflightRejectsBeforeMutation(
			t,
			ctx,
			sourceEndpoint,
			targetEndpoint,
			[]string{name},
			"CHECK "+checkName+" is violated",
		)
	})

	t.Run("historical foreign key orphan", func(t *testing.T) {
		parentName := mariaDBPreflightLiveTableName("parent")
		childName := mariaDBPreflightLiveTableName("child")
		foreignKeyName := childName + "_parent_fk"
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			parentName,
			`(
				id BIGINT NOT NULL,
				PRIMARY KEY (id)
			)`,
		)
		createMariaDBPreflightLiveTable(
			t,
			ctx,
			sourceDatabase,
			childName,
			fmt.Sprintf(
				`(
					id BIGINT NOT NULL,
					parent_id BIGINT NULL,
					PRIMARY KEY (id),
					CONSTRAINT %s FOREIGN KEY (parent_id)
						REFERENCES %s (id)
						ON UPDATE CASCADE
						ON DELETE RESTRICT
				)`,
				mySQLIdentifier(foreignKeyName),
				mySQLIdentifier(parentName),
			),
		)
		insertMariaDBForeignKeysDisabled(
			t,
			ctx,
			sourceDatabase,
			"INSERT INTO "+mySQLIdentifier(childName)+
				" (id, parent_id) VALUES (1, 404)",
		)
		assertMariaDBPostgresPreflightRejectsBeforeMutation(
			t,
			ctx,
			sourceEndpoint,
			targetEndpoint,
			[]string{parentName, childName},
			"foreign key "+foreignKeyName+" has orphan rows",
		)
	})
}

func TestMariaDBUnsafeEmptyStringModeFailsBeforeTargetMutationLive(
	t *testing.T,
) {
	mysqlDSN := os.Getenv("DMTX_TEST_MARIADB_DSN")
	mysqlCA := os.Getenv("DMTX_TEST_MARIADB_CA")
	if mysqlDSN == "" || mysqlCA == "" {
		t.Skip(
			"set DMTX_TEST_MARIADB_DSN and DMTX_TEST_MARIADB_CA " +
				"to run MariaDB SQL-mode preflight tests",
		)
	}
	const mariaDBTLSConfig = "dmtx_mariadb_test"
	registerMySQLCommonFixtureTLSNamed(
		t,
		mysqlCA,
		mariaDBTLSConfig,
	)
	mysqlConfig, err := mysqlDriver.ParseDSN(mysqlDSN)
	if err != nil {
		t.Fatalf("parse MariaDB SQL-mode DSN: %v", err)
	}
	if mysqlConfig.TLSConfig != mariaDBTLSConfig ||
		mysqlConfig.DBName == "" {
		t.Fatal("DMTX_TEST_MARIADB_DSN must select a database over verified TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	fixtureDatabase, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open MariaDB SQL-mode fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := fixtureDatabase.Close(); err != nil {
			t.Errorf("close MariaDB SQL-mode fixture: %v", err)
		}
	})
	if err := fixtureDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify MariaDB SQL-mode fixture: %v", err)
	}
	name := mariaDBPreflightLiveTableName("empty_mode")
	createMariaDBPreflightLiveTable(
		t,
		ctx,
		fixtureDatabase,
		name,
		`(
			id BIGINT NOT NULL,
			code VARCHAR(24) NOT NULL,
			PRIMARY KEY (id)
		)`,
	)

	unsafeConfig := *mysqlConfig
	unsafeConfig.Params = make(
		map[string]string,
		len(mysqlConfig.Params)+1,
	)
	for key, value := range mysqlConfig.Params {
		unsafeConfig.Params[key] = value
	}
	unsafeConfig.Params["sql_mode"] = "'" + strings.Join(
		[]string{
			"STRICT_TRANS_TABLES",
			"NO_ZERO_IN_DATE",
			"NO_ZERO_DATE",
			"ERROR_FOR_DIVISION_BY_ZERO",
			"NO_ENGINE_SUBSTITUTION",
			"EMPTY_STRING_IS_NULL",
		},
		",",
	) + "'"
	unsafeDatabase, err := sql.Open("mysql", unsafeConfig.FormatDSN())
	if err != nil {
		t.Fatalf("open MariaDB unsafe SQL-mode connection: %v", err)
	}
	unsafeDatabase.SetMaxOpenConns(1)
	unsafeDatabase.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := unsafeDatabase.Close(); err != nil {
			t.Errorf("close MariaDB unsafe SQL-mode connection: %v", err)
		}
	})
	if err := unsafeDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify MariaDB unsafe SQL-mode connection: %v", err)
	}
	var sqlMode string
	if err := unsafeDatabase.QueryRowContext(
		ctx,
		"SELECT @@session.sql_mode",
	).Scan(&sqlMode); err != nil {
		t.Fatalf("read MariaDB unsafe SQL mode: %v", err)
	}
	if !mysqlSQLModeContains(sqlMode, "EMPTY_STRING_IS_NULL") {
		t.Fatalf("unsafe MariaDB SQL mode was not enabled: %q", sqlMode)
	}

	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine:       "mysql",
			displayName:  "MySQL/MariaDB",
			listTables:   engine.ListMySQLTables,
			inspectTable: engine.InspectMySQLTable,
		},
		database:  unsafeDatabase,
		namespace: mysqlConfig.DBName,
	}
	events := make([]string, 0)
	target := &recordingAdapterTarget{events: &events}
	observer := &rejectMariaDBPreflightMutationObserver{}
	result, err := migrateWithAdapters(
		ctx,
		config.Config{Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{name},
		}},
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "EMPTY_STRING_IS_NULL") {
		t.Fatalf("result = %#v, SQL-mode error = %v", result, err)
	}
	if result != (Result{}) ||
		len(target.planned) != 0 ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 ||
		observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 ||
		observer.mutations != 0 {
		t.Fatalf(
			"target activity before SQL-mode rejection: result=%#v plan=%v prepare=%v write=%v observer=%#v",
			result,
			target.planned,
			target.prepared,
			target.written,
			observer,
		)
	}
}

func mysqlSQLModeContains(value, wanted string) bool {
	for _, mode := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(mode), wanted) {
			return true
		}
	}
	return false
}

type rejectMariaDBPreflightMutationObserver struct {
	beforeSets int
	before     int
	after      int
	mutations  int
}

func (observer *rejectMariaDBPreflightMutationObserver) BeforeTables(
	context.Context,
	[]string,
) error {
	observer.beforeSets++
	return nil
}

func (observer *rejectMariaDBPreflightMutationObserver) BeforeTable(
	context.Context,
	string,
) error {
	observer.before++
	return nil
}

func (observer *rejectMariaDBPreflightMutationObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.after++
	return nil
}

func (observer *rejectMariaDBPreflightMutationObserver) ProtectTargetMutation(
	context.Context,
	func() error,
) error {
	observer.mutations++
	return errors.New("unexpected target mutation")
}

func assertMariaDBPostgresPreflightRejectsBeforeMutation(
	t *testing.T,
	ctx context.Context,
	source config.Endpoint,
	target config.Endpoint,
	tables []string,
	want string,
) {
	t.Helper()
	observer := &rejectMariaDBPreflightMutationObserver{}
	result, err := MySQLToPostgresWithObserver(
		ctx,
		config.Config{
			Source: source,
			Target: target,
			Migration: config.Migration{
				TargetMode:    "drop_recreate",
				IncludeTables: tables,
			},
		},
		observer,
	)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("result = %#v, preflight error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial preflight result = %#v", result)
	}
	if observer.mutations != 0 {
		t.Fatalf(
			"target mutation boundary reached %d times before %s rejection",
			observer.mutations,
			want,
		)
	}
	if observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 {
		t.Fatalf(
			"checkpoint callbacks ran before %s rejection: table_sets=%d before=%d after=%d",
			want,
			observer.beforeSets,
			observer.before,
			observer.after,
		)
	}
}

func mariaDBPreflightLiveTableName(kind string) string {
	return "dmtx_maria_pf_" +
		kind + "_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
}

func createMariaDBPreflightLiveTable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
	definition string,
) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+mySQLIdentifier(name),
		); err != nil {
			t.Errorf("drop MariaDB preflight table %s: %v", name, err)
		}
	})
	statement := fmt.Sprintf(
		"CREATE TABLE %s %s ENGINE=InnoDB "+
			"DEFAULT CHARACTER SET=utf8mb4 "+
			"COLLATE=utf8mb4_nopad_bin ROW_FORMAT=DYNAMIC",
		mySQLIdentifier(name),
		definition,
	)
	if _, err := database.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create MariaDB preflight table %s: %v", name, err)
	}
}

func insertMariaDBPermissiveSQLMode(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve MariaDB permissive connection: %v", err)
	}
	defer connection.Close()
	var original string
	if err := connection.QueryRowContext(
		ctx,
		"SELECT @@session.sql_mode",
	).Scan(&original); err != nil {
		t.Fatalf("read MariaDB session SQL mode: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		"SET SESSION sql_mode = ''",
	); err != nil {
		t.Fatalf("disable MariaDB strict SQL mode: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(
			context.Background(),
			"SET SESSION sql_mode = ?",
			original,
		); err != nil {
			t.Errorf("restore MariaDB session SQL mode: %v", err)
		}
	}()
	if _, err := connection.ExecContext(ctx, statement); err != nil {
		t.Fatalf("insert permissive MariaDB fixture: %v", err)
	}
}

func insertMariaDBChecksDisabled(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve MariaDB CHECK-disabled connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		"SET SESSION check_constraint_checks = OFF",
	); err != nil {
		t.Fatalf("disable MariaDB CHECK constraints: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(
			context.Background(),
			"SET SESSION check_constraint_checks = ON",
		); err != nil {
			t.Errorf("restore MariaDB CHECK constraints: %v", err)
		}
	}()
	if _, err := connection.ExecContext(ctx, statement); err != nil {
		t.Fatalf("insert CHECK-disabled MariaDB fixture: %v", err)
	}
}

func insertMariaDBForeignKeysDisabled(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve MariaDB FK-disabled connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		"SET SESSION foreign_key_checks = OFF",
	); err != nil {
		t.Fatalf("disable MariaDB foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(
			context.Background(),
			"SET SESSION foreign_key_checks = ON",
		); err != nil {
			t.Errorf("restore MariaDB foreign keys: %v", err)
		}
	}()
	if _, err := connection.ExecContext(ctx, statement); err != nil {
		t.Fatalf("insert FK-disabled MariaDB fixture: %v", err)
	}
}
