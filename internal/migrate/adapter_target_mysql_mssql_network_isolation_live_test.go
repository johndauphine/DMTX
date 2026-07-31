package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	mssql "github.com/microsoft/go-mssqldb"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4MySQLNetworkReplayTransactionFenceLive(t *testing.T) {
	testStage4MySQLFamilyNetworkReplayTransactionFenceLive(
		t,
		stage4MySQLNetworkLiveFixture{
			name:       "MySQL",
			dsnEnv:     "DMTX_TEST_MYSQL_TARGET_DSN",
			caEnv:      "DMTX_TEST_MYSQL_CA",
			adminEnv:   "DMTX_TEST_MYSQL_ADMIN_DSN",
			required:   "MYSQL_REQUIRED",
			tlsConfig:  "dmtx_test",
			flavor:     engine.MySQLServerFlavorOracle80,
			namePrefix: "dmtx_s4_mysql_fence_",
		},
	)
}

func TestStage4MariaDBNetworkReplayTransactionFenceLive(t *testing.T) {
	testStage4MySQLFamilyNetworkReplayTransactionFenceLive(
		t,
		stage4MySQLNetworkLiveFixture{
			name:       "MariaDB",
			dsnEnv:     "DMTX_TEST_MARIADB_TARGET_DSN",
			caEnv:      "DMTX_TEST_MARIADB_CA",
			required:   "MARIADB_REQUIRED",
			tlsConfig:  "dmtx_mariadb_test",
			flavor:     engine.MySQLServerFlavorMariaDB1011,
			namePrefix: "dmtx_s4_maria_fence_",
		},
	)
}

type stage4MySQLNetworkLiveFixture struct {
	name       string
	dsnEnv     string
	caEnv      string
	adminEnv   string
	required   string
	tlsConfig  string
	flavor     engine.MySQLServerFlavor
	namePrefix string
}

func testStage4MySQLFamilyNetworkReplayTransactionFenceLive(
	t *testing.T,
	fixture stage4MySQLNetworkLiveFixture,
) {
	t.Helper()
	values := stage4NetworkLiveEnvironment(
		t,
		fixture.required,
		fixture.dsnEnv,
		fixture.caEnv,
	)
	dsn, caPath := values[0], values[1]
	registerMySQLCommonFixtureTLSNamed(
		t,
		caPath,
		fixture.tlsConfig,
	)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		fixture.name+" Stage 4 target",
		dsn,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	writerDatabase := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		fixture.name+" Stage 4 writer",
		dsn,
		fixture.flavor == engine.MySQLServerFlavorOracle80,
	)
	stage4ConfigureMySQLFamilyTargetSession(
		t,
		ctx,
		writerDatabase,
		fixture,
	)
	ddlDatabase := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		fixture.name+" Stage 4 DDL contender",
		dsn,
		fixture.flavor == engine.MySQLServerFlavorOracle80,
	)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	parentName := fixture.namePrefix + suffix + "_parent"
	childName := fixture.namePrefix + suffix + "_child"
	auditName := fixture.namePrefix + suffix + "_audit"
	tableCollation := "utf8mb4_0900_bin"
	if fixture.flavor == engine.MySQLServerFlavorMariaDB1011 {
		tableCollation = "utf8mb4_nopad_bin"
	}
	uniqueCodeName := "uq_" + parentName + "_code"
	checkIDName := "ck_" + parentName + "_id_nonnegative"
	cleanupMySQLNativeTables(
		t,
		writerDatabase,
		childName,
		parentName,
		auditName,
	)
	parent := schema.Table{
		Schema:         parsed.DBName,
		Name:           parentName,
		MySQLCollation: tableCollation,
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name: "code",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{64},
				},
			},
			{
				Name: "payload",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{64},
				},
			},
		},
		Indexes: []schema.Index{
			{
				Name:   uniqueCodeName,
				Unique: true,
				Columns: []schema.IndexColumn{
					{Name: "code", Collation: "BINARY"},
				},
			},
		},
	}
	parentCheck, err := schema.ParseMySQLCatalogCheck(
		"`code` <> ''",
		parent.Columns,
	)
	if err != nil {
		t.Fatalf("plan %s target CHECK: %v", fixture.name, err)
	}
	parent.Checks = []schema.CheckConstraint{
		{Name: checkIDName, Expression: parentCheck},
	}
	parentQualified := mySQLQualified(parsed.DBName, parentName)
	childQualified := mySQLQualified(parsed.DBName, childName)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+parentQualified+" ("+
			"`id` BIGINT NOT NULL AUTO_INCREMENT, "+
			"`code` VARCHAR(64) NOT NULL, "+
			"`payload` VARCHAR(64) NOT NULL, "+
			"PRIMARY KEY (`id`), "+
			"UNIQUE KEY "+mySQLIdentifier(uniqueCodeName)+
			" (`code`), CONSTRAINT "+mySQLIdentifier(checkIDName)+
			" CHECK (`code` <> '')) "+
			"ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 "+
			"COLLATE="+tableCollation+" ROW_FORMAT=DYNAMIC",
	); err != nil {
		t.Fatalf("create %s parent: %v", fixture.name, err)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+childQualified+" ("+
			"`id` BIGINT NOT NULL, "+
			"`parent_id` BIGINT NULL, "+
			"`parent_code` VARCHAR(64) NULL, "+
			"PRIMARY KEY (`id`)) "+
			"ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 "+
			"COLLATE="+tableCollation+" ROW_FORMAT=DYNAMIC",
	); err != nil {
		t.Fatalf("create %s child: %v", fixture.name, err)
	}

	fenced := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWriter := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseWriter()
	provider := &stage4MySQLLiveBarrierProvider{
		mysqlSQLTransactionProvider: mysqlSQLTransactionProvider{
			database: writerDatabase,
		},
		fenced:  fenced,
		release: release,
	}
	writer := &mysqlNativeWriter{
		transactions: provider,
		flavor:       fixture.flavor,
	}
	writeDone := make(chan stage4NetworkLiveWriteResult, 1)
	go func() {
		receipt, err := writer.WriteStage4NetworkBatch(
			ctx,
			parent,
			[]string{"id", "code", "payload"},
			[][]any{{int64(1), "concurrent", "concurrent"}},
		)
		writeDone <- stage4NetworkLiveWriteResult{
			receipt: receipt,
			err:     err,
		}
	}()
	stage4AwaitFenceOrFailure(
		t,
		fixture.name,
		fenced,
		writeDone,
	)

	if _, err := ddlDatabase.ExecContext(
		ctx,
		"SET SESSION lock_wait_timeout = 1",
	); err != nil {
		t.Fatalf("set %s metadata lock timeout: %v", fixture.name, err)
	}
	unsafeName := "fk_" + childName + "_code"
	started := time.Now()
	_, ddlErr := ddlDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+mySQLIdentifier(unsafeName)+
			" FOREIGN KEY (`parent_code`) REFERENCES "+
			parentQualified+" (`code`) ON UPDATE CASCADE",
	)
	if ddlErr == nil {
		t.Fatalf(
			"%s unsafe FK DDL committed while the page transaction held its fence",
			fixture.name,
		)
	}
	var mysqlError *mysqlDriver.MySQLError
	if !errors.As(ddlErr, &mysqlError) ||
		mysqlError.Number != 1205 {
		t.Fatalf(
			"%s competing DDL error = %T, want lock timeout 1205",
			fixture.name,
			ddlErr,
		)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf(
			"%s competing DDL timeout took %s",
			fixture.name,
			elapsed,
		)
	}
	select {
	case result := <-writeDone:
		t.Fatalf(
			"%s writer crossed its proof barrier early: receipt=%+v err=%v",
			fixture.name,
			result.receipt,
			result.err,
		)
	default:
	}
	releaseWriter()
	result := stage4AwaitMySQLWriteResult(t, fixture.name, writeDone)
	assertMySQLNativeReceipt(
		t,
		result.receipt,
		CommitDurable,
		1,
		1,
	)
	if result.err != nil {
		t.Fatalf("%s fenced page write: %v", fixture.name, result.err)
	}
	stage4AssertMySQLForeignKeyAbsent(
		t,
		ctx,
		writerDatabase,
		parsed.DBName,
		unsafeName,
	)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"INSERT INTO "+childQualified+
			" (`id`, `parent_id`, `parent_code`) "+
			"VALUES (1, 1, 'concurrent')",
	); err != nil {
		t.Fatalf(
			"seed %s replay child after empty-target fenced page: %v",
			fixture.name,
			err,
		)
	}

	if _, err := writerDatabase.ExecContext(
		ctx,
		"UPDATE "+childQualified+
			" SET `parent_code` = 'concurrent' WHERE `id` = 1",
	); err != nil {
		t.Fatalf("align %s child before unsafe FK: %v", fixture.name, err)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+mySQLIdentifier(unsafeName)+
			" FOREIGN KEY (`parent_code`) REFERENCES "+
			parentQualified+" (`code`) ON UPDATE CASCADE",
	); err != nil {
		t.Fatalf("create %s unsafe existing FK: %v", fixture.name, err)
	}
	receipt, err := newMySQLNativeWriterForFlavor(
		writerDatabase,
		fixture.flavor,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "must_not_apply", "must_not_apply"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"ON UPDATE CASCADE on mutable column code",
	) {
		t.Fatalf("%s unsafe existing FK error = %v", fixture.name, err)
	}
	assertMySQLNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertMySQLParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"concurrent",
		"concurrent",
	)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" DROP FOREIGN KEY "+mySQLIdentifier(unsafeName),
	); err != nil {
		t.Fatalf("drop %s unsafe FK: %v", fixture.name, err)
	}

	safeName := "fk_" + childName + "_id"
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+mySQLIdentifier(safeName)+
			" FOREIGN KEY (`parent_id`) REFERENCES "+
			parentQualified+" (`id`) ON UPDATE CASCADE",
	); err != nil {
		t.Fatalf("create %s legal PK FK: %v", fixture.name, err)
	}
	receipt, err = newMySQLNativeWriterForFlavor(
		writerDatabase,
		fixture.flavor,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "legal", "legal"}},
	)
	if err != nil {
		t.Fatalf("%s legal PK-reference page: %v", fixture.name, err)
	}
	assertMySQLNativeReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
	stage4AssertMySQLParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"legal",
		"legal",
	)
	testStage4MySQLFamilyPoisonedTargetSessionLive(
		t,
		ctx,
		fixture,
		writerDatabase,
		parent,
		parentQualified,
	)

	auditQualified := mySQLQualified(parsed.DBName, auditName)
	triggerName := fixture.namePrefix + suffix + "_trigger"
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+auditQualified+
			" (`parent_id` BIGINT NOT NULL)",
	); err != nil {
		t.Fatalf(
			"create %s replay-trigger audit: %v",
			fixture.name,
			err,
		)
	}
	triggerStatement := "CREATE TRIGGER " +
		mySQLQualified(parsed.DBName, triggerName) +
		" AFTER UPDATE ON " + parentQualified +
		" FOR EACH ROW INSERT INTO " + auditQualified +
		" (`parent_id`) VALUES (NEW.`id`)"
	_, triggerErr := writerDatabase.ExecContext(
		ctx,
		triggerStatement,
	)
	var triggerServerError *mysqlDriver.MySQLError
	if triggerErr != nil &&
		fixture.adminEnv != "" &&
		errors.As(triggerErr, &triggerServerError) &&
		triggerServerError.Number == 1419 {
		adminDSN := os.Getenv(fixture.adminEnv)
		if adminDSN == "" {
			t.Fatalf(
				"set %s to create the %s binary-log-safe trigger sentinel",
				fixture.adminEnv,
				fixture.name,
			)
		}
		parseMySQLNativeTargetDSNForTLS(
			t,
			fixture.name+" Stage 4 trigger administrator",
			adminDSN,
			fixture.tlsConfig,
		)
		triggerDDLDatabase, openErr := sql.Open("mysql", adminDSN)
		if openErr != nil {
			t.Fatalf(
				"open %s Stage 4 trigger administrator: %v",
				fixture.name,
				openErr,
			)
		}
		t.Cleanup(func() {
			if err := triggerDDLDatabase.Close(); err != nil {
				t.Errorf(
					"close %s Stage 4 trigger administrator: %v",
					fixture.name,
					err,
				)
			}
		})
		if pingErr := triggerDDLDatabase.PingContext(ctx); pingErr != nil {
			t.Fatalf(
				"ping %s Stage 4 trigger administrator: %v",
				fixture.name,
				pingErr,
			)
		}
		_, triggerErr = triggerDDLDatabase.ExecContext(ctx, triggerStatement)
	}
	if triggerErr != nil {
		t.Fatalf(
			"create %s between-page replay trigger: %v",
			fixture.name,
			triggerErr,
		)
	}
	receipt, err = newMySQLNativeWriterForFlavor(
		writerDatabase,
		fixture.flavor,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "triggered", "triggered"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"replay-unsafe trigger",
	) {
		t.Fatalf(
			"%s between-page trigger replay error = %v",
			fixture.name,
			err,
		)
	}
	assertMySQLNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertMySQLParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"legal",
		"legal",
	)
	var auditRows int
	if err := writerDatabase.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+auditQualified,
	).Scan(&auditRows); err != nil {
		t.Fatalf("read %s replay-trigger audit: %v", fixture.name, err)
	}
	if auditRows != 0 {
		t.Fatalf(
			"%s rejected replay fired %d trigger side effects",
			fixture.name,
			auditRows,
		)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"DROP TRIGGER "+
			mySQLQualified(parsed.DBName, triggerName),
	); err != nil {
		t.Fatalf("drop %s replay trigger: %v", fixture.name, err)
	}
	testStage4MySQLFamilyRetainedShapeDriftLive(
		t,
		ctx,
		fixture,
		writerDatabase,
		parent,
		parentQualified,
	)
}

func stage4ConfigureMySQLFamilyTargetSession(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	fixture stage4MySQLNetworkLiveFixture,
) {
	t.Helper()
	var sqlMode string
	if err := database.QueryRowContext(
		ctx,
		"SELECT @@session.sql_mode",
	).Scan(&sqlMode); err != nil {
		t.Fatalf("read %s target SQL mode: %v", fixture.name, err)
	}
	hasNoAutoValueOnZero := false
	for _, mode := range strings.Split(sqlMode, ",") {
		if strings.EqualFold(
			strings.TrimSpace(mode),
			"NO_AUTO_VALUE_ON_ZERO",
		) {
			hasNoAutoValueOnZero = true
			break
		}
	}
	if !hasNoAutoValueOnZero {
		if sqlMode != "" {
			sqlMode += ","
		}
		sqlMode += "NO_AUTO_VALUE_ON_ZERO"
	}
	if _, err := database.ExecContext(
		ctx,
		"SET SESSION sql_mode = ?",
		sqlMode,
	); err != nil {
		t.Fatalf("configure %s target SQL mode: %v", fixture.name, err)
	}
	switch fixture.flavor {
	case engine.MySQLServerFlavorOracle80:
		if _, err := database.ExecContext(
			ctx,
			`SET SESSION
				foreign_key_checks = 1,
				unique_checks = 1`,
		); err != nil {
			t.Fatalf(
				"configure MySQL target constraint session: %v",
				err,
			)
		}
	case engine.MySQLServerFlavorMariaDB1011:
		if _, err := database.ExecContext(
			ctx,
			`SET SESSION
				foreign_key_checks = 1,
				unique_checks = 1,
				check_constraint_checks = 1`,
		); err != nil {
			t.Fatalf(
				"configure MariaDB target constraint session: %v",
				err,
			)
		}
	default:
		t.Fatalf("unsupported MySQL-family fixture flavor %d", fixture.flavor)
	}
}

func testStage4MySQLFamilyPoisonedTargetSessionLive(
	t *testing.T,
	ctx context.Context,
	fixture stage4MySQLNetworkLiveFixture,
	database *sql.DB,
	parent schema.Table,
	parentQualified string,
) {
	t.Helper()
	var (
		beforeRows     int
		beforeFrontier sql.NullInt64
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+parentQualified,
	).Scan(&beforeRows); err != nil {
		t.Fatalf("read %s rows before session poison: %v", fixture.name, err)
	}
	if err := database.QueryRowContext(
		ctx,
		`SELECT AUTO_INCREMENT
		   FROM information_schema.TABLES
		  WHERE TABLE_SCHEMA = ?
		    AND TABLE_NAME = ?`,
		parent.Schema,
		parent.Name,
	).Scan(&beforeFrontier); err != nil {
		t.Fatalf(
			"read %s identity frontier before session poison: %v",
			fixture.name,
			err,
		)
	}

	var (
		poisonID   int64
		poisonCode string
		want       string
		restore    func()
	)
	switch fixture.flavor {
	case engine.MySQLServerFlavorOracle80:
		var originalMode string
		if err := database.QueryRowContext(
			ctx,
			"SELECT @@session.sql_mode",
		).Scan(&originalMode); err != nil {
			t.Fatalf("read MySQL target SQL mode: %v", err)
		}
		modes := make([]string, 0)
		for _, mode := range strings.Split(originalMode, ",") {
			if !strings.EqualFold(
				strings.TrimSpace(mode),
				"NO_AUTO_VALUE_ON_ZERO",
			) {
				modes = append(modes, strings.TrimSpace(mode))
			}
		}
		if _, err := database.ExecContext(
			ctx,
			"SET SESSION sql_mode = ?",
			strings.Join(modes, ","),
		); err != nil {
			t.Fatalf("poison MySQL target SQL mode: %v", err)
		}
		restore = func() {
			if _, err := database.ExecContext(
				ctx,
				"SET SESSION sql_mode = ?",
				originalMode,
			); err != nil {
				t.Fatalf("restore MySQL target SQL mode: %v", err)
			}
		}
		poisonID = 0
		poisonCode = "session_poison"
		want = "NO_AUTO_VALUE_ON_ZERO"
	case engine.MySQLServerFlavorMariaDB1011:
		if _, err := database.ExecContext(
			ctx,
			"SET SESSION check_constraint_checks = 0",
		); err != nil {
			t.Fatalf("poison MariaDB target CHECK enforcement: %v", err)
		}
		restore = func() {
			if _, err := database.ExecContext(
				ctx,
				"SET SESSION check_constraint_checks = 1",
			); err != nil {
				t.Fatalf(
					"restore MariaDB target CHECK enforcement: %v",
					err,
				)
			}
		}
		poisonID = -1
		poisonCode = ""
		want = "constraint enforcement"
	default:
		t.Fatalf("unsupported MySQL-family fixture flavor %d", fixture.flavor)
	}
	defer restore()

	receipt, err := newMySQLNativeWriterForFlavor(
		database,
		fixture.flavor,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{poisonID, poisonCode, "session_poison"}},
	)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf(
			"%s poisoned target-session error = %v, want %q",
			fixture.name,
			err,
			want,
		)
	}
	assertMySQLNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)

	var (
		afterRows     int
		poisonRows    int
		afterFrontier sql.NullInt64
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*), SUM(`id` = ?) FROM "+parentQualified,
		poisonID,
	).Scan(&afterRows, &poisonRows); err != nil {
		t.Fatalf("read %s rows after session poison: %v", fixture.name, err)
	}
	if err := database.QueryRowContext(
		ctx,
		`SELECT AUTO_INCREMENT
		   FROM information_schema.TABLES
		  WHERE TABLE_SCHEMA = ?
		    AND TABLE_NAME = ?`,
		parent.Schema,
		parent.Name,
	).Scan(&afterFrontier); err != nil {
		t.Fatalf(
			"read %s identity frontier after session poison: %v",
			fixture.name,
			err,
		)
	}
	if afterRows != beforeRows ||
		poisonRows != 0 ||
		afterFrontier != beforeFrontier {
		t.Fatalf(
			"%s poisoned target session mutated rows/frontier: rows=%d/%d poison=%d frontier=%v/%v",
			fixture.name,
			afterRows,
			beforeRows,
			poisonRows,
			afterFrontier,
			beforeFrontier,
		)
	}
}

func testStage4MySQLFamilyRetainedShapeDriftLive(
	t *testing.T,
	ctx context.Context,
	fixture stage4MySQLNetworkLiveFixture,
	database *sql.DB,
	parent schema.Table,
	parentQualified string,
) {
	t.Helper()
	var (
		beforeTemporal time.Time
		beforeHistory  int
		wantError      string
	)
	switch fixture.flavor {
	case engine.MySQLServerFlavorOracle80:
		if _, err := database.ExecContext(
			ctx,
			"ALTER TABLE "+parentQualified+
				" ADD COLUMN `dmtx_replay_clock` TIMESTAMP(6) "+
				"NOT NULL DEFAULT CURRENT_TIMESTAMP(6) "+
				"ON UPDATE CURRENT_TIMESTAMP(6)",
		); err != nil {
			t.Fatalf("add MySQL replay-unsafe ON UPDATE column: %v", err)
		}
		if err := database.QueryRowContext(
			ctx,
			"SELECT `dmtx_replay_clock` FROM "+parentQualified+
				" WHERE `id` = 1",
		).Scan(&beforeTemporal); err != nil {
			t.Fatalf("read MySQL replay clock before rejection: %v", err)
		}
		// Ensure an accidental UPDATE would have an observably later clock.
		time.Sleep(10 * time.Millisecond)
		wantError = "column extra"
	case engine.MySQLServerFlavorMariaDB1011:
		if _, err := database.ExecContext(
			ctx,
			"ALTER TABLE "+parentQualified+" ADD SYSTEM VERSIONING",
		); err != nil {
			t.Fatalf("add MariaDB replay-unsafe system versioning: %v", err)
		}
		if err := database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+parentQualified+
				" FOR SYSTEM_TIME ALL",
		).Scan(&beforeHistory); err != nil {
			t.Fatalf("read MariaDB history before rejection: %v", err)
		}
		wantError = "not the planned InnoDB base table"
	default:
		t.Fatalf("unsupported MySQL-family fixture flavor %d", fixture.flavor)
	}

	receipt, err := newMySQLNativeWriterForFlavor(
		database,
		fixture.flavor,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "temporal", "temporal"}},
	)
	if err == nil || !strings.Contains(err.Error(), wantError) {
		t.Fatalf(
			"%s retained-shape temporal drift error = %v, want %q",
			fixture.name,
			err,
			wantError,
		)
	}
	assertMySQLNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertMySQLParentRow(
		t,
		ctx,
		database,
		parentQualified,
		"legal",
		"legal",
	)

	switch fixture.flavor {
	case engine.MySQLServerFlavorOracle80:
		var afterTemporal time.Time
		if err := database.QueryRowContext(
			ctx,
			"SELECT `dmtx_replay_clock` FROM "+parentQualified+
				" WHERE `id` = 1",
		).Scan(&afterTemporal); err != nil {
			t.Fatalf("read MySQL replay clock after rejection: %v", err)
		}
		if !afterTemporal.Equal(beforeTemporal) {
			t.Fatalf(
				"MySQL rejected replay changed ON UPDATE clock from %s to %s",
				beforeTemporal,
				afterTemporal,
			)
		}
	case engine.MySQLServerFlavorMariaDB1011:
		var afterHistory int
		if err := database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+parentQualified+
				" FOR SYSTEM_TIME ALL",
		).Scan(&afterHistory); err != nil {
			t.Fatalf("read MariaDB history after rejection: %v", err)
		}
		if afterHistory != beforeHistory {
			t.Fatalf(
				"MariaDB rejected replay created history rows: before=%d after=%d",
				beforeHistory,
				afterHistory,
			)
		}
	}
}

type stage4MySQLLiveBarrierProvider struct {
	mysqlSQLTransactionProvider
	fenced  chan<- struct{}
	release <-chan struct{}
}

func (provider *stage4MySQLLiveBarrierProvider) BeginStage4Network(
	ctx context.Context,
) (mysqlStage4NetworkBatchTransaction, error) {
	transaction, err :=
		provider.mysqlSQLTransactionProvider.BeginStage4Network(ctx)
	if err != nil {
		return nil, err
	}
	return &stage4MySQLLiveBarrierTransaction{
		mysqlStage4NetworkBatchTransaction: transaction,
		fenced:                             provider.fenced,
		release:                            provider.release,
	}, nil
}

type stage4MySQLLiveBarrierTransaction struct {
	mysqlStage4NetworkBatchTransaction
	fenced  chan<- struct{}
	release <-chan struct{}
}

func (transaction *stage4MySQLLiveBarrierTransaction) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	table schema.Table,
	flavor engine.MySQLServerFlavor,
) error {
	if err := transaction.mysqlStage4NetworkBatchTransaction.
		PreflightStage4NetworkReplayIsolation(
			ctx,
			table,
			flavor,
		); err != nil {
		return err
	}
	close(transaction.fenced)
	select {
	case <-transaction.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stage4AssertMySQLForeignKeyAbsent(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) {
	t.Helper()
	var count int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM information_schema.REFERENTIAL_CONSTRAINTS
		  WHERE CONSTRAINT_SCHEMA = ?
		    AND CONSTRAINT_NAME = ?`,
		namespace,
		name,
	).Scan(&count); err != nil {
		t.Fatalf("inspect competing MySQL FK: %v", err)
	}
	if count != 0 {
		t.Fatalf("competing MySQL FK %s exists", name)
	}
}

func stage4AssertMySQLParentRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	qualified string,
	wantCode string,
	wantPayload string,
) {
	t.Helper()
	var code, payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT `code`, `payload` FROM "+qualified+
			" WHERE `id` = 1",
	).Scan(&code, &payload); err != nil {
		t.Fatalf("read MySQL Stage 4 parent row: %v", err)
	}
	if code != wantCode || payload != wantPayload {
		t.Fatalf(
			"MySQL Stage 4 parent row = (%q, %q), want (%q, %q)",
			code,
			payload,
			wantCode,
			wantPayload,
		)
	}
}

func stage4AwaitMySQLWriteResult(
	t *testing.T,
	engineName string,
	results <-chan stage4NetworkLiveWriteResult,
) stage4NetworkLiveWriteResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(10 * time.Second):
		t.Fatalf("%s Stage 4 page did not finish", engineName)
		return stage4NetworkLiveWriteResult{}
	}
}

func TestStage4SQLServerNetworkReplayTransactionFenceLive(
	t *testing.T,
) {
	values := stage4NetworkLiveEnvironment(
		t,
		"MSSQL_REQUIRED",
		"DMTX_TEST_MSSQL_TARGET_DSN",
		"DMTX_TEST_MSSQL_CA",
	)
	endpoint := sqlServerCommonFixtureEndpoint(
		t,
		values[0],
		values[1],
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	writerDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"Stage 4 replay-fence writer",
		endpoint,
	)
	ddlDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"Stage 4 replay-fence DDL contender",
		endpoint,
	)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	parentName := "dmtx_s4_mssql_fence_" + suffix + "_parent"
	childName := "dmtx_s4_mssql_fence_" + suffix + "_child"
	auditName := "dmtx_s4_mssql_fence_" + suffix + "_audit"
	uniqueCodeName := "ux_" + parentName + "_code"
	cleanupSQLServerNativeTables(
		t,
		writerDatabase,
		childName,
		parentName,
		auditName,
	)
	parent := schema.Table{
		Schema: "dbo",
		Name:   parentName,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name: "code",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{64},
				},
			},
			{
				Name: "payload",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{64},
				},
			},
		},
		Indexes: []schema.Index{
			{
				Name:   uniqueCodeName,
				Unique: true,
				Columns: []schema.IndexColumn{
					{Name: "code"},
				},
			},
		},
	}
	parentQualified := sqlServerQualified("dbo", parentName)
	childQualified := sqlServerQualified("dbo", childName)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+parentQualified+" ("+
			"[id] BIGINT NOT NULL, "+
			"[code] VARCHAR(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL, "+
			"[payload] VARCHAR(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL, "+
			"PRIMARY KEY NONCLUSTERED ([id] DESC)); "+
			"CREATE UNIQUE NONCLUSTERED INDEX "+
			sqlServerIdentifier(uniqueCodeName)+" ON "+
			parentQualified+" ([code] ASC); "+
			"CREATE TABLE "+childQualified+" ("+
			"[id] BIGINT NOT NULL PRIMARY KEY, "+
			"[parent_id] BIGINT NULL, "+
			"[parent_code] VARCHAR(64) "+
			"COLLATE Latin1_General_100_BIN2_UTF8 NULL)",
	); err != nil {
		t.Fatalf("create SQL Server replay fixture: %v", err)
	}

	fenced := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWriter := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseWriter()
	provider := &stage4SQLServerLiveBarrierProvider{
		sqlServerSQLConnectionProvider: sqlServerSQLConnectionProvider{
			database: writerDatabase,
		},
		fenced:  fenced,
		release: release,
	}
	writer := &sqlServerNativeWriter{connections: provider}
	writeDone := make(chan stage4NetworkLiveWriteResult, 1)
	go func() {
		receipt, err := writer.WriteStage4NetworkBatch(
			ctx,
			parent,
			[]string{"id", "code", "payload"},
			[][]any{{int64(1), "concurrent", "concurrent"}},
		)
		writeDone <- stage4NetworkLiveWriteResult{
			receipt: receipt,
			err:     err,
		}
	}()
	stage4AwaitFenceOrFailure(
		t,
		"SQL Server",
		fenced,
		writeDone,
	)

	ddlConnection, err := ddlDatabase.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire SQL Server DDL contender: %v", err)
	}
	defer ddlConnection.Close()
	if _, err := ddlConnection.ExecContext(
		ctx,
		"SET LOCK_TIMEOUT 1000",
	); err != nil {
		t.Fatalf("set SQL Server DDL lock timeout: %v", err)
	}
	unsafeName := "fk_" + childName + "_code"
	started := time.Now()
	_, ddlErr := ddlConnection.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+sqlServerIdentifier(unsafeName)+
			" FOREIGN KEY ([parent_code]) REFERENCES "+
			parentQualified+" ([code]) ON UPDATE CASCADE",
	)
	if ddlErr == nil {
		t.Fatal(
			"SQL Server unsafe FK DDL committed while the page transaction held its fence",
		)
	}
	numbers := stage4SQLServerErrorNumbers(ddlErr)
	if !containsSQLServerErrorNumber(numbers, 1222) {
		t.Fatalf(
			"SQL Server competing DDL error = %T numbers %v, want lock timeout 1222",
			ddlErr,
			numbers,
		)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("SQL Server competing DDL timeout took %s", elapsed)
	}
	select {
	case result := <-writeDone:
		t.Fatalf(
			"SQL Server writer crossed its proof barrier early: receipt=%+v err=%v",
			result.receipt,
			result.err,
		)
	default:
	}
	releaseWriter()
	var result stage4NetworkLiveWriteResult
	select {
	case result = <-writeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("SQL Server Stage 4 page did not finish")
	}
	if result.err != nil {
		t.Fatalf("SQL Server fenced page write: %v", result.err)
	}
	assertSQLServerNativeReceipt(
		t,
		result.receipt,
		CommitDurable,
		1,
		1,
	)
	stage4AssertSQLServerForeignKeyAbsent(
		t,
		ctx,
		writerDatabase,
		unsafeName,
	)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"INSERT INTO "+childQualified+
			" ([id], [parent_id], [parent_code]) "+
			"VALUES (1, 1, 'concurrent')",
	); err != nil {
		t.Fatalf(
			"seed SQL Server child after empty-target fenced page: %v",
			err,
		)
	}

	if _, err := writerDatabase.ExecContext(
		ctx,
		"UPDATE "+childQualified+
			" SET [parent_code] = N'concurrent' WHERE [id] = 1",
	); err != nil {
		t.Fatalf("align SQL Server child before unsafe FK: %v", err)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+sqlServerIdentifier(unsafeName)+
			" FOREIGN KEY ([parent_code]) REFERENCES "+
			parentQualified+" ([code]) ON UPDATE CASCADE",
	); err != nil {
		t.Fatalf("create SQL Server unsafe existing FK: %v", err)
	}
	receipt, err := newSQLServerNativeWriter(
		writerDatabase,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "must_not_apply", "must_not_apply"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"ON UPDATE CASCADE on mutable column code",
	) {
		t.Fatalf("SQL Server unsafe existing FK error = %v", err)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertSQLServerParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"concurrent",
		"concurrent",
	)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" DROP CONSTRAINT "+sqlServerIdentifier(unsafeName),
	); err != nil {
		t.Fatalf("drop SQL Server unsafe FK: %v", err)
	}

	safeName := "fk_" + childName + "_id"
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+sqlServerIdentifier(safeName)+
			" FOREIGN KEY ([parent_id]) REFERENCES "+
			parentQualified+" ([id]) ON UPDATE CASCADE",
	); err != nil {
		t.Fatalf("create SQL Server legal PK FK: %v", err)
	}
	receipt, err = newSQLServerNativeWriter(
		writerDatabase,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "legal", "legal"}},
	)
	if err != nil {
		t.Fatalf("SQL Server legal PK-reference page: %v", err)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
	stage4AssertSQLServerParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"legal",
		"legal",
	)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" DROP CONSTRAINT "+sqlServerIdentifier(safeName),
	); err != nil {
		t.Fatalf(
			"drop SQL Server legal PK FK before metadata-deny sentinel: %v",
			err,
		)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"UPDATE "+childQualified+
			" SET [parent_code] = N'legal' WHERE [id] = 1",
	); err != nil {
		t.Fatalf(
			"align SQL Server child before metadata-deny sentinel: %v",
			err,
		)
	}
	testStage4SQLServerMetadataDenyLive(
		t,
		ctx,
		endpoint,
		writerDatabase,
		parent,
		parentQualified,
		childQualified,
		unsafeName,
		suffix,
	)

	auditQualified := sqlServerQualified("dbo", auditName)
	triggerName := "dmtx_s4_trigger_" + suffix
	triggerQualified := sqlServerQualified("dbo", triggerName)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+auditQualified+
			" ([parent_id] BIGINT NOT NULL)",
	); err != nil {
		t.Fatalf("create SQL Server replay-trigger audit: %v", err)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE TRIGGER "+triggerQualified+
			" ON "+parentQualified+
			" AFTER UPDATE AS BEGIN SET NOCOUNT ON; "+
			"INSERT INTO "+auditQualified+
			" ([parent_id]) SELECT [id] FROM inserted; END",
	); err != nil {
		t.Fatalf(
			"create SQL Server between-page replay trigger: %v",
			err,
		)
	}
	receipt, err = newSQLServerNativeWriter(
		writerDatabase,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "triggered", "triggered"}},
	)
	if err == nil || !strings.Contains(err.Error(), "DML triggers") {
		t.Fatalf(
			"SQL Server between-page trigger replay error = %v",
			err,
		)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertSQLServerParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"legal",
		"legal",
	)
	var auditRows int
	if err := writerDatabase.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+auditQualified,
	).Scan(&auditRows); err != nil {
		t.Fatalf("read SQL Server replay-trigger audit: %v", err)
	}
	if auditRows != 0 {
		t.Fatalf(
			"SQL Server rejected replay fired %d trigger side effects",
			auditRows,
		)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"DROP TRIGGER "+triggerQualified,
	); err != nil {
		t.Fatalf("drop SQL Server replay trigger: %v", err)
	}

	functionName := "dmtx_s4_rls_fn_" + suffix
	policyName := "dmtx_s4_rls_policy_" + suffix
	functionQualified := sqlServerQualified("dbo", functionName)
	policyQualified := sqlServerQualified("dbo", policyName)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE FUNCTION "+functionQualified+
			" (@id BIGINT) RETURNS TABLE WITH SCHEMABINDING "+
			"AS RETURN SELECT 1 AS [allowed] WHERE @id = @id",
	); err != nil {
		t.Fatalf("create SQL Server replay RLS function: %v", err)
	}
	if _, err := writerDatabase.ExecContext(
		ctx,
		"CREATE SECURITY POLICY "+policyQualified+
			" ADD FILTER PREDICATE "+functionQualified+
			"([id]) ON "+parentQualified+
			" WITH (STATE = ON)",
	); err != nil {
		t.Fatalf("create SQL Server between-page RLS policy: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		_, _ = writerDatabase.ExecContext(
			cleanupCtx,
			"DROP SECURITY POLICY IF EXISTS "+policyQualified,
		)
		_, _ = writerDatabase.ExecContext(
			cleanupCtx,
			"DROP FUNCTION IF EXISTS "+functionQualified,
		)
	})
	receipt, err = newSQLServerNativeWriter(
		writerDatabase,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "secured", "secured"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"row-level security predicates",
	) {
		t.Fatalf(
			"SQL Server between-page security replay error = %v",
			err,
		)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertSQLServerParentRow(
		t,
		ctx,
		writerDatabase,
		parentQualified,
		"legal",
		"legal",
	)
	if _, err := writerDatabase.ExecContext(
		ctx,
		"DROP SECURITY POLICY "+policyQualified+"; "+
			"DROP FUNCTION "+functionQualified,
	); err != nil {
		t.Fatalf("drop SQL Server replay RLS fixture: %v", err)
	}
	testStage4SQLServerRetainedIdentityDriftLive(
		t,
		ctx,
		writerDatabase,
		parent,
		parentQualified,
	)
}

func testStage4SQLServerMetadataDenyLive(
	t *testing.T,
	ctx context.Context,
	endpoint config.Endpoint,
	admin *sql.DB,
	parent schema.Table,
	parentQualified string,
	childQualified string,
	unsafeForeignKey string,
	suffix string,
) {
	t.Helper()
	if _, err := admin.ExecContext(
		ctx,
		"ALTER TABLE "+childQualified+
			" ADD CONSTRAINT "+sqlServerIdentifier(unsafeForeignKey)+
			" FOREIGN KEY ([parent_code]) REFERENCES "+
			parentQualified+" ([code]) ON UPDATE CASCADE",
	); err != nil {
		t.Fatalf("create SQL Server hidden unsafe FK: %v", err)
	}
	defer func() {
		if _, err := admin.ExecContext(
			ctx,
			"ALTER TABLE "+childQualified+
				" DROP CONSTRAINT "+
				sqlServerIdentifier(unsafeForeignKey),
		); err != nil {
			t.Fatalf("drop SQL Server hidden unsafe FK: %v", err)
		}
	}()

	principalName := "dmtx_s4_deny_" + suffix
	principal := sqlServerIdentifier(principalName)
	password := "DmtxS4!Aa1" + suffix
	var restricted *sql.DB
	cleanupPrincipal := func() {
		if restricted != nil {
			if err := restricted.Close(); err != nil {
				t.Errorf(
					"close SQL Server metadata-deny principal: %v",
					err,
				)
			}
			restricted = nil
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := admin.ExecContext(
			cleanupCtx,
			"IF USER_ID(N'"+principalName+"') IS NOT NULL "+
				"DROP USER "+principal+"; "+
				"IF SUSER_ID(N'"+principalName+"') IS NOT NULL "+
				"DROP LOGIN "+principal,
		); err != nil {
			t.Errorf(
				"drop SQL Server metadata-deny principal: %v",
				err,
			)
		}
	}
	t.Cleanup(cleanupPrincipal)
	if _, err := admin.ExecContext(
		ctx,
		"CREATE LOGIN "+principal+
			" WITH PASSWORD = N'"+password+
			"', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF; "+
			"CREATE USER "+principal+" FOR LOGIN "+principal+"; "+
			"GRANT CONNECT TO "+principal+"; "+
			"GRANT VIEW DEFINITION TO "+principal+"; "+
			"GRANT VIEW SECURITY DEFINITION TO "+principal+"; "+
			"GRANT SELECT, INSERT, UPDATE ON OBJECT::"+
			parentQualified+" TO "+principal+"; "+
			"DENY VIEW DEFINITION ON OBJECT::"+
			childQualified+" TO "+principal,
	); err != nil {
		t.Fatalf("create SQL Server metadata-deny principal: %v", err)
	}

	restrictedEndpoint := endpoint
	restrictedEndpoint.User = principalName
	restrictedEndpoint.Password = password
	var err error
	restricted, err = engine.OpenSQLServer(ctx, restrictedEndpoint)
	if err != nil {
		t.Fatalf("open SQL Server metadata-deny principal: %v", err)
	}
	restricted.SetMaxOpenConns(1)
	restricted.SetMaxIdleConns(1)
	receipt, err := newSQLServerNativeWriter(
		restricted,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(1), "denied", "denied"}},
	)
	if err == nil || !strings.Contains(err.Error(), "metadata DENY") {
		t.Fatalf("SQL Server overriding metadata DENY error = %v", err)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	stage4AssertSQLServerParentRow(
		t,
		ctx,
		admin,
		parentQualified,
		"legal",
		"legal",
	)
	if err := restricted.Close(); err != nil {
		t.Fatalf("close SQL Server metadata-deny connection: %v", err)
	}
	restricted = nil
}

func testStage4SQLServerRetainedIdentityDriftLive(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	parent schema.Table,
	parentQualified string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"ALTER TABLE "+parentQualified+
			" ADD [dmtx_replay_identity] BIGINT "+
			"IDENTITY(1,1) NOT NULL",
	); err != nil {
		t.Fatalf("add SQL Server target-only replay identity: %v", err)
	}
	objectName := parent.Schema + "." + parent.Name
	var beforeFrontier int64
	if err := database.QueryRowContext(
		ctx,
		"SELECT CONVERT(bigint, IDENT_CURRENT(@p1))",
		objectName,
	).Scan(&beforeFrontier); err != nil {
		t.Fatalf("read SQL Server replay identity frontier: %v", err)
	}
	receipt, err := newSQLServerNativeWriter(
		database,
	).WriteStage4NetworkBatch(
		ctx,
		parent,
		[]string{"id", "code", "payload"},
		[][]any{{int64(2), "identity", "identity"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"retained shape",
	) {
		t.Fatalf("SQL Server retained identity drift error = %v", err)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	var (
		insertedRows  int
		afterFrontier int64
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+parentQualified+" WHERE [id] = 2",
	).Scan(&insertedRows); err != nil {
		t.Fatalf("read SQL Server rejected identity row: %v", err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT CONVERT(bigint, IDENT_CURRENT(@p1))",
		objectName,
	).Scan(&afterFrontier); err != nil {
		t.Fatalf("read SQL Server replay identity after rejection: %v", err)
	}
	if insertedRows != 0 || afterFrontier != beforeFrontier {
		t.Fatalf(
			"SQL Server rejected replay mutated row/frontier: rows=%d frontier=%d/%d",
			insertedRows,
			afterFrontier,
			beforeFrontier,
		)
	}
}

type stage4SQLServerLiveBarrierProvider struct {
	sqlServerSQLConnectionProvider
	fenced  chan<- struct{}
	release <-chan struct{}
}

func (provider *stage4SQLServerLiveBarrierProvider) WithConnection(
	ctx context.Context,
	operation func(sqlServerNativeConnection) error,
) error {
	return provider.sqlServerSQLConnectionProvider.WithConnection(
		ctx,
		func(connection sqlServerNativeConnection) error {
			return operation(&stage4SQLServerLiveBarrierConnection{
				sqlServerNativeConnection: connection,
				fenced:                    provider.fenced,
				release:                   provider.release,
			})
		},
	)
}

type stage4SQLServerLiveBarrierConnection struct {
	sqlServerNativeConnection
	fenced  chan<- struct{}
	release <-chan struct{}
}

func (connection *stage4SQLServerLiveBarrierConnection) BeginSerializable(
	ctx context.Context,
) (sqlServerNativeTransaction, error) {
	transaction, err :=
		connection.sqlServerNativeConnection.BeginSerializable(ctx)
	if err != nil {
		return nil, err
	}
	stage4Transaction, ok :=
		transaction.(sqlServerStage4NetworkTransaction)
	if !ok {
		return nil, errors.New(
			"production SQL Server transaction lacks Stage 4 guard",
		)
	}
	return &stage4SQLServerLiveBarrierTransaction{
		sqlServerStage4NetworkTransaction: stage4Transaction,
		fenced:                            connection.fenced,
		release:                           connection.release,
	}, nil
}

type stage4SQLServerLiveBarrierTransaction struct {
	sqlServerStage4NetworkTransaction
	fenced  chan<- struct{}
	release <-chan struct{}
}

func (transaction *stage4SQLServerLiveBarrierTransaction) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	table schema.Table,
) error {
	if err := transaction.sqlServerStage4NetworkTransaction.
		PreflightStage4NetworkReplayIsolation(
			ctx,
			table,
		); err != nil {
		return err
	}
	close(transaction.fenced)
	select {
	case <-transaction.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stage4AssertSQLServerForeignKeyAbsent(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
) {
	t.Helper()
	var count int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM sys.foreign_keys
		  WHERE name = @p1`,
		name,
	).Scan(&count); err != nil {
		t.Fatalf("inspect competing SQL Server FK: %v", err)
	}
	if count != 0 {
		t.Fatalf("competing SQL Server FK %s exists", name)
	}
}

func stage4AssertSQLServerParentRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	qualified string,
	wantCode string,
	wantPayload string,
) {
	t.Helper()
	var code, payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT [code], [payload] FROM "+qualified+
			" WHERE [id] = 1",
	).Scan(&code, &payload); err != nil {
		t.Fatalf("read SQL Server Stage 4 parent row: %v", err)
	}
	if code != wantCode || payload != wantPayload {
		t.Fatalf(
			"SQL Server Stage 4 parent row = (%q, %q), want (%q, %q)",
			code,
			payload,
			wantCode,
			wantPayload,
		)
	}
}

func stage4NetworkLiveEnvironment(
	t *testing.T,
	requiredEnv string,
	names ...string,
) []string {
	t.Helper()
	values := make([]string, len(names))
	missing := make([]string, 0, len(names))
	for index, name := range names {
		values[index] = os.Getenv(name)
		if values[index] == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return values
	}
	required := strings.ToLower(strings.TrimSpace(
		os.Getenv(requiredEnv),
	))
	message := "set " + strings.Join(missing, ", ") +
		" to run the Stage 4 transaction-fence live test"
	if required == "1" || required == "true" ||
		required == "yes" || required == "on" {
		t.Fatal(message + "; " + requiredEnv + " requires this gate")
	}
	t.Skip(message)
	return nil
}

func stage4SQLServerErrorNumbers(err error) []int32 {
	var serverError mssql.Error
	if !errors.As(err, &serverError) {
		return nil
	}
	numbers := make([]int32, 0, len(serverError.All)+1)
	numbers = append(numbers, serverError.Number)
	for _, item := range serverError.All {
		if !containsSQLServerErrorNumber(numbers, item.Number) {
			numbers = append(numbers, item.Number)
		}
	}
	return numbers
}

func containsSQLServerErrorNumber(
	numbers []int32,
	want int32,
) bool {
	for _, number := range numbers {
		if number == want {
			return true
		}
	}
	return false
}

type stage4NetworkLiveWriteResult struct {
	receipt WriteReceipt
	err     error
}

func stage4AwaitFenceOrFailure[T any](
	t *testing.T,
	engineName string,
	fenced <-chan struct{},
	writeDone <-chan T,
) {
	t.Helper()
	select {
	case <-fenced:
	case result := <-writeDone:
		if writeResult, ok := any(result).(stage4NetworkLiveWriteResult); ok {
			t.Fatalf(
				"%s Stage 4 writer failed before its fence: receipt=%+v err=%v",
				engineName,
				writeResult.receipt,
				writeResult.err,
			)
		}
		t.Fatalf(
			"%s Stage 4 writer failed before its fence: %#v",
			engineName,
			result,
		)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s Stage 4 writer did not acquire its fence", engineName)
	}
}
