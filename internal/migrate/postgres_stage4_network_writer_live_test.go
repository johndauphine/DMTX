package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresStage4NetworkWriterIncomingForeignKeyLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL Stage 4 network replay-fence sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL Stage 4 replay DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 replay target: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL Stage 4 replay target: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL Stage 4 replay target: %T", err)
	}
	var tlsActive bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect PostgreSQL Stage 4 replay TLS: %v", err)
	}
	if !tlsActive {
		t.Fatal("PostgreSQL Stage 4 replay target established a non-TLS session")
	}

	namespace := "dmtx_s4_replay_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL Stage 4 replay schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL Stage 4 replay schema: %v", err)
		}
	})
	parents := postgresQualified(namespace, "parents")
	children := postgresQualified(namespace, "external_children")
	if _, err := database.ExecContext(
		ctx,
		`CREATE TABLE `+parents+` (
			"id" integer NOT NULL PRIMARY KEY,
			"code" text NOT NULL UNIQUE
		);
		CREATE TABLE `+children+` (
			"id" integer NOT NULL PRIMARY KEY,
			"parent_code" text,
			CONSTRAINT "children_parent_code_fkey"
				FOREIGN KEY ("parent_code")
				REFERENCES `+parents+` ("code")
				ON UPDATE CASCADE
		);
		INSERT INTO `+parents+` ("id", "code") VALUES (1, 'old');
		INSERT INTO `+children+` ("id", "parent_code") VALUES (1, 'old')`,
	); err != nil {
		t.Fatalf("create PostgreSQL unsafe replay fixture: %v", err)
	}
	table := schema.Table{
		Schema: namespace,
		Name:   "parents",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "code", Type: "text"},
		},
	}
	writer := newPostgresNativeWriter(database)
	receipt, err := writer.WriteStage4NetworkBatch(
		ctx,
		table,
		[]string{"id", "code"},
		[][]any{{int64(1), "unsafe"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"ON UPDATE CASCADE on mutable column code",
	) {
		t.Fatalf("unsafe PostgreSQL incoming-FK error = %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	var parentCode, childCode string
	if err := database.QueryRowContext(
		ctx,
		"SELECT code FROM "+parents+" WHERE id = 1",
	).Scan(&parentCode); err != nil {
		t.Fatalf("read rejected PostgreSQL parent: %v", err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT parent_code FROM "+children+" WHERE id = 1",
	).Scan(&childCode); err != nil {
		t.Fatalf("read rejected PostgreSQL child: %v", err)
	}
	if parentCode != "old" || childCode != "old" {
		t.Fatalf(
			"rejected PostgreSQL write changed rows: parent=%q child=%q",
			parentCode,
			childCode,
		)
	}

	if _, err := database.ExecContext(
		ctx,
		`DROP TABLE `+children+`;
		CREATE TABLE `+children+` (
			"id" integer NOT NULL PRIMARY KEY,
			"parent_id" integer,
			CONSTRAINT "children_parent_id_fkey"
				FOREIGN KEY ("parent_id")
				REFERENCES `+parents+` ("id")
				ON UPDATE CASCADE
		);
		INSERT INTO `+children+` ("id", "parent_id") VALUES (1, 1)`,
	); err != nil {
		t.Fatalf("create PostgreSQL legal replay fixture: %v", err)
	}
	receipt, err = writer.WriteStage4NetworkBatch(
		ctx,
		table,
		[]string{"id", "code"},
		[][]any{{int64(1), "safe"}},
	)
	if err != nil {
		t.Fatalf("legal PostgreSQL Stage 4 network write: %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT code FROM "+parents+" WHERE id = 1",
	).Scan(&parentCode); err != nil {
		t.Fatalf("read committed PostgreSQL parent: %v", err)
	}
	if parentCode != "safe" {
		t.Fatalf(
			"committed PostgreSQL parent code = %q, want safe",
			parentCode,
		)
	}

	audit := postgresQualified(namespace, "parent_update_audit")
	function := postgresQualified(namespace, "audit_parent_update")
	trigger := "dmtx_parent_update_audit"
	if _, err := database.ExecContext(
		ctx,
		`CREATE TABLE `+audit+` ("parent_id" integer NOT NULL);
		 CREATE FUNCTION `+function+`() RETURNS trigger
		 LANGUAGE plpgsql
		 AS $dmtx$
		 BEGIN
		   INSERT INTO `+audit+` ("parent_id") VALUES (NEW."id");
		   RETURN NEW;
		 END
		 $dmtx$;
		 CREATE TRIGGER `+postgresIdentifier(trigger)+`
		 AFTER UPDATE ON `+parents+`
		 FOR EACH ROW EXECUTE FUNCTION `+function+`()`,
	); err != nil {
		t.Fatalf(
			"create PostgreSQL between-page replay trigger: %v",
			err,
		)
	}
	receipt, err = writer.WriteStage4NetworkBatch(
		ctx,
		table,
		[]string{"id", "code"},
		[][]any{{int64(1), "triggered"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"non-internal user triggers",
	) {
		t.Fatalf(
			"PostgreSQL between-page trigger replay error = %v",
			err,
		)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT code FROM "+parents+" WHERE id = 1",
	).Scan(&parentCode); err != nil {
		t.Fatalf("read PostgreSQL parent after trigger rejection: %v", err)
	}
	var auditRows int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+audit,
	).Scan(&auditRows); err != nil {
		t.Fatalf("read PostgreSQL replay-trigger audit: %v", err)
	}
	if parentCode != "safe" || auditRows != 0 {
		t.Fatalf(
			"rejected PostgreSQL replay changed target: parent=%q audit=%d",
			parentCode,
			auditRows,
		)
	}
}

func TestPostgresStage4NetworkWriterFenceBlocksConcurrentUnsafeForeignKeyDDLLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL Stage 4 concurrent-DDL replay-fence sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL Stage 4 concurrent-DDL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 concurrent-DDL target: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL concurrent-DDL target: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL Stage 4 concurrent-DDL target: %T", err)
	}
	ddlDatabase, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 concurrent-DDL session: %T", err)
	}
	t.Cleanup(func() {
		if err := ddlDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL concurrent-DDL session: %v", err)
		}
	})

	namespace := "dmtx_s4_ddl_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL concurrent-DDL schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL concurrent-DDL schema: %v", err)
		}
	})
	parents := postgresQualified(namespace, "parents")
	children := postgresQualified(namespace, "external_children")
	if _, err := database.ExecContext(
		ctx,
		`CREATE TABLE `+parents+` (
			"id" integer NOT NULL PRIMARY KEY,
			"code" text NOT NULL UNIQUE
		);
		CREATE TABLE `+children+` (
			"id" integer NOT NULL PRIMARY KEY,
			"parent_code" text
		);
		INSERT INTO `+parents+` ("id", "code") VALUES (1, 'old');
		INSERT INTO `+children+` ("id", "parent_code") VALUES (1, 'old')`,
	); err != nil {
		t.Fatalf("create PostgreSQL concurrent-DDL fixture: %v", err)
	}
	table := schema.Table{
		Schema: namespace,
		Name:   "parents",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "code", Type: "text"},
		},
	}
	proofComplete := make(chan struct{})
	releaseWrite := make(chan struct{})
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		close(releaseWrite)
	}
	defer release()
	writer := &postgresNativeWriter{
		connections: postgresStage4PausingPGXProvider{
			database:      database,
			proofComplete: proofComplete,
			releaseWrite:  releaseWrite,
		},
	}
	type writeResult struct {
		receipt WriteReceipt
		err     error
	}
	writeResults := make(chan writeResult, 1)
	go func() {
		receipt, writeErr := writer.WriteStage4NetworkBatch(
			context.Background(),
			table,
			[]string{"id", "code"},
			[][]any{{int64(1), "new"}},
		)
		writeResults <- writeResult{
			receipt: receipt,
			err:     writeErr,
		}
	}()
	select {
	case <-proofComplete:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for PostgreSQL replay proof under DDL fence")
	}

	ddlConnection, err := ddlDatabase.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve PostgreSQL concurrent-DDL connection: %v", err)
	}
	if _, err := ddlConnection.ExecContext(
		ctx,
		"SET lock_timeout = '250ms'",
	); err != nil {
		_ = ddlConnection.Close()
		t.Fatalf("set PostgreSQL concurrent-DDL lock timeout: %v", err)
	}
	_, ddlErr := ddlConnection.ExecContext(
		ctx,
		`ALTER TABLE `+children+`
		 ADD CONSTRAINT "children_parent_code_fkey"
		 FOREIGN KEY ("parent_code")
		 REFERENCES `+parents+` ("code")
		 ON UPDATE CASCADE`,
	)
	if closeErr := ddlConnection.Close(); closeErr != nil {
		t.Errorf("close PostgreSQL concurrent-DDL connection: %v", closeErr)
	}
	if ddlErr == nil {
		t.Fatal("unsafe PostgreSQL foreign-key DDL crossed the page fence")
	}
	var postgresError *pgconn.PgError
	if !errors.As(ddlErr, &postgresError) ||
		postgresError.Code != "55P03" {
		t.Fatalf(
			"concurrent PostgreSQL DDL error = %T %v, want lock_not_available",
			ddlErr,
			ddlErr,
		)
	}
	var constraints int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM pg_catalog.pg_constraint
		  WHERE connamespace = $1::regnamespace
		    AND conname = 'children_parent_code_fkey'`,
		namespace,
	).Scan(&constraints); err != nil {
		t.Fatalf("inspect blocked PostgreSQL foreign-key DDL: %v", err)
	}
	if constraints != 0 {
		t.Fatal("blocked PostgreSQL foreign-key DDL became visible")
	}

	release()
	select {
	case result := <-writeResults:
		if result.err != nil {
			t.Fatalf("fenced PostgreSQL network write: %v", result.err)
		}
		assertPostgresReceipt(
			t,
			result.receipt,
			CommitDurable,
			1,
			1,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fenced PostgreSQL network write")
	}
	var parentCode, childCode string
	if err := database.QueryRowContext(
		ctx,
		"SELECT code FROM "+parents+" WHERE id = 1",
	).Scan(&parentCode); err != nil {
		t.Fatalf("read fenced PostgreSQL parent: %v", err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT parent_code FROM "+children+" WHERE id = 1",
	).Scan(&childCode); err != nil {
		t.Fatalf("read fenced PostgreSQL child: %v", err)
	}
	if parentCode != "new" || childCode != "old" {
		t.Fatalf(
			"fenced PostgreSQL page escaped through concurrent DDL: parent=%q child=%q",
			parentCode,
			childCode,
		)
	}
}

type postgresStage4PausingPGXProvider struct {
	database      *sql.DB
	proofComplete chan struct{}
	releaseWrite  chan struct{}
}

func (provider postgresStage4PausingPGXProvider) WithConnection(
	ctx context.Context,
	operation func(postgresNativeConnection) error,
) (operationError error) {
	connection, err := provider.database.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			operationError = errors.Join(
				operationError,
				fmt.Errorf(
					"close PostgreSQL Stage 4 pausing connection: %w",
					closeErr,
				),
			)
		}
	}()
	discard := false
	operationError = connection.Raw(func(driverConnection any) error {
		stdlibConnection, ok := driverConnection.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf(
				"PostgreSQL Stage 4 concurrent-DDL sentinel requires pgx stdlib",
			)
		}
		return operation(postgresStage4PausingPGXConnection{
			connection:    stdlibConnection.Conn(),
			proofComplete: provider.proofComplete,
			releaseWrite:  provider.releaseWrite,
			discard:       &discard,
		})
	})
	if discard {
		_ = connection.Raw(func(any) error {
			return driver.ErrBadConn
		})
	}
	return operationError
}

type postgresStage4PausingPGXConnection struct {
	connection    *pgx.Conn
	proofComplete chan struct{}
	releaseWrite  chan struct{}
	discard       *bool
}

func (connection postgresStage4PausingPGXConnection) Discard() {
	if connection.discard != nil {
		*connection.discard = true
	}
}

func (connection postgresStage4PausingPGXConnection) Begin(
	ctx context.Context,
) (postgresNativeTransaction, error) {
	transaction, err := connection.connection.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &postgresStage4PausingPGXTransaction{
		delegate: postgresPGXTransaction{
			transaction: transaction,
		},
		proofComplete: connection.proofComplete,
		releaseWrite:  connection.releaseWrite,
	}, nil
}

type postgresStage4PausingPGXTransaction struct {
	delegate      postgresPGXTransaction
	proofComplete chan struct{}
	releaseWrite  chan struct{}
}

func (transaction *postgresStage4PausingPGXTransaction) ReadStage4PostgresRetainedShape(
	ctx context.Context,
	table schema.Table,
) (postgresCatalogTableShape, bool, error) {
	return transaction.delegate.ReadStage4PostgresRetainedShape(
		ctx,
		table,
	)
}

func (transaction *postgresStage4PausingPGXTransaction) QueryStage4PostgresIncomingForeignKeys(
	ctx context.Context,
	namespace string,
	table string,
) (stage4PostgresReplayCatalogRows, error) {
	rows, err := transaction.delegate.
		QueryStage4PostgresIncomingForeignKeys(
			ctx,
			namespace,
			table,
		)
	if err != nil {
		return nil, err
	}
	return &postgresStage4PausingPGXRows{
		delegate:      rows,
		ctx:           ctx,
		proofComplete: transaction.proofComplete,
		releaseWrite:  transaction.releaseWrite,
	}, nil
}

func (transaction *postgresStage4PausingPGXTransaction) CopyRows(
	ctx context.Context,
	table []string,
	columns []string,
	rows [][]any,
) (int64, error) {
	return transaction.delegate.CopyRows(
		ctx,
		table,
		columns,
		rows,
	)
}

func (transaction *postgresStage4PausingPGXTransaction) Exec(
	ctx context.Context,
	statement string,
) (int64, error) {
	return transaction.delegate.Exec(ctx, statement)
}

func (transaction *postgresStage4PausingPGXTransaction) Commit(
	ctx context.Context,
) error {
	return transaction.delegate.Commit(ctx)
}

func (transaction *postgresStage4PausingPGXTransaction) Rollback(
	ctx context.Context,
) error {
	return transaction.delegate.Rollback(ctx)
}

type postgresStage4PausingPGXRows struct {
	delegate      stage4PostgresReplayCatalogRows
	ctx           context.Context
	proofComplete chan struct{}
	releaseWrite  chan struct{}
	waitOnce      sync.Once
	waitErr       error
}

func (rows *postgresStage4PausingPGXRows) Next() bool {
	if rows.delegate.Next() {
		return true
	}
	rows.waitOnce.Do(func() {
		close(rows.proofComplete)
		select {
		case <-rows.ctx.Done():
			rows.waitErr = rows.ctx.Err()
		case <-rows.releaseWrite:
		}
	})
	return false
}

func (rows *postgresStage4PausingPGXRows) Scan(
	destinations ...any,
) error {
	return rows.delegate.Scan(destinations...)
}

func (rows *postgresStage4PausingPGXRows) Err() error {
	if rows.waitErr != nil {
		return rows.waitErr
	}
	return rows.delegate.Err()
}

func (rows *postgresStage4PausingPGXRows) Close() error {
	return rows.delegate.Close()
}
