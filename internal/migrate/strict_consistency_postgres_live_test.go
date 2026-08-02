package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestPostgresStrictConsistencyStableSnapshotsLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL strict-consistency sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL strict-consistency DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		75*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL strict-consistency database: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL strict-consistency database: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL strict-consistency database: %T", err)
	}

	namespace := "dmtx_strict_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	firstName := "stable_alpha"
	secondName := "stable_beta"
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL strict-consistency schema: %v", err)
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
			t.Errorf(
				"drop PostgreSQL strict-consistency schema: %v",
				err,
			)
		}
	})
	firstQualified := postgresQualified(namespace, firstName)
	secondQualified := postgresQualified(namespace, secondName)
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE `+firstQualified+` (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		);
		CREATE TABLE `+secondQualified+` (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		)
	`); err != nil {
		t.Fatalf("create PostgreSQL strict-consistency tables: %v", err)
	}

	opener, err := NewPostgresStrictConsistencyOpener(database)
	if err != nil {
		t.Fatal(err)
	}
	tables := []StrictConsistencyTable{
		postgresStrictLiveTable(namespace, firstName),
		postgresStrictLiveTable(namespace, secondName),
	}
	for _, scope := range []state.StrictSnapshotScope{
		state.StrictSnapshotTable,
		state.StrictSnapshotMigration,
	} {
		scope := scope
		t.Run(string(scope), func(t *testing.T) {
			if _, err := database.ExecContext(
				ctx,
				"TRUNCATE TABLE "+firstQualified+", "+secondQualified+
					"; INSERT INTO "+firstQualified+
					" VALUES (1, 'one'), (2, 'two')"+
					"; INSERT INTO "+secondQualified+
					" VALUES (1, 'one')",
			); err != nil {
				t.Fatalf("seed PostgreSQL strict source: %v", err)
			}

			rawSession, err := opener.OpenStrictConsistency(
				ctx,
				StrictConsistencyOpenRequest{
					RunID:        "run-live-" + string(scope),
					SourceEngine: StrictConsistencyPostgres,
					Scope:        scope,
					ProcessEpoch: "process-live-" +
						string(scope) + "-1",
					Tables: tables,
				},
			)
			if err != nil {
				t.Fatalf("open PostgreSQL strict snapshot: %v", err)
			}
			session := rawSession.(*PostgresStrictConsistencySession)
			capture, err := session.CaptureSameViewEvidence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			assertPostgresStrictLiveCapture(
				t,
				scope,
				capture,
				tables,
				map[state.TaskKey]int64{
					tables[0].Task: 2,
					tables[1].Task: 1,
				},
			)

			writeCtx, writeCancel := context.WithTimeout(
				ctx,
				5*time.Second,
			)
			writeDone := make(chan error, 1)
			go func() {
				_, err := database.ExecContext(
					writeCtx,
					"INSERT INTO "+firstQualified+
						" VALUES (3, 'concurrent')"+
						"; INSERT INTO "+secondQualified+
						" VALUES (2, 'concurrent')",
				)
				writeDone <- err
			}()
			select {
			case err := <-writeDone:
				writeCancel()
				if err != nil {
					t.Fatalf(
						"concurrent PostgreSQL source write was blocked or failed: %v",
						err,
					)
				}
			case <-writeCtx.Done():
				writeCancel()
				t.Fatalf(
					"concurrent PostgreSQL source write blocked behind exported snapshot: %v",
					writeCtx.Err(),
				)
			}

			var group sync.WaitGroup
			readerErrors := make(chan error, len(tables))
			for _, selected := range tables {
				selected := selected
				group.Add(1)
				go func() {
					defer group.Done()
					readerErrors <- session.RunReader(
						ctx,
						selected.Task,
						func(
							readerCtx context.Context,
							queryer PostgresStrictSnapshotQueryer,
						) error {
							var count int64
							if err := queryer.QueryRowContext(
								readerCtx,
								"SELECT count(*)::bigint FROM "+
									postgresQualified(
										selected.Task.Schema,
										selected.Task.Table,
									),
							).Scan(&count); err != nil {
								return err
							}
							want := int64(1)
							if selected.Task == tables[0].Task {
								want = 2
							}
							if count != want {
								return fmt.Errorf(
									"snapshot count for %s.%s = %d, want %d",
									selected.Task.Schema,
									selected.Task.Table,
									count,
									want,
								)
							}
							return nil
						},
					)
				}()
			}
			group.Wait()
			close(readerErrors)
			for err := range readerErrors {
				if err != nil {
					t.Fatal(err)
				}
			}

			for _, live := range []struct {
				qualified string
				want      int64
			}{
				{qualified: firstQualified, want: 3},
				{qualified: secondQualified, want: 2},
			} {
				var count int64
				if err := database.QueryRowContext(
					ctx,
					"SELECT count(*)::bigint FROM "+live.qualified,
				).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != live.want {
					t.Fatalf(
						"later live source count = %d, want %d",
						count,
						live.want,
					)
				}
			}

			reference := capture.Tables[0].SnapshotReference
			if err := session.Close(ctx); err != nil {
				t.Fatalf("release PostgreSQL strict snapshot: %v", err)
			}
			assertPostgresStrictSnapshotReleased(
				t,
				ctx,
				database,
				reference,
			)

			if scope == state.StrictSnapshotMigration {
				rawResume, err := opener.OpenStrictConsistency(
					ctx,
					StrictConsistencyOpenRequest{
						RunID:        "run-live-" + string(scope),
						SourceEngine: StrictConsistencyPostgres,
						Scope:        scope,
						Resume:       true,
						ProcessEpoch: "process-live-" +
							string(scope) + "-2",
						Tables: tables,
					},
				)
				if err != nil {
					t.Fatalf(
						"open fresh PostgreSQL resume epoch: %v",
						err,
					)
				}
				resume := rawResume.(*PostgresStrictConsistencySession)
				resumeCapture, err := resume.CaptureSameViewEvidence(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if resumeCapture.MigrationEpochID ==
					capture.MigrationEpochID ||
					resumeCapture.MigrationSnapshotReference ==
						capture.MigrationSnapshotReference {
					t.Fatalf(
						"resume reused prior PostgreSQL snapshot: first=%#v resume=%#v",
						capture,
						resumeCapture,
					)
				}
				if err := resume.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func postgresStrictLiveTable(
	namespace string,
	name string,
) StrictConsistencyTable {
	table := StrictConsistencyTable{
		Task: state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: namespace,
			Table:  name,
		},
		WorkTopologyHash: "strict-live-" + name,
	}
	return postgresStrictRepairAttempt(table)
}

func assertPostgresStrictLiveCapture(
	t *testing.T,
	scope state.StrictSnapshotScope,
	capture StrictConsistencyCapture,
	tables []StrictConsistencyTable,
	counts map[state.TaskKey]int64,
) {
	t.Helper()
	if len(capture.Tables) != len(tables) {
		t.Fatalf("strict capture table count = %d", len(capture.Tables))
	}
	references := make(map[string]struct{}, len(capture.Tables))
	for _, table := range capture.Tables {
		if table.ExactSourceRowCount != counts[table.Task] {
			t.Fatalf(
				"strict evidence count for %#v = %d, want %d",
				table.Task,
				table.ExactSourceRowCount,
				counts[table.Task],
			)
		}
		references[table.SnapshotReference] = struct{}{}
	}
	switch scope {
	case state.StrictSnapshotTable:
		if capture.MigrationEpochID != "" ||
			capture.MigrationSnapshotReference != "" ||
			len(references) != len(tables) {
			t.Fatalf("invalid table-scoped capture: %#v", capture)
		}
	case state.StrictSnapshotMigration:
		if capture.MigrationEpochID == "" ||
			capture.MigrationSnapshotReference == "" ||
			len(references) != 1 {
			t.Fatalf("invalid migration-scoped capture: %#v", capture)
		}
		for reference := range references {
			if reference != capture.MigrationSnapshotReference {
				t.Fatalf(
					"table reference %q differs from migration reference %q",
					reference,
					capture.MigrationSnapshotReference,
				)
			}
		}
	default:
		t.Fatalf("unexpected strict scope %q", scope)
	}
}

func assertPostgresStrictSnapshotReleased(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	reference string,
) {
	t.Helper()
	transaction, err := database.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatalf("begin released snapshot sentinel: %v", err)
	}
	defer func() {
		_ = transaction.Rollback()
	}()
	if _, err := transaction.ExecContext(
		ctx,
		"SET TRANSACTION SNAPSHOT '"+reference+"'",
	); err == nil {
		t.Fatalf(
			"released PostgreSQL snapshot %q remained importable",
			reference,
		)
	}
}
