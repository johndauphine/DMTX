package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/migrate"
)

// The probes that gather evidence from a live endpoint - reachability,
// version, encoding, runtime, and source read privileges.

func probeNetworkReachability(
	ctx context.Context,
	endpoint config.Endpoint,
) bool {
	if strings.TrimSpace(endpoint.Host) == "" {
		return false
	}
	port := endpoint.Port
	if port == 0 {
		switch endpoint.Type {
		case "postgres":
			port = 5432
		case "mysql":
			port = 3306
		case "mssql":
			port = 1433
		case "clickhouse":
			port = 9440
		default:
			return false
		}
	}
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(
		ctx,
		"tcp",
		net.JoinHostPort(endpoint.Host, strconv.Itoa(port)),
	)
	if err != nil {
		return false
	}
	return connection.Close() == nil
}

func openGenericPreflightDatabase(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	switch endpoint.Type {
	case "postgres":
		return engine.OpenPostgres(ctx, endpoint)
	case "mysql":
		return engine.OpenMySQL(ctx, endpoint)
	case "mssql":
		return engine.OpenSQLServer(ctx, endpoint)
	case "clickhouse":
		return engine.OpenClickHouse(ctx, endpoint)
	default:
		return nil, fmt.Errorf("unsupported preflight engine")
	}
}

func verifyProductionEndpointVersion(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	side migrate.PreflightSide,
) bool {
	if database == nil {
		return false
	}
	var err error
	switch endpoint.Type {
	case "postgres":
		err = engine.VerifyPostgres16Source(ctx, database)
	case "mysql":
		if side == migrate.PreflightSource {
			err = engine.VerifyMySQLSource(ctx, database)
		} else {
			_, err = engine.VerifyMySQLTarget(ctx, database)
		}
	case "mssql":
		if side == migrate.PreflightSource {
			err = engine.VerifySQLServer2022Source(ctx, database)
		} else {
			err = engine.VerifySQLServer2022Target(ctx, database)
		}
	case "clickhouse":
		if side == migrate.PreflightSource {
			err = engine.VerifyClickHouse248Source(
				ctx,
				database,
				endpoint.Database,
			)
		} else {
			err = engine.VerifyClickHouse248Target(
				ctx,
				database,
				endpoint.Database,
			)
		}
	default:
		return false
	}
	return err == nil
}

func querySQLiteVersion(ctx context.Context, database *sql.DB) bool {
	var version string
	return database.QueryRowContext(
		ctx,
		`SELECT sqlite_version()`,
	).Scan(&version) == nil && strings.TrimSpace(version) != ""
}

func probeSQLiteRuntime(ctx context.Context) (bool, string) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return false, ""
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return false, ""
	}
	versionVerified := querySQLiteVersion(ctx, database)
	encoding := probeEndpointEncoding(
		ctx,
		database,
		config.Endpoint{Type: "sqlite"},
	)
	return versionVerified, encoding
}

func probeSchemaAccess(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) (bool, bool) {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		var count int
		err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM pragma_database_list WHERE name = 'main'`,
		).Scan(&count)
		return err == nil && count == 1, err == nil && count == 1
	case "postgres":
		var exists, usable bool
		err := database.QueryRowContext(
			ctx,
			`SELECT
				to_regnamespace($1) IS NOT NULL,
				has_schema_privilege(current_user, $1, 'USAGE')`,
			namespace,
		).Scan(&exists, &usable)
		return err == nil && exists, err == nil && exists && usable
	case "mysql":
		var count int
		err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			 FROM information_schema.schemata
			 WHERE schema_name = ?`,
			namespace,
		).Scan(&count)
		return err == nil && count == 1, err == nil && count == 1
	case "mssql":
		var schemaID sql.NullInt64
		var usable int
		err := database.QueryRowContext(
			ctx,
			`SELECT
				SCHEMA_ID(@p1),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT')`,
			namespace,
		).Scan(&schemaID, &usable)
		return err == nil && schemaID.Valid,
			err == nil && schemaID.Valid && usable == 1
	case "clickhouse":
		var count uint64
		err := database.QueryRowContext(
			ctx,
			`SELECT count()
			 FROM system.databases
			 WHERE name = ?`,
			namespace,
		).Scan(&count)
		return err == nil && count == 1, err == nil && count == 1
	default:
		return false, false
	}
}

func probeEndpointEncoding(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) string {
	var value string
	switch endpoint.Type {
	case "sqlite":
		if database.QueryRowContext(
			ctx,
			`PRAGMA encoding`,
		).Scan(&value) != nil || !strings.EqualFold(value, "UTF-8") {
			return ""
		}
	case "postgres":
		if database.QueryRowContext(
			ctx,
			`SHOW server_encoding`,
		).Scan(&value) != nil || !strings.EqualFold(value, "UTF8") {
			return ""
		}
	case "mysql":
		if database.QueryRowContext(
			ctx,
			`SELECT @@character_set_database`,
		).Scan(&value) != nil ||
			!strings.HasPrefix(strings.ToLower(value), "utf8") {
			return ""
		}
	case "mssql":
		if database.QueryRowContext(
			ctx,
			`SELECT CONVERT(varchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))`,
		).Scan(&value) != nil || strings.TrimSpace(value) == "" {
			return ""
		}
	case "clickhouse":
		value = "byte_or_utf8_adapter_contract"
	default:
		return ""
	}
	return value
}

func probeConnectionPoolHeadroom(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	connectionLimit int,
) bool {
	if connectionLimit <= 0 {
		return false
	}
	switch endpoint.Type {
	case "sqlite":
		return true
	case "postgres":
		var maximum, active int
		return database.QueryRowContext(
			ctx,
			`SELECT
				current_setting('max_connections')::integer
				  - current_setting('superuser_reserved_connections')::integer,
				(SELECT COUNT(*) FROM pg_catalog.pg_stat_activity)`,
		).Scan(&maximum, &active) == nil &&
			maximum-active >= connectionLimit
	case "mysql":
		var maximum, userMaximum, active int
		if database.QueryRowContext(
			ctx,
			`SELECT @@max_connections, @@max_user_connections`,
		).Scan(&maximum, &userMaximum) != nil {
			return false
		}
		var variable string
		if database.QueryRowContext(
			ctx,
			`SHOW GLOBAL STATUS LIKE 'Threads_connected'`,
		).Scan(&variable, &active) != nil ||
			!strings.EqualFold(variable, "Threads_connected") {
			return false
		}
		if userMaximum > 0 && userMaximum < maximum {
			maximum = userMaximum
		}
		return maximum-active >= connectionLimit
	case "mssql":
		var maximum, active int64
		return database.QueryRowContext(
			ctx,
			`SELECT @@MAX_CONNECTIONS, COUNT_BIG(*)
			 FROM sys.dm_exec_connections`,
		).Scan(&maximum, &active) == nil &&
			maximum-active >= int64(connectionLimit)
	case "clickhouse":
		var maximum, active uint64
		err := database.QueryRowContext(
			ctx,
			`SELECT
				toUInt64(value),
				(
					SELECT count()
					FROM system.processes
					WHERE user = currentUser()
				)
			 FROM system.settings
			 WHERE name = 'max_concurrent_queries_for_user'`,
		).Scan(&maximum, &active)
		return err == nil && (maximum == 0 ||
			maximum >= active &&
				maximum-active >= uint64(connectionLimit))
	default:
		return false
	}
}

func probeSelectedSourceTables(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	cfg config.Config,
) []string {
	tables, err := listPreflightTables(ctx, database, endpoint)
	if err != nil {
		return nil
	}
	selected, err := config.SelectTables(
		tables,
		cfg.Migration.IncludeTables,
		cfg.Migration.ExcludeTables,
	)
	if err != nil {
		return nil
	}
	return selected
}

func listPreflightTables(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) ([]string, error) {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		rows, err := database.QueryContext(ctx, `
			SELECT name
			FROM sqlite_schema
			WHERE type = 'table'
			  AND name NOT LIKE 'sqlite_%'
			ORDER BY name
		`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var result []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			result = append(result, name)
		}
		return result, rows.Err()
	case "postgres":
		return engine.ListPostgresTables(ctx, database, namespace)
	case "mysql":
		return engine.ListMySQLTables(ctx, database, namespace)
	case "mssql":
		return engine.ListSQLServerTables(ctx, database, namespace)
	case "clickhouse":
		return engine.ListClickHouseTables(ctx, database, namespace)
	default:
		return nil, fmt.Errorf("unsupported preflight table list")
	}
}

func probeSourceReadPrivileges(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) bool {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		var count int
		return database.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM sqlite_schema`,
		).Scan(&count) == nil
	case "postgres":
		var allowed bool
		err := database.QueryRowContext(ctx, `
			SELECT
				has_schema_privilege(current_user, $1, 'USAGE')
				AND COALESCE(bool_and(
					has_table_privilege(relation.oid, 'SELECT')
				), true)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relkind IN ('r', 'p')
		`, namespace).Scan(&allowed)
		return err == nil && allowed
	case "mysql":
		return probeMySQLGrants(
			ctx,
			database,
			namespace,
			[]string{"SELECT"},
		)
	case "mssql":
		var allowed int
		return database.QueryRowContext(
			ctx,
			`SELECT HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT')`,
			namespace,
		).Scan(&allowed) == nil && allowed == 1
	case "clickhouse":
		return probeClickHouseGrant(
			ctx,
			database,
			"SELECT",
			namespace,
		)
	default:
		return false
	}
}

func probeTargetPrivileges(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	mode string,
) (bool, bool) {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		writable := sqliteTargetPathWritable(endpoint.Database)
		return writable, writable
	case "postgres":
		var (
			create        bool
			insert        bool
			selectAllowed bool
			update        bool
			deleteRows    bool
			truncate      bool
			owns          bool
		)
		err := database.QueryRowContext(ctx, `
			SELECT
				has_schema_privilege(current_user, $1, 'CREATE'),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'INSERT')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'SELECT')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'UPDATE')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'DELETE')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'TRUNCATE')
				), true),
				COALESCE(bool_and(
					pg_has_role(current_user, relation.relowner, 'USAGE')
				), true)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relkind IN ('r', 'p')
		`, namespace).Scan(
			&create,
			&insert,
			&selectAllowed,
			&update,
			&deleteRows,
			&truncate,
			&owns,
		)
		if err != nil {
			return false, false
		}
		return admitPostgresTargetPrivileges(
			mode,
			create,
			insert,
			selectAllowed,
			update,
			deleteRows,
			truncate,
			owns,
		)
	case "mysql":
		required := []string{"INSERT", "SELECT", "UPDATE"}
		if mode == "drop_recreate" {
			required = []string{
				"ALTER",
				"CREATE",
				"DROP",
				"INDEX",
				"INSERT",
				"REFERENCES",
				"SELECT",
			}
		}
		write := probeMySQLGrants(
			ctx,
			database,
			namespace,
			required,
		)
		create := probeMySQLGrants(
			ctx,
			database,
			namespace,
			[]string{"CREATE"},
		)
		return write, create
	case "mssql":
		var insert, selectAllowed, update, alter, create int
		err := database.QueryRowContext(ctx, `
			SELECT
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'INSERT'),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT'),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'UPDATE'),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'ALTER'),
				HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')
		`, namespace).Scan(
			&insert,
			&selectAllowed,
			&update,
			&alter,
			&create,
		)
		if err != nil {
			return false, false
		}
		createAllowed := alter == 1 && create == 1
		write := insert == 1 && selectAllowed == 1 && update == 1
		if mode == "drop_recreate" {
			write = write && createAllowed
		}
		return write, createAllowed
	case "clickhouse":
		insert := probeClickHouseGrant(
			ctx,
			database,
			"INSERT",
			namespace,
		)
		create := probeClickHouseGrant(
			ctx,
			database,
			"CREATE TABLE",
			namespace,
		)
		drop := probeClickHouseGrant(
			ctx,
			database,
			"DROP TABLE",
			namespace,
		)
		selectAllowed := probeClickHouseGrant(
			ctx,
			database,
			"SELECT",
			namespace,
		)
		return insert && selectAllowed &&
			(mode != "drop_recreate" || create && drop), create
	default:
		return false, false
	}
}

func admitPostgresTargetPrivileges(
	mode string,
	create bool,
	insert bool,
	selectAllowed bool,
	update bool,
	deleteRows bool,
	truncate bool,
	ownsExistingTables bool,
) (bool, bool) {
	write := insert && selectAllowed && update
	if mode == "drop_recreate" {
		return write && deleteRows && truncate && create &&
			ownsExistingTables, create
	}
	return write, create
}

func probeTargetDeletePrivilege(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) bool {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		return sqliteTargetPathWritable(endpoint.Database)
	case "postgres":
		var allowed bool
		err := database.QueryRowContext(ctx, `
			SELECT COALESCE(bool_and(
				has_table_privilege(relation.oid, 'DELETE')
			), true)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relkind IN ('r', 'p')
		`, namespace).Scan(&allowed)
		return err == nil && allowed
	case "mysql":
		return probeMySQLGrants(
			ctx,
			database,
			namespace,
			[]string{"DELETE"},
		)
	case "mssql":
		var allowed int
		return database.QueryRowContext(
			ctx,
			`SELECT HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'DELETE')`,
			namespace,
		).Scan(&allowed) == nil && allowed == 1
	default:
		return false
	}
}

func probeMySQLTransportCapabilities(
	ctx context.Context,
	database *sql.DB,
) (int64, bool, bool, bool) {
	if database == nil {
		return 0, false, false, false
	}
	var (
		maximum   int64
		localFile int
	)
	err := database.QueryRowContext(
		ctx,
		`SELECT @@max_allowed_packet, @@GLOBAL.local_infile`,
	).Scan(&maximum, &localFile)
	if err != nil || maximum <= 0 ||
		(localFile != 0 && localFile != 1) {
		return 0, false, false, false
	}
	return maximum, true, localFile == 1, true
}

func probeSQLServerSnapshotIsolation(
	ctx context.Context,
	database *sql.DB,
) (bool, bool) {
	if database == nil {
		return false, false
	}
	var state string
	err := database.QueryRowContext(
		ctx,
		`SELECT snapshot_isolation_state_desc
		 FROM sys.databases
		 WHERE database_id = DB_ID()`,
	).Scan(&state)
	if err != nil || strings.TrimSpace(state) == "" {
		return false, false
	}
	return strings.EqualFold(state, "ON"), true
}

func probeSourceSize(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) (int64, bool) {
	var size sql.NullInt64
	var err error
	switch endpoint.Type {
	case "sqlite":
		info, statErr := os.Stat(endpoint.Database)
		if statErr != nil {
			return 0, false
		}
		return info.Size(), info.Size() >= 0
	case "postgres":
		err = database.QueryRowContext(
			ctx,
			`SELECT pg_database_size(current_database())::bigint`,
		).Scan(&size)
	case "mysql":
		err = database.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(data_length + index_length), 0)
			FROM information_schema.tables
			WHERE table_schema = DATABASE()
		`).Scan(&size)
	case "mssql":
		err = database.QueryRowContext(
			ctx,
			`SELECT COALESCE(SUM(CONVERT(bigint, size)) * 8192, 0)
			 FROM sys.database_files`,
		).Scan(&size)
	case "clickhouse":
		err = database.QueryRowContext(ctx, `
			SELECT toInt64(sum(bytes_on_disk))
			FROM system.parts
			WHERE active AND database = currentDatabase()
		`).Scan(&size)
	default:
		return 0, false
	}
	return size.Int64, err == nil && size.Valid && size.Int64 >= 0
}

func probeMySQLGrants(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	required []string,
) bool {
	rows, err := database.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return false
	}
	defer rows.Close()
	found := make(map[string]bool, len(required))
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return false
		}
		upper := strings.ToUpper(grant)
		if !mySQLGrantCoversNamespace(upper, namespace) {
			continue
		}
		if strings.Contains(upper, "GRANT ALL PRIVILEGES ") {
			for _, privilege := range required {
				found[privilege] = true
			}
			continue
		}
		for _, privilege := range required {
			if strings.Contains(
				upper,
				"GRANT "+privilege+",",
			) || strings.Contains(
				upper,
				", "+privilege+",",
			) || strings.Contains(
				upper,
				", "+privilege+" ON ",
			) || strings.Contains(
				upper,
				"GRANT "+privilege+" ON ",
			) {
				found[privilege] = true
			}
		}
	}
	if rows.Err() != nil {
		return false
	}
	for _, privilege := range required {
		if !found[privilege] {
			return false
		}
	}
	return true
}

func mySQLGrantCoversNamespace(grant string, namespace string) bool {
	scope := strings.ToUpper(
		"`" + strings.ReplaceAll(namespace, "`", "``") + "`.*",
	)
	return strings.Contains(grant, " ON *.* ") ||
		strings.Contains(grant, " ON "+scope+" ")
}

func probeClickHouseGrant(
	ctx context.Context,
	database *sql.DB,
	privilege string,
	namespace string,
) bool {
	query := "CHECK GRANT " + privilege + " ON " +
		clickHousePreflightIdentifier(namespace) + ".*"
	var allowed uint8
	return database.QueryRowContext(ctx, query).Scan(&allowed) == nil &&
		allowed == 1
}

func probeSelectedTargetNonEmpty(
	ctx context.Context,
	selected []string,
	target productionEndpointProbe,
) (bool, bool) {
	if target.databaseAbsent && target.endpoint.Type == "sqlite" {
		return false, true
	}
	if target.database == nil || selected == nil {
		return false, false
	}
	targetTables, err := listPreflightTables(
		ctx,
		target.database,
		target.endpoint,
	)
	if err != nil {
		return false, false
	}
	existing := make(map[string]struct{}, len(targetTables))
	for _, table := range targetTables {
		existing[table] = struct{}{}
	}
	for _, table := range selected {
		if _, exists := existing[table]; !exists {
			continue
		}
		nonEmpty, err := preflightTableNonEmpty(
			ctx,
			target.database,
			target.endpoint,
			table,
		)
		if err != nil {
			return false, false
		}
		if nonEmpty {
			return true, true
		}
	}
	return false, true
}

func preflightTableNonEmpty(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	table string,
) (bool, error) {
	qualified := preflightQualifiedTable(endpoint, table)
	query := "SELECT 1 FROM " + qualified + " LIMIT 1"
	if endpoint.Type == "mssql" {
		query = "SELECT TOP (1) 1 FROM " + qualified
	}
	var one int
	err := database.QueryRowContext(ctx, query).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, err
	}
}

func preflightNamespace(endpoint config.Endpoint) string {
	if endpoint.Schema != "" {
		return endpoint.Schema
	}
	switch endpoint.Type {
	case "postgres":
		return "public"
	case "mssql":
		return "dbo"
	case "mysql", "clickhouse":
		return endpoint.Database
	default:
		return "main"
	}
}

func preflightQualifiedTable(
	endpoint config.Endpoint,
	table string,
) string {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "mysql", "clickhouse":
		return clickHousePreflightIdentifier(namespace) + "." +
			clickHousePreflightIdentifier(table)
	case "mssql":
		return "[" + strings.ReplaceAll(namespace, "]", "]]") +
			"].[" + strings.ReplaceAll(table, "]", "]]") + "]"
	default:
		return quotePreflightIdentifier(namespace) + "." +
			quotePreflightIdentifier(table)
	}
}

func quotePreflightIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func clickHousePreflightIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func sqlitePreflightReadOnlyURI(path string) string {
	normalized := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	location := url.URL{Scheme: "file", Path: normalized}
	query := location.Query()
	query.Set("mode", "ro")
	location.RawQuery = query.Encode()
	return location.String()
}

func sqliteTargetPathWriteEvidence(path string) (bool, bool) {
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return false, true
		}
		file, openErr := os.OpenFile(path, os.O_WRONLY, 0)
		if openErr != nil {
			if os.IsPermission(openErr) {
				return false, true
			}
			return false, false
		}
		fileWritable := file.Close() == nil
		parentWritable, parentKnown :=
			sqliteTargetParentWriteAccess(filepath.Dir(path))
		if !parentKnown {
			return false, false
		}
		return fileWritable && parentWritable, true
	}
	if !os.IsNotExist(err) {
		return false, false
	}
	return sqliteTargetParentWriteAccess(filepath.Dir(path))
}

func sqliteTargetPathWritable(path string) bool {
	writable, known := sqliteTargetPathWriteEvidence(path)
	return known && writable
}
