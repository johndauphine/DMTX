package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

// MySQLServerFlavor identifies one version-pinned server implementation behind
// the canonical mysql engine. Configuration aliases are not flavor evidence:
// callers must use the live server identity returned by this package.
type MySQLServerFlavor uint8

const (
	MySQLServerFlavorUnknown MySQLServerFlavor = iota
	MySQLServerFlavorOracle80
	MySQLServerFlavorMariaDB1011
)

// Preserve the package-private names used by the existing source discovery
// implementation and its tests while exposing the same closed flavor type to
// target adapters.
type mysqlServerFlavor = MySQLServerFlavor

const (
	mysqlServerFlavorUnknown     = MySQLServerFlavorUnknown
	mysqlServerFlavorOracle80    = MySQLServerFlavorOracle80
	mysqlServerFlavorMariaDB1011 = MySQLServerFlavorMariaDB1011
)

type mysqlServerFlavorCatalog struct {
	version        string
	versionComment string
}

const mysqlServerFlavorQuery = `
	SELECT
		VERSION(),
		@@version_comment
`

// OpenMySQLSource opens the version-pinned source implementation selected
// from the live server identity. Public engine aliases are deliberately not
// trusted as flavor evidence because config canonicalization maps mysql,
// mariadb, and maria to the same engine.
func OpenMySQLSource(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	probe, err := OpenMySQL(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	flavor, err := detectMySQLServerFlavor(ctx, probe)
	if err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf("detect MySQL source flavor: %w", err)
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf("close MySQL source flavor probe: %w", err)
	}
	switch flavor {
	case mysqlServerFlavorOracle80:
		return OpenMySQL80(ctx, endpoint)
	case mysqlServerFlavorMariaDB1011:
		return OpenMariaDB1011(ctx, endpoint)
	default:
		return nil, fmt.Errorf("unsupported MySQL source flavor")
	}
}

// OpenMySQLTarget opens and verifies the version-pinned native target selected
// from the live server identity. The returned flavor is the only supported
// basis for choosing flavor-specific planning and write syntax.
func OpenMySQLTarget(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, MySQLServerFlavor, error) {
	probe, err := OpenMySQL(ctx, endpoint)
	if err != nil {
		return nil, MySQLServerFlavorUnknown, err
	}
	flavor, err := DetectMySQLServerFlavor(ctx, probe)
	if err != nil {
		_ = probe.Close()
		return nil, MySQLServerFlavorUnknown, fmt.Errorf(
			"detect MySQL target flavor: %w",
			err,
		)
	}
	if err := probe.Close(); err != nil {
		return nil, MySQLServerFlavorUnknown, fmt.Errorf(
			"close MySQL target flavor probe: %w",
			err,
		)
	}

	var database *sql.DB
	switch flavor {
	case MySQLServerFlavorOracle80:
		database, err = OpenMySQL80Target(ctx, endpoint)
	case MySQLServerFlavorMariaDB1011:
		database, err = OpenMariaDB1011Target(ctx, endpoint)
	default:
		err = fmt.Errorf("unsupported MySQL target flavor")
	}
	if err != nil {
		return nil, MySQLServerFlavorUnknown, err
	}
	return database, flavor, nil
}

// OpenMariaDB1011 verifies and pins a MariaDB 10.11 source connection. Unlike
// Oracle MySQL, MariaDB must not receive information_schema_stats_expiry.
func OpenMariaDB1011(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	database, err := OpenMySQL(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := VerifyMariaDB1011Source(ctx, database); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("verify MariaDB 10.11 connection: %w", err)
	}
	return database, nil
}

// VerifyMySQLSource dispatches source verification using the live server
// flavor, then delegates to a version-pinned catalog contract.
func VerifyMySQLSource(
	ctx context.Context,
	database *sql.DB,
) error {
	flavor, err := detectMySQLServerFlavor(ctx, database)
	if err != nil {
		return err
	}
	switch flavor {
	case mysqlServerFlavorOracle80:
		return VerifyMySQL80Source(ctx, database)
	case mysqlServerFlavorMariaDB1011:
		return VerifyMariaDB1011Source(ctx, database)
	default:
		return fmt.Errorf("unsupported MySQL source flavor")
	}
}

// VerifyMySQLTarget dispatches target verification using the live server
// flavor, then returns that flavor for target planning and writer selection.
func VerifyMySQLTarget(
	ctx context.Context,
	database *sql.DB,
) (MySQLServerFlavor, error) {
	flavor, err := DetectMySQLServerFlavor(ctx, database)
	if err != nil {
		return MySQLServerFlavorUnknown, err
	}
	switch flavor {
	case MySQLServerFlavorOracle80:
		err = VerifyMySQL80Target(ctx, database)
	case MySQLServerFlavorMariaDB1011:
		err = VerifyMariaDB1011Target(ctx, database)
	default:
		err = fmt.Errorf("unsupported MySQL target flavor")
	}
	if err != nil {
		return MySQLServerFlavorUnknown, err
	}
	return flavor, nil
}

// VerifyMySQLTargetForFlavor re-runs the complete flavor-pinned target
// contract through a caller-supplied queryer. Target replay fencing uses this
// with its pinned *sql.Tx so per-session SQL modes and constraint switches are
// proven on the exact connection that will execute page DML.
func VerifyMySQLTargetForFlavor(
	ctx context.Context,
	queryer MySQLCatalogQueryer,
	flavor MySQLServerFlavor,
) error {
	if queryer == nil {
		return fmt.Errorf("verify MySQL target: catalog queryer is required")
	}
	switch flavor {
	case MySQLServerFlavorOracle80:
		return verifyMySQL80Target(ctx, queryer)
	case MySQLServerFlavorMariaDB1011:
		return verifyMariaDB1011Target(ctx, queryer)
	default:
		return fmt.Errorf("unsupported MySQL target flavor")
	}
}

// DetectMySQLServerFlavor reads the live server identity and rejects ambiguous
// or inconsistent MySQL-compatible distributions.
func DetectMySQLServerFlavor(
	ctx context.Context,
	database *sql.DB,
) (MySQLServerFlavor, error) {
	return detectMySQLServerFlavor(ctx, database)
}

func detectMySQLServerFlavor(
	ctx context.Context,
	database *sql.DB,
) (mysqlServerFlavor, error) {
	var catalog mysqlServerFlavorCatalog
	if err := database.QueryRowContext(
		ctx,
		mysqlServerFlavorQuery,
	).Scan(
		&catalog.version,
		&catalog.versionComment,
	); err != nil {
		return mysqlServerFlavorUnknown, fmt.Errorf(
			"read MySQL server flavor: %w",
			err,
		)
	}
	return mysqlServerFlavorFromCatalog(catalog)
}

func mysqlServerFlavorFromCatalog(
	catalog mysqlServerFlavorCatalog,
) (mysqlServerFlavor, error) {
	version := strings.ToLower(strings.TrimSpace(catalog.version))
	comment := strings.ToLower(strings.TrimSpace(catalog.versionComment))
	versionMariaDB := strings.Contains(version, "mariadb")
	commentMariaDB := strings.Contains(comment, "mariadb")
	switch {
	case versionMariaDB && commentMariaDB:
		return mysqlServerFlavorMariaDB1011, nil
	case !versionMariaDB &&
		!commentMariaDB &&
		strings.Contains(comment, "mysql"):
		return mysqlServerFlavorOracle80, nil
	default:
		return mysqlServerFlavorUnknown, fmt.Errorf(
			"unsupported MySQL server flavor version=%q comment=%q",
			catalog.version,
			catalog.versionComment,
		)
	}
}
