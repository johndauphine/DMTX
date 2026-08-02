package migrate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
)

type stage4LiveEnvironmentLookup func(string) string

type stage4ClickHouseTLSAuthority struct {
	environmentVariable string
	host                string
	port                int
	database            string
	caPath              string
}

type stage4ClickHouseTLSVerifier func(stage4ClickHouseTLSAuthority) error

func stage4LiveEnvironmentMissing(
	lookup stage4LiveEnvironmentLookup,
) []string {
	missing := make([]string, 0, len(stage4LiveEnvironment))
	for _, name := range stage4LiveEnvironment {
		if strings.TrimSpace(lookup(name)) == "" {
			missing = append(missing, name)
		}
	}
	return missing
}

// stage4LiveEnvironmentPreflight validates the fixture facts that individual
// live tests otherwise discover only after some of the matrix has started.
// It deliberately reports variable names and required properties, never DSNs
// or connection errors, because those can contain credentials.
func stage4LiveEnvironmentPreflight(
	lookup stage4LiveEnvironmentLookup,
	verifyClickHouseTLS stage4ClickHouseTLSVerifier,
) []string {
	issues := make([]string, 0)
	issues = append(issues, stage4ValidatePostgresLiveEnvironment(lookup)...)
	issues = append(
		issues,
		stage4ValidateMySQLFamilyLiveEnvironment(
			lookup,
			"DMTX_TEST_MYSQL_DSN",
			"DMTX_TEST_MYSQL_TARGET_DSN",
			"DMTX_TEST_MYSQL_ADMIN_DSN",
			"DMTX_TEST_MYSQL_CA",
			"dmtx_test",
		)...,
	)
	issues = append(
		issues,
		stage4ValidateMySQLFamilyLiveEnvironment(
			lookup,
			"DMTX_TEST_MARIADB_DSN",
			"DMTX_TEST_MARIADB_TARGET_DSN",
			"DMTX_TEST_MARIADB_ADMIN_DSN",
			"DMTX_TEST_MARIADB_CA",
			"dmtx_mariadb_test",
		)...,
	)
	issues = append(issues, stage4ValidateSQLServerLiveEnvironment(lookup)...)
	issues = append(
		issues,
		stage4ValidateClickHouseLiveEnvironment(lookup, verifyClickHouseTLS)...,
	)
	return issues
}

func stage4ValidatePostgresLiveEnvironment(
	lookup stage4LiveEnvironmentLookup,
) []string {
	const dsnVariable = "DMTX_TEST_POSTGRES_DSN"
	dsn := strings.TrimSpace(lookup(dsnVariable))
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return []string{
			dsnVariable + " must be a parseable PostgreSQL DSN with verified TLS",
		}
	}
	if strings.TrimSpace(parsed.Host) == "" ||
		strings.TrimSpace(parsed.Database) == "" ||
		!postgresRouteLiveRequiresTLS(parsed) {
		return []string{
			dsnVariable + " must select a database with verified TLS and a hostname",
		}
	}
	caPath := stage4PostgresLiveCAPath(
		dsn,
		lookup("PGSSLROOTCERT"),
	)
	if caPath == "" || strings.EqualFold(caPath, "system") {
		return []string{
			dsnVariable + " must expose sslrootcert or PGSSLROOTCERT must name an explicit CA file",
		}
	}
	if issue := stage4ValidatePEMCAFile(
		dsnVariable+" sslrootcert/PGSSLROOTCERT",
		caPath,
	); issue != "" {
		return []string{issue}
	}
	return nil
}

func stage4PostgresLiveCAPath(dsn string, fallback string) string {
	if value := strings.TrimSpace(fallback); value != "" {
		return value
	}
	if parsed, err := url.Parse(dsn); err == nil {
		if value := strings.TrimSpace(parsed.Query().Get("sslrootcert")); value != "" {
			return value
		}
	}
	if value, found := stage4PostgresDeleteKeywordValue(dsn, "sslrootcert"); found {
		return strings.TrimSpace(value)
	}
	return ""
}

func stage4ValidateMySQLFamilyLiveEnvironment(
	lookup stage4LiveEnvironmentLookup,
	sourceVariable string,
	targetVariable string,
	adminVariable string,
	caVariable string,
	tlsConfiguration string,
) []string {
	issues := make([]string, 0, 5)
	if issue := stage4ValidatePEMCAFile(caVariable, lookup(caVariable)); issue != "" {
		issues = append(issues, issue)
	}
	source, issue := stage4ParseMySQLLiveDSN(
		sourceVariable,
		lookup(sourceVariable),
		tlsConfiguration,
	)
	if issue != "" {
		issues = append(issues, issue)
	}
	target, issue := stage4ParseMySQLLiveDSN(
		targetVariable,
		lookup(targetVariable),
		tlsConfiguration,
	)
	if issue != "" {
		issues = append(issues, issue)
	}
	admin, issue := stage4ParseMySQLLiveDSN(
		adminVariable,
		lookup(adminVariable),
		tlsConfiguration,
	)
	if issue != "" {
		issues = append(issues, issue)
	}
	if source != nil && !source.ParseTime {
		issues = append(
			issues,
			sourceVariable+" must set parseTime=true for Stage 4 date/incremental source coverage",
		)
	}
	if source != nil && target != nil && source.Addr == target.Addr &&
		strings.EqualFold(source.DBName, target.DBName) {
		issues = append(
			issues,
			sourceVariable+" and "+targetVariable+
				" must select distinct MySQL-family databases",
		)
	}
	if target != nil && admin != nil && target.Addr != admin.Addr {
		issues = append(
			issues,
			adminVariable+" must use the exact target server address from "+targetVariable,
		)
	}
	return issues
}

func stage4ParseMySQLLiveDSN(
	variable string,
	value string,
	tlsConfiguration string,
) (*mysqlDriver.Config, string) {
	value = strings.TrimSpace(value)
	parseValue, tlsSetting, ok := stage4MySQLLiveDSNForParse(
		value,
		tlsConfiguration,
	)
	if !ok {
		return nil, variable + " must require verified TLS"
	}
	parsed, err := mysqlDriver.ParseDSN(parseValue)
	if err != nil {
		return nil, variable + " must be a parseable TCP MySQL-family database DSN"
	}
	if parsed.Net != "tcp" || strings.TrimSpace(parsed.Addr) == "" ||
		strings.TrimSpace(parsed.DBName) == "" {
		return nil, variable + " must select one TCP database"
	}
	if tlsSetting != tlsConfiguration && tlsSetting != "true" {
		return nil, variable + " must require verified TLS"
	}
	return parsed, ""
}

// mysql.ParseDSN validates a named TLS configuration against the driver's
// process-global registry. The gate runs before individual fixtures install
// their per-test roots, so normalize only the already-authenticated fixture
// name to tls=true for syntax/parseTime inspection. The original value remains
// checked above; this does not make an unknown TLS setting acceptable.
func stage4MySQLLiveDSNForParse(
	value string,
	tlsConfiguration string,
) (string, string, bool) {
	queryOffset := strings.IndexByte(value, '?')
	if queryOffset < 0 || queryOffset == len(value)-1 {
		return "", "", false
	}
	query, err := url.ParseQuery(value[queryOffset+1:])
	if err != nil {
		return "", "", false
	}
	tlsValues, found := query["tls"]
	if !found || len(tlsValues) != 1 {
		return "", "", false
	}
	tlsSetting := strings.TrimSpace(tlsValues[0])
	if tlsSetting != tlsConfiguration && tlsSetting != "true" {
		return "", tlsSetting, false
	}
	if tlsSetting == tlsConfiguration {
		query.Set("tls", "true")
		return value[:queryOffset+1] + query.Encode(), tlsSetting, true
	}
	return value, tlsSetting, true
}

func stage4ValidateSQLServerLiveEnvironment(
	lookup stage4LiveEnvironmentLookup,
) []string {
	const (
		sourceVariable = "DMTX_TEST_MSSQL_DSN"
		targetVariable = "DMTX_TEST_MSSQL_TARGET_DSN"
		caVariable     = "DMTX_TEST_MSSQL_CA"
	)
	issues := make([]string, 0, 3)
	if issue := stage4ValidatePEMCAFile(caVariable, lookup(caVariable)); issue != "" {
		issues = append(issues, issue)
	}
	source, issue := stage4ParseSQLServerLiveDSN(
		sourceVariable,
		lookup(sourceVariable),
		lookup(caVariable),
	)
	if issue != "" {
		issues = append(issues, issue)
	}
	target, issue := stage4ParseSQLServerLiveDSN(
		targetVariable,
		lookup(targetVariable),
		lookup(caVariable),
	)
	if issue != "" {
		issues = append(issues, issue)
	}
	if source != nil && target != nil &&
		strings.EqualFold(source.database, target.database) {
		issues = append(
			issues,
			sourceVariable+" and "+targetVariable+
				" must select distinct SQL Server databases",
		)
	}
	return issues
}

type stage4SQLServerLiveEndpoint struct {
	database string
}

func stage4ParseSQLServerLiveDSN(
	variable string,
	value string,
	caPath string,
) (*stage4SQLServerLiveEndpoint, string) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "sqlserver" ||
		parsed.Hostname() == "" || parsed.User == nil ||
		strings.TrimSpace(parsed.User.Username()) == "" {
		return nil, variable + " must be a complete SQL Server URI"
	}
	if _, hasPassword := parsed.User.Password(); !hasPassword {
		return nil, variable + " must include SQL Server user credentials"
	}
	if rawPort := parsed.Port(); rawPort != "" {
		port, err := strconv.Atoi(rawPort)
		if err != nil || port < 1 || port > 65535 {
			return nil, variable + " has an invalid SQL Server port"
		}
	}
	query := parsed.Query()
	database := strings.TrimSpace(query.Get("database"))
	if database == "" ||
		!strings.EqualFold(query.Get("encrypt"), "true") ||
		!strings.EqualFold(query.Get("guid conversion"), "true") ||
		query.Get("tlsmin") != "1.2" ||
		strings.TrimSpace(query.Get("certificate")) == "" {
		return nil, variable +
			" must set database, encrypt=true, guid conversion=true, tlsmin=1.2, and certificate"
	}
	if strings.EqualFold(query.Get("trustservercertificate"), "true") {
		return nil, variable + " must not trust an unverified SQL Server certificate"
	}
	if filepath.Clean(query.Get("certificate")) !=
		filepath.Clean(strings.TrimSpace(caPath)) {
		return nil, variable + " certificate must match DMTX_TEST_MSSQL_CA"
	}
	return &stage4SQLServerLiveEndpoint{database: database}, ""
}

func stage4ValidateClickHouseLiveEnvironment(
	lookup stage4LiveEnvironmentLookup,
	verifyTLS stage4ClickHouseTLSVerifier,
) []string {
	const caVariable = "DMTX_TEST_CLICKHOUSE_CA"
	issues := make([]string, 0, 5)
	caPath := lookup(caVariable)
	caIssue := stage4ValidatePEMCAFile(caVariable, caPath)
	if caIssue != "" {
		issues = append(issues, caIssue)
	}
	authorities := make([]stage4ClickHouseTLSAuthority, 0, 3)
	for _, variable := range []string{
		"DMTX_TEST_CLICKHOUSE_DSN",
		"DMTX_TEST_CLICKHOUSE_SOURCE_DSN",
		"DMTX_TEST_CLICKHOUSE_TARGET_DSN",
	} {
		authority, issue := stage4ParseClickHouseLiveDSN(
			variable,
			lookup(variable),
			caPath,
		)
		if issue != "" {
			issues = append(issues, issue)
			continue
		}
		authorities = append(authorities, *authority)
	}
	if len(authorities) == 3 &&
		stage4SameClickHouseEndpoint(authorities[1], authorities[2]) {
		issues = append(
			issues,
			"DMTX_TEST_CLICKHOUSE_SOURCE_DSN and DMTX_TEST_CLICKHOUSE_TARGET_DSN must select distinct ClickHouse databases",
		)
	}
	if caIssue != "" || verifyTLS == nil {
		return issues
	}
	for _, authority := range authorities {
		if err := verifyTLS(authority); err != nil {
			issues = append(
				issues,
				authority.environmentVariable+
					" cannot establish verified TLS for its configured hostname; configure the URI hostname to resolve to the fixture and match a certificate signed by DMTX_TEST_CLICKHOUSE_CA",
			)
		}
	}
	return issues
}

func stage4ParseClickHouseLiveDSN(
	variable string,
	value string,
	caPath string,
) (*stage4ClickHouseTLSAuthority, string) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "clickhouse" ||
		parsed.Hostname() == "" || parsed.User == nil ||
		strings.TrimSpace(parsed.User.Username()) == "" ||
		strings.Trim(strings.TrimPrefix(parsed.Path, "/"), " ") == "" {
		return nil, variable + " must be a complete ClickHouse URI"
	}
	query := parsed.Query()
	if !strings.EqualFold(query.Get("secure"), "true") ||
		stage4ClickHouseSkipVerify(query.Get("skip_verify")) {
		return nil, variable + " must require verified TLS"
	}
	port := 9440
	if rawPort := parsed.Port(); rawPort != "" {
		port, err = strconv.Atoi(rawPort)
		if err != nil || port < 1 || port > 65535 {
			return nil, variable + " has an invalid ClickHouse TLS port"
		}
	}
	return &stage4ClickHouseTLSAuthority{
		environmentVariable: variable,
		host:                parsed.Hostname(),
		port:                port,
		database:            strings.TrimPrefix(parsed.Path, "/"),
		caPath:              strings.TrimSpace(caPath),
	}, ""
}

func stage4ClickHouseSkipVerify(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	parsed, err := strconv.ParseBool(value)
	return err != nil || parsed
}

func stage4SameClickHouseEndpoint(
	first stage4ClickHouseTLSAuthority,
	second stage4ClickHouseTLSAuthority,
) bool {
	return strings.EqualFold(first.host, second.host) &&
		first.port == second.port && first.database == second.database
}

func verifyStage4ClickHouseTLSHostname(
	authority stage4ClickHouseTLSAuthority,
) error {
	roots, err := stage4LoadPEMCertificatePool(authority.caPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := (&net.Dialer{}).DialContext(
		ctx,
		"tcp",
		net.JoinHostPort(authority.host, strconv.Itoa(authority.port)),
	)
	if err != nil {
		return err
	}
	tlsConnection := tls.Client(connection, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: authority.host,
		RootCAs:    roots,
	})
	defer tlsConnection.Close()
	if err := tlsConnection.HandshakeContext(ctx); err != nil {
		return err
	}
	if len(tlsConnection.ConnectionState().VerifiedChains) == 0 {
		return errors.New("ClickHouse TLS peer did not produce a verified chain")
	}
	return nil
}

func stage4ValidatePEMCAFile(variable string, value string) string {
	if _, err := stage4LoadPEMCertificatePool(strings.TrimSpace(value)); err != nil {
		return variable + " must name a readable PEM CA file"
	}
	return ""
}

func stage4LoadPEMCertificatePool(path string) (*x509.CertPool, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemData) {
		return nil, errors.New("no certificates in PEM bundle")
	}
	return roots, nil
}

func TestStage4LiveEnvironmentPreflightAcceptsCompleteContract(t *testing.T) {
	environment := stage4CompleteLiveEnvironmentForTest(t)
	verified := make([]string, 0, 3)
	issues := stage4LiveEnvironmentPreflight(
		environment.get,
		func(authority stage4ClickHouseTLSAuthority) error {
			verified = append(verified, authority.environmentVariable)
			return nil
		},
	)
	if len(issues) != 0 {
		t.Fatalf("complete Stage 4 live environment issues = %v", issues)
	}
	if strings.Join(verified, ",") != strings.Join([]string{
		"DMTX_TEST_CLICKHOUSE_DSN",
		"DMTX_TEST_CLICKHOUSE_SOURCE_DSN",
		"DMTX_TEST_CLICKHOUSE_TARGET_DSN",
	}, ",") {
		t.Fatalf("verified ClickHouse authorities = %v", verified)
	}
}

func TestStage4LiveEnvironmentRequiresMySQLFamilyAdminDSNs(t *testing.T) {
	environment := stage4CompleteLiveEnvironmentForTest(t)
	delete(environment, "DMTX_TEST_MYSQL_ADMIN_DSN")
	delete(environment, "DMTX_TEST_MARIADB_ADMIN_DSN")
	missing := stage4LiveEnvironmentMissing(environment.get)
	for _, want := range []string{
		"DMTX_TEST_MYSQL_ADMIN_DSN",
		"DMTX_TEST_MARIADB_ADMIN_DSN",
	} {
		if !stage4LiveEnvironmentContains(missing, want) {
			t.Fatalf("missing variables = %v, want %s", missing, want)
		}
	}
}

func TestStage4LiveEnvironmentPreflightRejectsRequiredFixtureFacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(stage4LiveEnvironmentValues)
		want   string
	}{
		{
			name: "MySQL parse time",
			mutate: func(environment stage4LiveEnvironmentValues) {
				environment["DMTX_TEST_MYSQL_DSN"] = strings.Replace(
					environment["DMTX_TEST_MYSQL_DSN"],
					"parseTime=true",
					"parseTime=false",
					1,
				)
			},
			want: "DMTX_TEST_MYSQL_DSN must set parseTime=true",
		},
		{
			name: "MariaDB parse time",
			mutate: func(environment stage4LiveEnvironmentValues) {
				environment["DMTX_TEST_MARIADB_DSN"] = strings.Replace(
					environment["DMTX_TEST_MARIADB_DSN"],
					"parseTime=true",
					"parseTime=false",
					1,
				)
			},
			want: "DMTX_TEST_MARIADB_DSN must set parseTime=true",
		},
		{
			name: "MySQL source target database separation",
			mutate: func(environment stage4LiveEnvironmentValues) {
				environment["DMTX_TEST_MYSQL_TARGET_DSN"] =
					environment["DMTX_TEST_MYSQL_DSN"]
			},
			want: "DMTX_TEST_MYSQL_DSN and DMTX_TEST_MYSQL_TARGET_DSN must select distinct MySQL-family databases",
		},
		{
			name: "MySQL administrator target address",
			mutate: func(environment stage4LiveEnvironmentValues) {
				environment["DMTX_TEST_MYSQL_ADMIN_DSN"] = strings.Replace(
					environment["DMTX_TEST_MYSQL_ADMIN_DSN"],
					"mysql.example.test:3306",
					"mysql-admin.example.test:3306",
					1,
				)
			},
			want: "DMTX_TEST_MYSQL_ADMIN_DSN must use the exact target server address from DMTX_TEST_MYSQL_TARGET_DSN",
		},
		{
			name: "SQL Server source target database separation",
			mutate: func(environment stage4LiveEnvironmentValues) {
				environment["DMTX_TEST_MSSQL_TARGET_DSN"] =
					environment["DMTX_TEST_MSSQL_DSN"]
			},
			want: "DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_TARGET_DSN must select distinct SQL Server databases",
		},
		{
			name: "ClickHouse source target database separation",
			mutate: func(environment stage4LiveEnvironmentValues) {
				environment["DMTX_TEST_CLICKHOUSE_TARGET_DSN"] =
					environment["DMTX_TEST_CLICKHOUSE_SOURCE_DSN"]
			},
			want: "DMTX_TEST_CLICKHOUSE_SOURCE_DSN and DMTX_TEST_CLICKHOUSE_TARGET_DSN must select distinct ClickHouse databases",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := stage4CompleteLiveEnvironmentForTest(t)
			test.mutate(environment)
			issues := stage4LiveEnvironmentPreflight(
				environment.get,
				func(stage4ClickHouseTLSAuthority) error { return nil },
			)
			if !stage4LiveEnvironmentIssueContains(issues, test.want) {
				t.Fatalf("issues = %v, want %q", issues, test.want)
			}
		})
	}
}

func TestStage4LiveEnvironmentPreflightRedactsClickHouseTLSFailure(t *testing.T) {
	environment := stage4CompleteLiveEnvironmentForTest(t)
	issues := stage4LiveEnvironmentPreflight(
		environment.get,
		func(stage4ClickHouseTLSAuthority) error {
			return errors.New("certificate for secret-clickhouse.example.test is wrong")
		},
	)
	const want = "DMTX_TEST_CLICKHOUSE_DSN cannot establish verified TLS for its configured hostname"
	if !stage4LiveEnvironmentIssueContains(issues, want) {
		t.Fatalf("issues = %v, want %q", issues, want)
	}
	for _, issue := range issues {
		if strings.Contains(issue, "secret-clickhouse.example.test") {
			t.Fatalf("ClickHouse TLS preflight leaked verifier detail: %q", issue)
		}
	}
}

type stage4LiveEnvironmentValues map[string]string

func (values stage4LiveEnvironmentValues) get(name string) string {
	return values[name]
}

func stage4CompleteLiveEnvironmentForTest(
	t *testing.T,
) stage4LiveEnvironmentValues {
	t.Helper()
	caPath := stage4WriteLivePreflightTestCA(t)
	postgresQuery := url.Values{}
	postgresQuery.Set("sslmode", "verify-full")
	postgresQuery.Set("sslrootcert", caPath)
	mssqlSource := stage4SQLServerLiveTestDSN(caPath, "dmtx_source")
	mssqlTarget := stage4SQLServerLiveTestDSN(caPath, "dmtx_target")
	return stage4LiveEnvironmentValues{
		"DMTX_TEST_POSTGRES_DSN":          "postgres://operator:secret@postgres.example.test:5432/dmtx?" + postgresQuery.Encode(),
		"DMTX_TEST_MYSQL_DSN":             "operator:secret@tcp(mysql.example.test:3306)/dmtx_source?parseTime=true&tls=dmtx_test",
		"DMTX_TEST_MYSQL_TARGET_DSN":      "operator:secret@tcp(mysql.example.test:3306)/dmtx_target?tls=dmtx_test",
		"DMTX_TEST_MYSQL_ADMIN_DSN":       "admin:secret@tcp(mysql.example.test:3306)/mysql?tls=dmtx_test",
		"DMTX_TEST_MYSQL_CA":              caPath,
		"DMTX_TEST_MARIADB_DSN":           "operator:secret@tcp(mariadb.example.test:3306)/dmtx_source?parseTime=true&tls=dmtx_mariadb_test",
		"DMTX_TEST_MARIADB_TARGET_DSN":    "operator:secret@tcp(mariadb.example.test:3306)/dmtx_target?tls=dmtx_mariadb_test",
		"DMTX_TEST_MARIADB_ADMIN_DSN":     "admin:secret@tcp(mariadb.example.test:3306)/mysql?tls=dmtx_mariadb_test",
		"DMTX_TEST_MARIADB_CA":            caPath,
		"DMTX_TEST_MSSQL_DSN":             mssqlSource,
		"DMTX_TEST_MSSQL_TARGET_DSN":      mssqlTarget,
		"DMTX_TEST_MSSQL_CA":              caPath,
		"DMTX_TEST_CLICKHOUSE_DSN":        stage4ClickHouseLiveTestDSN("dmtx"),
		"DMTX_TEST_CLICKHOUSE_SOURCE_DSN": stage4ClickHouseLiveTestDSN("dmtx_source"),
		"DMTX_TEST_CLICKHOUSE_TARGET_DSN": stage4ClickHouseLiveTestDSN("dmtx_target"),
		"DMTX_TEST_CLICKHOUSE_CA":         caPath,
	}
}

func stage4SQLServerLiveTestDSN(caPath string, database string) string {
	query := url.Values{}
	query.Set("database", database)
	query.Set("encrypt", "true")
	query.Set("guid conversion", "true")
	query.Set("tlsmin", "1.2")
	query.Set("certificate", caPath)
	return (&url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("operator", "secret"),
		Host:     "mssql.example.test:1433",
		RawQuery: query.Encode(),
	}).String()
}

func stage4ClickHouseLiveTestDSN(database string) string {
	query := url.Values{}
	query.Set("secure", "true")
	return (&url.URL{
		Scheme:   "clickhouse",
		User:     url.UserPassword("operator", "secret"),
		Host:     "clickhouse.example.test:9440",
		Path:     "/" + database,
		RawQuery: query.Encode(),
	}).String()
}

func stage4WriteLivePreflightTestCA(t *testing.T) string {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	certificate := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dmtx stage4 live preflight test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(
		rand.Reader,
		&certificate,
		&certificate,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "stage4-live-preflight-ca.pem")
	if err := os.WriteFile(
		path,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	return path
}

func stage4LiveEnvironmentContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func stage4LiveEnvironmentIssueContains(values []string, wanted string) bool {
	for _, value := range values {
		if strings.Contains(value, wanted) {
			return true
		}
	}
	return false
}
