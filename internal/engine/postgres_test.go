package engine

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/johndauphine/dmtx/internal/config"
)

func TestPostgresDSNUsesSecureDefaultAndStructuredEscaping(t *testing.T) {
	endpoint := config.Endpoint{
		Host:     "2001:db8::1",
		Database: "dmtx/name?tenant=1",
		User:     "a@user:ops",
		Password: "p@ss word/?#%",
	}
	dsn, err := PostgresDSN(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	if parsed.Scheme != "postgres" ||
		parsed.Hostname() != endpoint.Host ||
		parsed.Port() != "5432" ||
		parsed.Path != "/"+endpoint.Database {
		t.Fatalf("unexpected PostgreSQL authority or database path")
	}
	if parsed.User.Username() != endpoint.User {
		t.Fatalf("PostgreSQL user was not preserved")
	}
	password, ok := parsed.User.Password()
	if !ok || password != endpoint.Password {
		t.Fatalf("PostgreSQL password was not preserved")
	}
	if got := parsed.Query().Get("sslmode"); got != "require" {
		t.Fatalf("default sslmode = %q, want require", got)
	}
	if got := parsed.Query().Get("sslrootcert"); got != "" {
		t.Fatalf("unexpected default sslrootcert")
	}
	driverConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgx rejected structured PostgreSQL DSN: %v", err)
	}
	if driverConfig.Host != endpoint.Host ||
		driverConfig.Database != endpoint.Database ||
		driverConfig.User != endpoint.User ||
		driverConfig.Password != endpoint.Password {
		t.Fatalf("pgx did not recover the exact escaped endpoint identity")
	}
}

func TestPostgresDSNRequiresConnectionIdentity(t *testing.T) {
	if _, err := PostgresDSN(config.Endpoint{}); err == nil {
		t.Fatal("expected incomplete endpoint to be rejected")
	}
}

func TestPostgresDSNSupportsExplicitVerifiedTLSModes(t *testing.T) {
	caPath := writePostgresTestCA(t)
	for _, test := range []struct {
		name string
		mode string
		want string
	}{
		{name: "verify CA", mode: " Verify-CA ", want: "verify-ca"},
		{name: "verify host", mode: "VERIFY-FULL", want: "verify-full"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := PostgresDSN(config.Endpoint{
				Host:      "db.example.test",
				Database:  "dmtx",
				User:      "reader",
				Password:  "secret",
				SSLMode:   test.mode,
				TLSCAFile: caPath,
			})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := url.Parse(dsn)
			if err != nil {
				t.Fatalf("parse PostgreSQL DSN: %v", err)
			}
			if got := parsed.Query().Get("sslmode"); got != test.want {
				t.Fatalf("sslmode = %q, want %q", got, test.want)
			}
			if got := parsed.Query().Get("sslrootcert"); got != caPath {
				t.Fatalf("sslrootcert was not preserved through URL escaping")
			}
			driverConfig, err := pgconn.ParseConfig(dsn)
			if err != nil {
				t.Fatalf("pgx rejected verified PostgreSQL DSN: %v", err)
			}
			if !postgresTLSConfigVerifiesServer(driverConfig.TLSConfig) {
				t.Fatalf("pgx did not configure verified PostgreSQL TLS")
			}
		})
	}
}

func TestPostgresDSNRejectsUnsafeAndUnsupportedTLSModes(t *testing.T) {
	for _, mode := range []string{
		"disable",
		"allow",
		"prefer",
		"verify",
		"unknown",
	} {
		t.Run(mode, func(t *testing.T) {
			const password = "dsn-secret-must-not-leak"
			_, err := PostgresDSN(config.Endpoint{
				Host:     "db.example.test",
				Database: "dmtx",
				User:     "reader",
				Password: password,
				SSLMode:  mode,
			})
			if err == nil {
				t.Fatalf("expected ssl_mode %q to be rejected", mode)
			}
			if strings.Contains(err.Error(), password) {
				t.Fatalf("TLS admission error leaked the password")
			}
		})
	}
}

func TestPostgresDSNRequiresCAForVerifiedTLS(t *testing.T) {
	for _, mode := range []string{"verify-ca", "verify-full"} {
		t.Run(mode, func(t *testing.T) {
			_, err := PostgresDSN(config.Endpoint{
				Host:     "db.example.test",
				Database: "dmtx",
				User:     "reader",
				SSLMode:  mode,
			})
			if err == nil || !strings.Contains(err.Error(), "requires tls_ca_file") {
				t.Fatalf("expected verified mode without CA to fail closed, got %v", err)
			}
		})
	}
}

func TestPostgresDSNRejectsCAWithUnverifiedTLSMode(t *testing.T) {
	_, err := PostgresDSN(config.Endpoint{
		Host:      "db.example.test",
		Database:  "dmtx",
		User:      "reader",
		SSLMode:   "require",
		TLSCAFile: writePostgresTestCA(t),
	})
	if err == nil || !strings.Contains(err.Error(), "requires ssl_mode") {
		t.Fatalf("expected CA with unverified mode to be rejected, got %v", err)
	}
}

func TestPostgresDSNValidatesCAWithoutLeakingPathOrPassword(t *testing.T) {
	const password = "postgres-secret-must-not-leak"
	missingPath := filepath.Join(t.TempDir(), password+"-missing.pem")
	invalidPath := filepath.Join(t.TempDir(), password+"-invalid.pem")
	if err := os.WriteFile(invalidPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write invalid CA: %v", err)
	}
	for _, test := range []struct {
		name string
		path string
	}{
		{name: "unavailable", path: missingPath},
		{name: "invalid PEM", path: invalidPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := PostgresDSN(config.Endpoint{
				Host:      "db.example.test",
				Database:  "dmtx",
				User:      "reader",
				Password:  password,
				SSLMode:   "verify-full",
				TLSCAFile: test.path,
			})
			if err == nil {
				t.Fatal("expected invalid CA to fail closed")
			}
			if strings.Contains(err.Error(), test.path) ||
				strings.Contains(err.Error(), password) {
				t.Fatalf("CA admission error leaked endpoint secrets")
			}
		})
	}
}

func writePostgresTestCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "DMTX PostgreSQL test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&key.PublicKey,
		key,
	)
	if err != nil {
		t.Fatalf("create test CA: %v", err)
	}
	caPath := filepath.Join(t.TempDir(), "root CA #1.pem")
	if err := os.WriteFile(
		caPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		0o600,
	); err != nil {
		t.Fatalf("write test CA: %v", err)
	}
	return caPath
}
