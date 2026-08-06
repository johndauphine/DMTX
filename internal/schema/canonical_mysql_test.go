package schema

import (
	"strings"
	"testing"
)

// TestMySQLKeyCollationsAreFlavourSpecific pins the distinction five earlier
// copies of this fact disagreed about.
//
// MySQL 8.0 and MariaDB share no binary collation. A combined set would
// certify, on each server, a collation that server does not have - passing a
// schema check and failing at the engine, which is worse than either list.
func TestMySQLKeyCollationsAreFlavourSpecific(t *testing.T) {
	for _, testCase := range []struct {
		flavor    MySQLFlavor
		collation string
		certified bool
	}{
		{MySQLFlavorOracle, "utf8mb4_bin", true},
		{MySQLFlavorOracle, "utf8mb4_0900_bin", true},
		// MySQL 8.0's own default, and the one that made ordinary tables
		// unreadable when it was asked of every column rather than of keys.
		{MySQLFlavorOracle, "utf8mb4_0900_ai_ci", false},
		{MySQLFlavorOracle, "utf8mb4_unicode_ci", false},
		// MariaDB's collation, which MySQL does not have.
		{MySQLFlavorOracle, "utf8mb4_nopad_bin", false},

		{MySQLFlavorMariaDB, "utf8mb4_nopad_bin", true},
		// And the reverse: MySQL's two do not exist on MariaDB.
		{MySQLFlavorMariaDB, "utf8mb4_bin", false},
		{MySQLFlavorMariaDB, "utf8mb4_0900_bin", false},
		{MySQLFlavorMariaDB, "utf8mb4_unicode_ci", false},

		// An unmeasured server certifies nothing.
		{MySQLFlavorUnknown, "utf8mb4_bin", false},
		{MySQLFlavorUnknown, "utf8mb4_nopad_bin", false},
	} {
		name := string(testCase.flavor) + "/" + testCase.collation
		t.Run(name, func(t *testing.T) {
			got := MySQLTextKeyCollationCertified(testCase.flavor, testCase.collation)
			if got != testCase.certified {
				t.Errorf("certified=%v, want %v", got, testCase.certified)
			}
		})
	}
}

// TestMySQLRemedyNamesACollationThatExists guards against sending an operator
// somewhere that is not there.
//
// The two servers share no binary collation, so one generic message would tell
// half of them to use something their engine does not have - and they would
// try it, because the message came from the tool that refused them.
func TestMySQLRemedyNamesACollationThatExists(t *testing.T) {
	oracle := MySQLKeyCollationRemedy(MySQLFlavorOracle)
	if !strings.Contains(oracle, "utf8mb4_bin") {
		t.Errorf("MySQL remedy does not name a MySQL collation: %s", oracle)
	}
	if strings.Contains(oracle, "utf8mb4_nopad_bin") {
		t.Errorf("MySQL remedy names MariaDB's collation: %s", oracle)
	}

	maria := MySQLKeyCollationRemedy(MySQLFlavorMariaDB)
	if !strings.Contains(maria, "utf8mb4_nopad_bin") {
		t.Errorf("MariaDB remedy does not name MariaDB's collation: %s", maria)
	}

	// Both must mention that only the key column needs it. An operator whose
	// table default is ordinary should not read this as "recollate the table".
	for name, remedy := range map[string]string{"mysql": oracle, "mariadb": maria} {
		if !strings.Contains(remedy, "key column") {
			t.Errorf("%s remedy does not say the table may keep its default: %s", name, remedy)
		}
	}
}

// TestCanonicalFromMySQLAsksOrderingOnlyOfKeys mirrors the SQL Server case,
// because the mistake was the same on both engines.
func TestCanonicalFromMySQLAsksOrderingOnlyOfKeys(t *testing.T) {
	hundred := int64(100)
	body := Column{
		Name:         "note",
		Type:         "text",
		DeclaredType: &DeclaredType{Base: "varchar", Length: &hundred},
	}

	canonical, err := CanonicalFromMySQL(body, MySQLFlavorOracle, "utf8mb4_unicode_ci", false)
	if err != nil {
		t.Fatalf("an ordinary data column was refused: %v", err)
	}
	if !canonical.Certified() {
		t.Error("an ordinary data column was not certified for transfer")
	}
	if canonical.CertifiedAsKey() {
		t.Error("a data column was certified for ordering without being asked")
	}

	canonical, err = CanonicalFromMySQL(body, MySQLFlavorOracle, "utf8mb4_unicode_ci", true)
	if err != nil {
		t.Fatalf("a key should be refused by certification, not by error: %v", err)
	}
	if canonical.CertifiedAsKey() {
		t.Error("a case-insensitive key was certified")
	}
	if !canonical.Certified() {
		t.Error("a refused key lost its transfer certification")
	}
	if canonical.Certification.Reason == "" {
		t.Error("a refused key was given no remedy")
	}
}
