package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestSelectTablesUsesDeterministicSourceOrder(t *testing.T) {
	names := []string{"accounts", "audit_log", "temp_import", "users"}
	selected, err := SelectTables(names, []string{"a*", "u*", "temp_*"}, []string{"audit_*", "temp_*"})
	if err != nil {
		t.Fatalf("SelectTables returned an error: %v", err)
	}
	want := []string{"accounts", "users"}
	if !reflect.DeepEqual(selected, want) {
		t.Fatalf("selected = %v, want %v", selected, want)
	}
}

func TestParseRejectsInvalidTableGlob(t *testing.T) {
	_, err := Parse([]byte("migration:\n  include_tables: ['[']\n"))
	if err == nil || !strings.Contains(err.Error(), "invalid include_tables glob") {
		t.Fatalf("Parse error = %v, want invalid include glob error", err)
	}
}
