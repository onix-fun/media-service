package main

import (
	"net/url"
	"testing"
)

func TestDatabaseMigrationURLPinsMetadataToPublicSchema(t *testing.T) {
	result, err := databaseMigrationURL("postgres://media:p%40ss@localhost:5432/media?sslmode=disable&search_path=media")
	if err != nil {
		t.Fatalf("databaseMigrationURL: %v", err)
	}
	parsed, err := url.Parse(result)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if got := parsed.Query().Get("search_path"); got != "public" {
		t.Fatalf("search_path = %q, want public", got)
	}
	if got := parsed.Query().Get("sslmode"); got != "disable" {
		t.Fatalf("sslmode = %q, want disable", got)
	}
	password, present := parsed.User.Password()
	if !present || password != "p@ss" {
		t.Fatal("password was not preserved")
	}
}

func TestDatabaseMigrationURLRejectsNonPostgresURL(t *testing.T) {
	if _, err := databaseMigrationURL("mysql://localhost/media"); err == nil {
		t.Fatal("expected non-Postgres URL to be rejected")
	}
}
