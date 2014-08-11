package database

import (
	"os"
	"testing"

	"github.com/polyglottis/platform/content/test"
)

var testDB = "content_test.db"

func TestAll(t *testing.T) {
	os.Remove(testDB)

	db, err := Open(testDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(testDB)

	if db == nil {
		t.Fatal("Open should never return a nil db")
	}

	tester := test.NewTester(db, t)
	tester.All()
}
