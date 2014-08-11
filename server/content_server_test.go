package server

import (
	"os"
	"testing"

	"github.com/polyglottis/platform/content/test"
)

var testDB = "content_test.db"

func TestAll(t *testing.T) {
	os.Remove(testDB)

	s, err := NewServer(testDB)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer os.Remove(testDB)

	if s == nil {
		t.Fatal("NewServer should never return a nil server")
	}

	tester := test.NewTester(s, t)
	tester.All()
}
