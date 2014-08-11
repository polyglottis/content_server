package operations

import (
	"os"
	"testing"

	"github.com/polyglottis/content_server/database"
)

var file = "content_test.db"
var testAddr = ":1234"

func TestClientOperationServer(t *testing.T) {

	os.Remove(file)
	db, err := database.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(file)

	op := NewOpServer(db, testAddr)
	if err != nil {
		t.Fatal(err)
	}

	err = op.RegisterAndListen()
	if err != nil {
		t.Fatal(err)
	}

	go op.Accept()

	c, err := NewClient(testAddr)
	if err != nil {
		t.Fatal(err)
	}

	if c == nil {
		t.Fatal("Client should not be nil")
	}
}
