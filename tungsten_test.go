package tungsten

import (
	"github.com/etcd-io/bbolt"
	"os"
	"path"
	"testing"
)

const test_dir = "test-data"
const db_name = "tungsten-test.db"

func cleanup() {
	err := os.Remove(path.Join(test_dir, db_name))
	if err != nil {
		println("--> ", err.Error())
	}
}


func TestNS(t *testing.T) {
	var encoded = []string{
		"Hello world!",
		"",
		"Goodbye world",
	}
	ns := NewNetString()
	ns.EncodeString(encoded[0], encoded[1], encoded[2])
	if ns.err != nil {
		t.Error(ns.err)
	}
	out := ns.buffer.String()
	if "12:Hello world!,0:,13:Goodbye world," != string(out) {
		t.Error("Encoding error")
	}

	ns = NewNetString(out)
	decoded := ns.DecodeString()
	if ns.err != nil {
		t.Error(ns.err)
	}
	for pos, part := range decoded {
		if part != encoded[pos] {
			t.Error("Decoding error")
		}
	}
}


func TestEndtoEnd(t *testing.T) {
	cleanup()
	db, err := bbolt.Open(path.Join(test_dir, db_name), 0600, nil)
	if err != nil {
		t.Error(err)
	}
	// Create label
	label := "test"
	columns := []string{"x","y","z"}
	err = CreateLabel(db, label, columns)
	if err != nil {
		t.Error(err)
	}
	// Add data
	input_fh, err := os.Open(path.Join(test_dir, "basic.csv"))
	if err != nil {
		t.Error(err)
	}
	defer input_fh.Close()
	err = Write(db, label, input_fh)
}
