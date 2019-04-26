package tungsten

import (
	"bytes"
	"github.com/etcd-io/bbolt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

const test_dir = "test-data"
const db_name = "tungsten-test.db"

func cleanup() {
	err := os.Remove(path.Join(db_name))
	if err != nil {
		println("--> ", err.Error())
	}
}

func check(t *testing.T, err error) {
	if err != nil {
		panic(err)
	}
}

func TestEndtoEnd(t *testing.T) {
	columns := []string{"x", "y", "z"}
	source_file := "basic-two-cols.csv"
	RunEndtoEnd(t, columns, source_file)
	columns = []string{"x", "z"}
	source_file = "basic-one-col.csv"
	RunEndtoEnd(t, columns, source_file)
}

func RunEndtoEnd(t *testing.T, columns []string, source_file string) {
	cleanup()
	db, err := bbolt.Open(path.Join(db_name), 0600, nil)
	check(t, err)

	// Create label
	label := "test"
	err = CreateLabel(db, label, columns)
	check(t, err)
	// Add data
	in_file := path.Join(test_dir, source_file)
	input_fh, err := os.Open(in_file)
	check(t, err)
	defer input_fh.Close()
	err = Write(db, label, input_fh)
	check(t, err)

	// Read it again
	out_file := path.Join(test_dir, "tmp.csv")
	out_fh, err := os.OpenFile(out_file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	defer out_fh.Close()
	check(t, err)
	qr, err := NewQuery(db, label, "csv")
	check(t, err)
	_, err = qr.WriteTo(out_fh)
	check(t, err)

	// Close db
	err = db.Close()
	check(t, err)

	// Compare files
	in_content, err := ioutil.ReadFile(in_file)
	check(t, err)

	out_content, err := ioutil.ReadFile(out_file)
	check(t, err)

	if !bytes.Equal(in_content, out_content) {
		t.Error("Content mismatch!")
	}
}
