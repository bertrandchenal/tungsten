package tungsten

import (
	"github.com/etcd-io/bbolt"
	"os"
	"path"
	"testing"
	"io/ioutil"
	"bytes"
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


func TestNS(t *testing.T) {
	var encoded = []string{
		"Hello world!",
		"",
		"Goodbye world",
	}
	ns := NewNetString()
	ns.EncodeString(encoded[0], encoded[1], encoded[2])
	check(t, ns.err)
	out := ns.buffer.String()
	if "12:Hello world!,0:,13:Goodbye world," != string(out) {
		t.Error("Encoding error")
	}

	ns = NewNetString(out)
	decoded := ns.DecodeString()
	check(t, ns.err)
	for pos, part := range decoded {
		if part != encoded[pos] {
			t.Error("Decoding error")
		}
	}
}


func TestEndtoEnd(t *testing.T) {
	cleanup()
	db, err := bbolt.Open(path.Join(db_name), 0600, nil)
	check(t, err)

	// Create label
	label := "test"
	columns := []string{"x","y","z"}
	err = CreateLabel(db, label, columns)
	check(t, err)
	// Add data
	in_file := path.Join(test_dir, "basic.csv")
	input_fh, err := os.Open(in_file)
	check(t, err)
	defer input_fh.Close()
	err = Write(db, label, input_fh)
	check(t, err)

	// Read it again
	out_file := path.Join(test_dir, "basic-cp.csv")
	out_fh, err := os.OpenFile(out_file, os.O_RDWR|os.O_CREATE, 0640)
	defer out_fh.Close()
	check(t, err)
	err = Read(db, label, out_fh)
	check(t, err)

	// Close db
	err = db.Close()
	check(t, err)

	// Compare files
	in_content, err :=  ioutil.ReadFile(in_file)
	check(t, err)

	out_content, err :=  ioutil.ReadFile(out_file)
	check(t, err)

	if !bytes.Equal(in_content, out_content) {
		t.Error("KO")
	}
}
