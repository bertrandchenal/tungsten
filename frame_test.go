package tungsten

import (
	"os"
	"testing"
)


func TestFrameFromCSV(t *testing.T) {
	source_file := "test-data/big-two-cols.csv"
	fh, err := os.Open(source_file)
	check(t, err)
	schema := []string{"x", "y", "z"}
	fr := NewFrameFromCsv(schema, fh)

	fh, err = os.OpenFile("output.tun", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	check(t, err)
	fr.Save(fh)
	fh.Close()


	// source_file = "test-data/basic-one-col.csv"
	// fh, err = os.Open(source_file)
	// check(t, err)
	// schema = []string{"x", "z"}
	// fr = NewFrameFromCsv(schema, fh)

	// fh, err = os.OpenFile("output.tun", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	// check(t, err)
	// fr.Save(fh)
	// fh.Close()
}

