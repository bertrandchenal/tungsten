package londinium

import (
	"bytes"
	"testing"
)

func TestIt(t *testing.T) {
	var encoded = [][]byte{
		[]byte("Hello world!"),
		[]byte(""),
		[]byte("Goodbye world"),
	}
	out := Encode(encoded[0], encoded[1], encoded[2])
	if "12:Hello world!,0:,13:Goodbye world," != string(out) {
		t.Error("Encoding error")
	}

	decoded, err := Decode(bytes.NewBuffer(out))
	if err != nil {
		t.Error(err)
	}
	for pos, part := range decoded {
		if string(part) != string(encoded[pos]) {
			t.Error("Decoding error")
		}
	}
}
