package netstring

import (
	"testing"
)

func TestIt(t *testing.T) {

	var encoded = [][]byte{
		[]byte("Hello world!"),
		[]byte(""),
		[]byte("Goodbye world"),
	}

	out, err := Encode(encoded[0], encoded[1], encoded[2])
	if err != nil {
		t.Error(err)
	}
	if "12:Hello world!,0:,13:Goodbye world," != string(out) {
		t.Error("Encoding error")
	}

	decoded, err := Decode(out)
	if err != nil {
		t.Error(err)
	}
	if encoded != decoded {
		t.Error("decoding error")
	}
}
