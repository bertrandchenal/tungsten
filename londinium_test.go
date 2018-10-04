package londinium

import (

	"testing"
)

func TestIt(t *testing.T) {
	var encoded = [][]byte{
		[]byte("Hello world!"),
		[]byte(""),
		[]byte("Goodbye world"),
	}
	ns := NewNetString()
	ns.Encode(encoded[0], encoded[1], encoded[2])
	if ns.err != nil {
		t.Error(ns.err)
	}
	out := ns.buffer.Bytes()
	if "12:Hello world!,0:,13:Goodbye world," != string(out) {
		t.Error("Encoding error")
	}

	ns = NewNetString(out)
	decoded := ns.Decode()
	if ns.err != nil {
		t.Error(ns.err)
	}
	for pos, part := range decoded {
		if string(part) != string(encoded[pos]) {
			t.Error("Decoding error")
		}
	}
}
