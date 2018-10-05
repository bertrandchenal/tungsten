package londinium

import (

	"testing"
)

func TestIt(t *testing.T) {
	var encoded = []string{
		"Hello world!",
		"",
		"Goodbye world",
	}
	ns := NewNetString()
	ns.Encode(encoded[0], encoded[1], encoded[2])
	if ns.err != nil {
		t.Error(ns.err)
	}
	out := ns.buffer.String()
	if "12:Hello world!,0:,13:Goodbye world," != out {
		t.Error("Encoding error")
	}

	ns = NewNetString(out)
	decoded := ns.Decode()
	if ns.err != nil {
		t.Error(ns.err)
	}
	for pos, part := range decoded {
		if part != encoded[pos] {
			t.Error("Decoding error")
		}
	}
}
