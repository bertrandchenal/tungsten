package tungsten

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/DataDog/zstd"
	"github.com/couchbase/vellum"
	"io"
	"strconv"
)

type Frame struct {
	Schema      []string
	transducers []*vellum.Builder // Contains actual data
	buffers     []*bytes.Buffer
	lastTic     *Tic
	Depth       int // Number of non-leaf transducers
	Err         error
}

type Tic struct {
	Key   []string
	Value uint64
}

func panicker(err error) {
	if err != nil {
		panic(err)
	}
}

func (self *Tic) CommonPrefix(otherTic *Tic) int {
	// Compare self with otherTic and returns at which position Data
	// is different
	if otherTic == nil {
		return 0
	}

	depth := len(self.Key)
	res := depth
	for pos := 0; pos < depth; pos++ {
		if self.Key[pos] != otherTic.Key[pos] {
			return pos + 1
		}
	}
	return res
}

func NewFrame(schema []string) *Frame {
	depth := len(schema) - 1
	buffers := make([]*bytes.Buffer, len(schema))
	for pos := 0; pos < len(schema); pos++ {
		buffers[pos] = &bytes.Buffer{}
	}
	frame := &Frame{Schema: schema, Depth: depth}
	frame.buffers = buffers
	frame.transducers = make([]*vellum.Builder, len(schema))
	return frame
}

func (self *Frame) RollOver(fromPos int) {
	// Re-instantiate a new transducer from fromPos to end of array of
	// transducers
	// initialize transducers
	for pos := fromPos; pos < len(self.Schema); pos++ {
		println("ROLLOVER", pos)
		if self.transducers[pos] != nil {
			self.transducers[pos].Close()
		}
		buff := self.buffers[pos]
		fst, err := vellum.New(buff, nil)
		panicker(err)
		self.transducers[pos] = fst
	}

}

func NewFrameFromCsv(schema []string, csv_in io.Reader) *Frame {
	frame := NewFrame(schema)
	r := csv.NewReader(csv_in)
	r.ReuseRecord = true
	headers, err := r.Read()
	if err != nil {
		frame.Err = err
		return frame
	}

	// Identify position of each column [schema pos -> csv pos]
	colIdx := make([]int, len(schema))
	for schPos, col := range schema {
		found := false
		for pos, header := range headers {
			if header == col {
				found = true
				colIdx[schPos] = pos
				break
			}
		}
		if !found {
			err := fmt.Errorf("Missing column: %v", col)
			frame.Err = err
			return frame
		}
	}

	// Fill frame
	keyCols := colIdx[:len(colIdx)-1]
	floatCol := colIdx[len(colIdx)-1]
	for {
		csv_record, err := r.Read()
		if err == io.EOF {
			return frame
		}
		if err != nil {
			frame.Err = err
			return frame
		}
		tic := &Tic{Key: make([]string, frame.Depth)}
		for schPos, csvPos := range keyCols {
			tic.Key[schPos] = csv_record[csvPos]
		}
		val, err := strconv.ParseInt(csv_record[floatCol], 10, 64)
		panicker(err)
		tic.Value = uint64(val)
		frame.Append(tic)
	}
}

func (self *Frame) Append(tic *Tic) {
	prefix := tic.CommonPrefix(self.lastTic)
	self.lastTic = tic
	if prefix < self.Depth {
		self.RollOver(prefix)
	}

	for i := prefix; i < self.Depth-1; i++ {
		offset := self.buffers[i+1].Len()
		self.transducers[i].Insert([]byte(tic.Key[i]), uint64(offset))
	}
	self.transducers[self.Depth].Insert([]byte(tic.Key[self.Depth-1]), tic.Value)
}

func (self *Frame) Save(fh io.Writer) {
	buff := &bytes.Buffer{}
	offsets := make([]int, len(self.Schema))
	// TODO use pre-computed dict feature from zstd
	z_fh := zstd.NewWriterLevel(fh, 10)

	// Write fst from leaf to root
	for pos := self.Depth; pos > 0; pos-- {
		b := self.buffers[pos].Bytes()
		println("WRITE OUT", pos, len(b))
		offsets[pos] = len(b)
		_, err := z_fh.Write(b)
		panicker(err)
	}
	// Write index TODO
	z_fh.Write(buff.Bytes())
	// Write index lenght TODO
	z_fh.Close()
}
