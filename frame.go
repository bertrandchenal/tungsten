package tungsten

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"github.com/couchbase/vellum"
	"io"
	"sort"
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
			return pos
		}
	}
	return res
}

func NewFrame(schema []string) *Frame {
	depth := len(schema) - 1
	buffers := make([]*bytes.Buffer, depth)
	for pos := 0; pos < depth; pos++ {
		buffers[pos] = &bytes.Buffer{}
	}
	frame := &Frame{Schema: schema, Depth: depth}
	frame.buffers = buffers
	frame.transducers = make([]*vellum.Builder, depth)

	// Instanciate top-level fst
	fst, err := vellum.New(frame.buffers[0], nil)
	panicker(err)
	frame.transducers[0] = fst

	return frame
}

func (self *Frame) RollOver(fromPos int) {
	// Re-instantiate a new transducer from fromPos to end of array of
	// transducers
	for pos := fromPos + 1 ; pos < self.Depth; pos++ {
		if self.transducers[pos] != nil {
			self.transducers[pos].Close()
		}
		println("ROLLOVER", self.Schema[pos], len(self.buffers[pos].Bytes()))
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
	self.RollOver(prefix)

	for i := prefix; i < self.Depth-1; i++ {
		offset := self.buffers[i+1].Len()
		println("INSERT", self.Schema[i], tic.Key[i], offset)
		self.transducers[i].Insert([]byte(tic.Key[i]), uint64(offset))
	}
	self.transducers[self.Depth-1].Insert([]byte(tic.Key[self.Depth-1]), tic.Value)
}

func (self *Frame) Save(fh io.Writer) {
	offsets := make(map[string]int)

	// Write fst from leaf to root
	offset := 0
	for pos := self.Depth-1; pos >= 0; pos-- {
		self.transducers[pos].Close()
		b := self.buffers[pos].Bytes()
		offset += len(b)
		key := self.Schema[pos]
		println("WRITE OUT", key, pos, offset)
		offsets[key] = offset
		_, err := fh.Write(b)
		panicker(err)
	}
	// Write offsets
	buff := &bytes.Buffer{}
	sort.Strings(self.Schema)
	transducer, err := vellum.New(buff, nil)
	panicker(err)
	for _, name := range self.Schema {
		transducer.Insert([]byte(name), uint64(offsets[name]))
	}
	transducer.Close()
	b := buff.Bytes()
	println("WRITE Offsets", len(b))
	_, err = fh.Write(b)
	panicker(err)

	// Write offset fst lenght
	len_buff := make([]byte, 2)
	binary.BigEndian.PutUint16(len_buff, uint16(len(b)))
	fh.Write(len_buff)
}
