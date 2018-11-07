
package tungsten

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/couchbase/vellum"
	"github.com/etcd-io/bbolt"
	"io"
	// "log"
	"math"
	// "os"
	"sort"
	"strconv"
	"strings"
)

const CHUNK_SIZE = 1e6 // Must be < 2^32
const CONV_FACTOR = 10000


type Schema []string
type Segment struct {
	Schema      []string
	KeyColumns  []string
	ValueColumn string
	Records []*Record
	err         error
}

type Record struct {
	Data      []string
	Value float64
	err error
}

func NewRecord(length int) *Record {
	data:= make([]string, length)
	return &Record{data, 0, nil}
}

type Index struct {
	Buff  bytes.Buffer
	Reverse []uint32
	Name   string
	err    error
}

type NetString struct {
	buffer bytes.Buffer
	err    error
}

func NewSegment(schema Schema) *Segment {
	data := make([]*Record, 0) //, CHUNK_SIZE / 2
	key_columns := schema[:len(schema)-1]
	value_column := schema[len(schema)-1]
	return &Segment{schema, key_columns, value_column, data, nil}
}

func (self *Segment) Append(record *Record) {
	self.Records = append(self.Records, record)
}

func (self *Segment) Len() int {
	return len(self.Records)
}

func (self *Segment) Key(row int) []byte {
	ns := NewNetString(self.Records[row].Data...)
	return ns.buffer.Bytes()
}

func (self *Segment) ColPos(colName string) int {
	for pos, col := range self.KeyColumns {
		if col == colName {
			return pos
		}
	}
	return -1
}


func (self *Segment) Width() int {
	return len(self.Schema)
}

func (self *Segment) KeyWidth() int {
	return len(self.KeyColumns)
}


func (self *Segment) StartKey() []byte {
	return self.Key(0)
}

func (self *Segment) EndKey() []byte {
	return self.Key(self.Len() - 1)
}


func loadSegment(segment_b []byte, schema Schema) *Segment {
	ns := NewNetBytes(segment_b)
	items := ns.Decode()
	indexes := items[:len(items)-3]
	conv_factor_b := items[len(items)-3]
	min_value_b := items[len(items)-2]
	data := items[len(items)-1]
	// Extract conv_factor & min_value of value column
	conv_factor, err := strconv.ParseFloat(string(conv_factor_b), 64)
	if err != nil {
		return &Segment{err:err}
	}
	min_value, err := strconv.ParseFloat(string(min_value_b), 64)
	if err != nil {
		return &Segment{err:err}
	}
	var resolvers []*Resolver
	for _, idx := range indexes {
		resolvers = append(resolvers, NewResolver(idx))
	}
	fst, err := vellum.Load(data)
	if err != nil {
		return &Segment{err:err}
	}
	fst_it, err := fst.Iterator(nil, nil)

	resolver_len := len(resolvers)
	sgm := NewSegment(schema)
	for {
		key, val := fst_it.Current()
		record := NewRecord(resolver_len + 1)
		for pos, resolver := range resolvers {
			buff := key[pos * 4:(pos + 1) * 4]
			id := binary.BigEndian.Uint32(buff)
			record.Data[pos] = resolver.Get(id)
		}
		val_f := (float64(val) / conv_factor) + min_value
		record.Value = val_f
		sgm.Append(record)
		if fst_it.Next() != nil {
			break
		}
	}

	fst_it.Close()
	fst.Close()
	return sgm
}


// func loadSimpleSegment(segment []byte, csv_writer *csv.Writer) error {
// 	// TODO implement a goroutine to parallelize decode-load and iteration
// 	ns := NewNetBytes(segment)
// 	items := ns.Decode()
// 	conv_factor_b := items[len(items)-3]
// 	min_value_b := items[len(items)-2]
// 	data := items[len(items)-1]
// 	// Extra conv_factor & min_value of value column
// 	conv_factor, err := strconv.ParseFloat(string(conv_factor_b), 64)
// 	if err != nil {
// 		return err
// 	}
// 	min_value, err := strconv.ParseFloat(string(min_value_b), 64)
// 	if err != nil {
// 		return err
// 	}
// 	fst, err := vellum.Load(data)
// 	if err != nil {
// 		return err
// 	}
// 	itr, err := fst.Iterator(nil, nil)
// 	record := make([]string, 2)
// 	for err == nil {
// 		key, val := itr.Current()
// 		val_f := (float64(val) / conv_factor) + min_value
// 		record[0] = string(key)
// 		record[1] = strconv.FormatFloat(val_f, 'f', -1, 64)
// 		err := csv_writer.Write(record)
// 		if err != nil {
// 			return err
// 		}
// 		if itr.Next() != nil {
// 			break
// 		}
// 	}
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// Netstring constructors
func NewNetBytes(values ...[]byte) *NetString {
	var buffer bytes.Buffer
	for _, val := range values {
		buffer.Write(val)
	}
	return &NetString{buffer, nil}
}

func NewNetString(values ...string) *NetString {
	var buffer bytes.Buffer
	for _, val := range values {
		buffer.WriteString(val)
	}
	return &NetString{buffer, nil}
}

// NetString encoding
func (self *NetString) Encode(items ...[]byte) {
	tail := ","
	for _, item := range items {
		head := fmt.Sprintf("%d:", len(item))
		_, err := self.buffer.WriteString(head)
		_, err = self.buffer.Write(item)
		_, err = self.buffer.WriteString(tail)
		if err != nil && self.err != nil {
			self.err = err
			return
		}
	}
}

func (self *NetString) EncodeString(items ...string) {
	for _, item := range items {
		self.Encode([]byte(item))
	}
}

// Netstring decoding
func (self *NetString) Decode() [][]byte {
	head, err := self.buffer.ReadBytes(byte(':'))
	if err == io.EOF {
		return make([][]byte, 0)
	}
	// Read header giving item size
	length, err := strconv.ParseInt(string(head[:len(head)-1]), 10, 32)
	if err != nil {
		self.err = err
		return nil
	}
	// Read payload
	payload := make([]byte, length)
	_, err = self.buffer.Read(payload)
	if err != nil {
		self.err = err
		return nil
	}
	res := [][]byte{payload}
	// Read end delimiter
	delim, err := self.buffer.ReadByte()
	if err != nil {
		self.err = err
		return nil
	}
	if delim != byte(',') {
		self.err = errors.New("Unable de decode netstring, unexpected end of stream")
		return nil
	}
	// Recurse
	tail := self.Decode()
	if self.err != nil {
		return nil
	}

	return append(res, tail...)
}

func (self *NetString) DecodeString() []string {
	res_bytes := self.Decode()
	res := make([]string, len(res_bytes))
	for pos, val := range res_bytes {
		res[pos] = string(val)
	}
	return res
}

func loadCsv(fh io.Reader, schema Schema, segment_chan chan *Segment) {
	// Read a csv and feed the segment_chan with segments of size
	// CHUNK_SIZE or less
	r := csv.NewReader(fh)
	// r.ReuseRecord = true // TODO enable for go >= 1.9
	headers, err := r.Read()
	if err != nil {
		segment_chan <- &Segment{err: err}
		return
	}

	// Identify position of each column
	colIdx := make(map[int]int)
	for schPos, col := range schema {
		found := false
		for pos, header := range headers {
			if header == col {
				found = true
				colIdx[schPos] = pos
			}
		}
		if !found {
			err := fmt.Errorf("Missing column: %v", col)
			segment_chan <- &Segment{err: err}
			return
		}
	}

	for {
		segment := NewSegment(schema)
		row := 0
		for {
			if row > CHUNK_SIZE {
				segment_chan <- segment
				break
			}
			csv_record, err := r.Read()
			if err == io.EOF {
				// Send trailing rows and stop
				if row > 1 {
					segment_chan <- segment
				}
				close(segment_chan)
				return
			}
			if err != nil {
				segment_chan <- &Segment{err: err}
				return
			}

			// Create and fill record
			record := NewRecord(len(schema))
			for schPos, _ := range(segment.KeyColumns) {
				csvPos := colIdx[schPos]
				record.Data[schPos] = csv_record[csvPos]
			}
			valPos := colIdx[len(schema) - 1]
			val := csv_record[valPos]
			record.Value, err = strconv.ParseFloat(val, 64)
			if err != nil {
				segment_chan <- &Segment{err: err}
				return
			}
			segment.Append(record)
			row += 1
		}
	}
}

func dumpSegment(bkt *bbolt.Bucket, segment *Segment) ([]byte, error) {
	// Compute index for every column of the segment, save those and
	// save the fst
	inbox := make(chan *Index, segment.Len())
	go func() {
		for _, colName := range segment.KeyColumns {
			idx := segment.buildIndex(colName)
			inbox <- idx
		}
	}()
	ns := NewNetString()
	// Save indexes
	data_map := make(map[string][]uint32)
	for i := 0; i < segment.KeyWidth(); i++ {
		idx := <- inbox
		if idx.err != nil {
			return nil, idx.err
		}
		// Save the index in netstring
		ns.Encode(idx.Buff.Bytes())
		if ns.err != nil {
			return nil, ns.err
		}
		// Keep values (aka references to index)
		data_map[idx.Name] = idx.Reverse
	}
	// Convert value column to float & compute min
	int_values := make([]uint64, segment.Len())
	float_values := make([]float64, segment.Len())
	min_value := math.MaxFloat64
	for pos, record := range segment.Records {
		float_values[pos] = record.Value
		min_value = math.Min(record.Value, min_value)
	}
	// Convert float to int
	for pos, float_v := range float_values {
		int_v := uint64((float_v - min_value) * CONV_FACTOR)
		int_values[pos] = int_v
	}
	// Save factor & min_value
	ns.EncodeString(strconv.FormatFloat(CONV_FACTOR, 'E', -1, 64))
	if ns.err != nil {
		return nil, ns.err
	}
	ns.EncodeString(strconv.FormatFloat(min_value, 'E', -1, 64))
	if ns.err != nil {
		return nil, ns.err
	}
	// Use columns id to encode main fst
	var fst bytes.Buffer
	builder, err := vellum.New(&fst, nil)
	if err != nil {
		return nil, err
	}
	key_len := len(data_map) * 4
	key := make([]byte, key_len)
	// IDEA: Save float_v has is and simply reference it from the
	// fst. This as the advantage of simplifying simple vs non-simple
	// segment code
	for row := 0; row < segment.Len(); row++ {
		for pos, colname := range segment.KeyColumns {
			buff := key[pos*4 : (pos+1)*4]
			binary.BigEndian.PutUint32(buff, data_map[colname][row])
		}
		if err != nil {
			return nil, err
		}
		err = builder.Insert(key, int_values[row])
		if err != nil {
			return nil, err
		}
	}
	builder.Close()
	// Add main fst to netstring & save in db
	ns.Encode(fst.Bytes())
	payload := ns.buffer.Bytes()
	if ns.err != nil {
		return nil, ns.err
	}
	return payload, nil
}

// func dumpSimpleSegment(bkt *bbolt.Bucket, segment *Segment) ([]byte, error) {
// 	ns := NewNetString()
// 	// Convert value column to float & compute min
// 	int_values := make([]uint64, segment.Len())
// 	float_values := make([]float64, segment.Len())
// 	min_value := math.MaxFloat64
// 	for pos, v := range segment.Data[segment.ValueColumn] {
// 		float_v, err := strconv.ParseFloat(v, 64)
// 		if err != nil {
// 			panic(err)
// 		}
// 		float_values[pos] = float_v
// 		min_value = math.Min(float_v, min_value)
// 	}
// 	// Convert float to int
// 	for pos, float_v := range float_values {
// 		int_v := uint64((float_v - min_value) * CONV_FACTOR)
// 		int_values[pos] = int_v
// 	}
// 	// Save factor & min_value
// 	ns.EncodeString(strconv.FormatFloat(CONV_FACTOR, 'E', -1, 64))
// 	if ns.err != nil {
// 		return nil, ns.err
// 	}
// 	ns.EncodeString(strconv.FormatFloat(min_value, 'E', -1, 64))
// 	if ns.err != nil {
// 		return nil, ns.err
// 	}
// 	// Encode main fst
// 	var fst bytes.Buffer
// 	builder, err := vellum.New(&fst, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	column := segment.Data[segment.KeyColumns[0]]
// 	for pos, val := range column {
// 		err = builder.Insert([]byte(val), int_values[pos])
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	builder.Close()
// 	// Add main fst to netstring & save in db
// 	ns.Encode(fst.Bytes())
// 	payload := ns.buffer.Bytes()
// 	if ns.err != nil {
// 		return nil, ns.err
// 	}
// 	return payload, nil
// }

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func (self *Segment) buildIndex(colName string) *Index {
	// Organise values as column & sort it
	length := self.Len()
	colPos := self.ColPos(colName)
	tmp := make([]string, length)
	reverse := make([]uint32, length)
	for i := 0; i < length; i++ {
		tmp[i] = self.Records[i].Data[colPos]
	}
	sort.Strings(tmp)
	// Build fst index in-memory
	var buff bytes.Buffer
	builder, err := vellum.New(&buff, nil)
	if err != nil {
		return &Index{err: err}
	}

	var prev string
	pos := uint64(0)
	for _, item := range tmp {
		if item == prev {
			continue
		}
		builder.Insert([]byte(item), pos)
		prev = item
		pos++
	}
	builder.Close()

	// Save fst
	if err != nil {
		return &Index{err: err}
	}

	// Use index to compute reverse array
	fst, err := vellum.Load(buff.Bytes())
	if err != nil {
		return &Index{err: err}
	}
	for i := 0; i < length; i++ {
		item := self.Records[i].Data[colPos]
		id, exists, err := fst.Get([]byte(item))
		if err != nil {
			return &Index{err: err}
		}

		if !exists {
			panic("Fatal!")
		}
		// TODO assert array len is less than 2^32
		reverse[i] = uint32(id)
	}
	return &Index{buff, reverse, colName, nil}
}

func Basename(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n >= 0 {
		return s[:n]
	}
	return s
}

func CreateLabel(db *bbolt.DB, name string, columns []string) error {
	// DB organisation:
	//
	// - label_1
	//   - schema: netstring list of cols
	//   - segment:
	//     - 0x1: segment-blob (netsting of chunks)
	//     - ...
	//     - 0x3f6: fst
	//   - start:
	//     - key_1: 0x1
	//     - ...
	//     - key_n: 0x3f6: fst
	//   - end:
	//     - key_1: 0x1
	//     - ...
	//     - key_n: 0x3f6: fst
	// - ...
	// - label_n
	//
	// Each chunk is a netstring of indexes fst, and data fst.
	if len(columns) < 2 {
		return errors.New("Number of columns in schema should be at least 2")
	}

	err := db.Update(func(tx *bbolt.Tx) error {
		bkt, err := tx.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
		ns := NewNetString()
		ns.EncodeString(columns...)
		schema := ns.buffer.Bytes()
		if ns.err != nil {
			return ns.err
		}
		if err = bkt.Put([]byte("schema"), schema); err != nil {
			// Save schema
			return err
		} else if _, err = bkt.CreateBucket([]byte("segment")); err != nil {
			// Create segment bucket
			return err
		} else if _, err = bkt.CreateBucket([]byte("start")); err != nil {
			// Create starts & ends bucket
			return err
		}
		_, err = bkt.CreateBucket([]byte("end"))
		return err
	})
	return err
}

func GetSchema(db *bbolt.DB, label string) (*Schema, error) {
	var schema *Schema
	err := db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte(label))
		if bkt == nil {
			return errors.New("Missing label bucket")
		}
		value := bkt.Get([]byte("schema"))
		ns := NewNetString(string(value))
		columns := ns.DecodeString()
		schema_ := Schema(columns)
		schema = &schema_
		return nil
	})
	return schema, err
}

func Write(db *bbolt.DB, label string, csv_stream io.Reader) error {
	schema, err := GetSchema(db, label)
	if err != nil {
		return err
	}

	// Load csv and fill chan with chunks
	segment_chan := make(chan *Segment)
	var sgm *Segment
	go loadCsv(csv_stream, *schema, segment_chan)

	// Transaction closure
	err = db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte(label))
		if bkt == nil {
			return errors.New("Missing label bucket")
		}
		segment_bkt := bkt.Bucket([]byte("segment"))
		if segment_bkt == nil {
			return errors.New("Missing segment bucket")
		}

		// Save each chunk
		var payload []byte
		for sgm = range segment_chan {
			if sgm.err != nil {
				return sgm.err
			}
			if len(*schema) > 2 {
				payload, err = dumpSegment(segment_bkt, sgm)
				if err != nil {
					return err
				}
			} else {
				panic("Not Implemented")
				// payload, err = dumpSimpleSegment(segment_bkt, fr)
				// if err != nil {
				// 	return err
				// }
			}
			seq, err := segment_bkt.NextSequence()
			if err != nil {
				return err
			}
			key := itob(seq)
			err = segment_bkt.Put(key, payload)
			if err != nil {
				return err
			}
			// Add first line of csv to start index
			start_bkt := bkt.Bucket([]byte("start"))
			if start_bkt == nil {
				return errors.New("Missing 'starts' bucket")
			} // TODO append ref to existing segment if key is already in bucket
			err = start_bkt.Put(key, sgm.StartKey())
			if err != nil {
				return err
			}
			// Add last line of csv to end index
			end_bkt := bkt.Bucket([]byte("end"))
			if end_bkt == nil {
				return errors.New("Missing 'ends' bucket")
			}
			err = end_bkt.Put(key, sgm.EndKey())
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

type Resolver struct {
	id2name []string
}

func NewResolver(idx []byte) *Resolver {
	// Construct mapping between a string id and the string
	// representation. Idx contains an fst which acts as a sorted set
	fst, err := vellum.Load(idx)
	if err != nil {
		panic(err)
	}
	id2name := make([]string, fst.Len())
	itr, err := fst.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	for err == nil {
		key, val := itr.Current()
		id2name[int(val)] = string(key)
		err = itr.Next()
	}
	return &Resolver{id2name}
}

func (self *Resolver) Get(id uint32) string {
	res := self.id2name[id]
	return res
}

type Query struct {
	tx *bbolt.Tx
	label string
	resChan chan *Segment
	encoder Encoder
}

type Encoder interface {
	Setup(Schema) ([]byte, error)
	Encode(*Segment) ([]byte, error)
}

type CSVEncoder struct {
	csv_writer *csv.Writer
}

func (self CSVEncoder) Encode(s *Segment)  ([]byte, error) {
	buffer := &bytes.Buffer{}
	csv_writer := csv.NewWriter(buffer)
	key_width := s.KeyWidth()
	row := make([]string, key_width + 1)
	for _, rec := range s.Records {
		copy(row, rec.Data)
		row[key_width] = strconv.FormatFloat(rec.Value, 'f', -1, 64)
		err := csv_writer.Write(row)
		if err != nil {
			panic(err)
		}
	}
	csv_writer.Flush()
	return buffer.Bytes(), nil
}

func (self CSVEncoder) Setup(s Schema)  ([]byte, error) {
	return []byte("TODO"), nil
}

func NewQuery(db *bbolt.DB, label string, encoding string) (*Query, error) {
	resChan := make(chan *Segment)
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	bkt := tx.Bucket([]byte(label))
	if bkt == nil {
		return nil, errors.New("Missing label bucket")
	}
	segment_bkt := bkt.Bucket([]byte("segment"))
	if segment_bkt == nil {
		return nil, errors.New("Missing segment bucket")
	}
	schema_b := bkt.Get([]byte("schema"))
	ns := NewNetBytes(schema_b)
	schema := Schema(ns.DecodeString())

	go func() {
		segment_bkt.ForEach(func(key, segment_b []byte) error {
			if len(schema) > 2 {
				sgm := loadSegment(segment_b, schema)
				resChan <- sgm
				if sgm.err != nil {
					close(resChan)
				return sgm.err
				}
			} else {
				panic("Not implemented")
				//err = loadSimpleSegment(segment, csv_writer)
			}
			return nil
		})
		close(resChan)
		tx.Rollback()
	}()

	var encoder Encoder
	if encoding == "csv" {
		encoder = CSVEncoder{}
	} else {
		err := fmt.Errorf("Unknown encoding : %v", encoding)
		return nil, err
	}
	encoder.Setup(schema)
	return &Query{tx, label, resChan, encoder}, nil
}

func (self *Query) WriteTo(w io.Writer) (int64, error) {
	written := int64(0)
	for sgm := range self.resChan {
		b, err := self.encoder.Encode(sgm)
		if err != nil {
			return written, err
		}
		written += int64(len(b))
		w.Write(b)
	}
	return written, nil
}
