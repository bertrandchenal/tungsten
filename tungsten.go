
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

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type Schema []string
type Segment struct {
	Schema      []string
	KeyColumns  []string
	ValueColumn string
	Data        map[string][]string
	err         error
}
type Column struct {
	Index  bytes.Buffer
	Values []uint32
	Name   string
	err    error
}

type NetString struct {
	buffer bytes.Buffer
	err    error
}

func NewSegment(schema Schema) *Segment {
	data := make(map[string][]string)
	key_columns := schema[:len(schema)-1]
	value_column := schema[len(schema)-1]
	return &Segment{schema, key_columns, value_column, data, nil}
}

func (self *Segment) Len() int {
	return len(self.Data[self.ValueColumn])
}

func (self *Segment) Width() int {
	return len(self.Data)
}

func (self *Segment) KeyWidth() int {
	return len(self.KeyColumns)
}

func (self *Segment) Row(offset int) []byte {
	var buffer bytes.Buffer
	buff_p := &buffer
	w := csv.NewWriter(buff_p)
	record := make([]string, len(self.KeyColumns))
	for pos, col := range self.KeyColumns {
		record[pos] = self.Data[col][offset]
	}
	w.WriteAll([][]string{record})

	if err := w.Error(); err != nil {
		panic(err) // TODO
	}
	return buffer.Bytes()
}

func (self *Segment) StartKey() []byte {
	return self.Row(0)
}

func (self *Segment) EndKey() []byte {
	return self.Row(self.Len() - 1)
}

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
	col_idx := make(map[int]string)
	for _, col := range schema {
		found := false
		for pos, header := range headers {
			if header == col {
				found = true
				col_idx[pos] = col
			}
		}
		// Column not found
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
			record, err := r.Read()
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

			for i, col := range col_idx {
				segment.Data[col] = append(segment.Data[col], record[i])
			}
			row += 1
		}
	}
}

func serializeSegment(bkt *bbolt.Bucket, segment *Segment) ([]byte, error) {
	// Compute index for every column of the segment, save those and
	// save the fst
	inbox := make(chan *Column, segment.Len())
	go func() {
		for _, header := range segment.KeyColumns {
			column := buildIndex(segment.Data[header])
			column.Name = header
			inbox <- column
		}
	}()
	ns := NewNetString()
	// Save indexes
	data_map := make(map[string][]uint32)
	for i := 0; i < segment.KeyWidth(); i++ {
		col := <-inbox
		if col.err != nil {
			return nil, col.err
		}
		// Save the index in netstring
		ns.Encode(col.Index.Bytes())
		if ns.err != nil {
			return nil, ns.err
		}
		// Keep values (aka references to index)
		data_map[col.Name] = col.Values
	}
	// Convert value column to float & compute min
	int_values := make([]uint64, segment.Len())
	float_values := make([]float64, segment.Len())
	min_value := math.MaxFloat64
	for pos, v := range segment.Data[segment.ValueColumn] {
		float_v, err := strconv.ParseFloat(v, 64)
		if err != nil {
			panic(err)
		}
		float_values[pos] = float_v
		min_value = math.Min(float_v, min_value)
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
	// TODO: some column may only need 2 bytes (aka uint16)
	var fst bytes.Buffer
	builder, err := vellum.New(&fst, nil)
	if err != nil {
		return nil, err
	}
	key_len := len(data_map) * 4
	key := make([]byte, key_len)
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

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func buildIndex(arr []string) *Column {
	// Sort input
	tmp := make([]string, len(arr))
	reverse := make([]uint32, len(arr))
	copy(tmp, arr)
	sort.Strings(tmp)
	// Build fst index in-memory
	var idx bytes.Buffer
	builder, err := vellum.New(&idx, nil)
	if err != nil {
		return &Column{err: err}
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
		return &Column{err: err}
	}

	// Use index to compute reverse array
	fst, err := vellum.Load(idx.Bytes())
	if err != nil {
		return &Column{err: err}
	}
	for pos, item := range arr {
		id, exists, err := fst.Get([]byte(item))
		if err != nil {
			return &Column{err: err}
		}

		if !exists {
			panic("Fatal!")
		}
		// TODO assert array len is less than 2^32
		reverse[pos] = uint32(id)
	}
	return &Column{idx, reverse, "", nil}
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
	var fr *Segment
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
		for fr = range segment_chan {
			if fr.err != nil {
				return fr.err
			}
			payload, err := serializeSegment(segment_bkt, fr)
			if err != nil {
				return err
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
			} // TODO KEEP REFERENCE TO EXISTING SEGMENT IF KEY IS ALREADY IN BUCKET
			err = start_bkt.Put(key, fr.StartKey())
			if err != nil {
				return err
			}
			// Add last line of csv to end index
			end_bkt := bkt.Bucket([]byte("end"))
			if end_bkt == nil {
				return errors.New("Missing 'ends' bucket")
			}
			err = end_bkt.Put(key, fr.EndKey())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = db.Close()
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

func Read(db *bbolt.DB, label string, csv_stream io.Writer) error {
	// Transaction closure
	err := db.View(func(tx *bbolt.Tx) error {
		// Create a bucket.
		bkt := tx.Bucket([]byte(label))
		if bkt == nil {
			return errors.New("Missing label bucket")
		}
		segment_bkt := bkt.Bucket([]byte("segment"))
		if segment_bkt == nil {
			return errors.New("Missing segment bucket")
		}
		csv_writer := csv.NewWriter(csv_stream)
		err := segment_bkt.ForEach(func(k, segment []byte) error {
			// TODO implement a goroutine to parallelize decode-load and iteration
			ns := NewNetBytes(segment)
			items := ns.Decode()
			indexes := items[:len(items)-3]
			conv_factor_b := items[len(items)-3]
			min_value_b := items[len(items)-2]
			data := items[len(items)-1]

			// Extra conv_factor & min_value of value column
			conv_factor, err := strconv.ParseFloat(string(conv_factor_b), 64)
			if err != nil {
				return err
			}
			min_value, err := strconv.ParseFloat(string(min_value_b), 64)
			if err != nil {
				return err
			}

			var resolvers []*Resolver
			for _, idx := range indexes {
				resolvers = append(resolvers, NewResolver(idx))
			}
			fst, err := vellum.Load(data)
			if err != nil {
				return err
			}
			itr, err := fst.Iterator(nil, nil)
			resolver_len := len(resolvers)
			record := make([]string, resolver_len+1)
			for err == nil {
				key, val := itr.Current()
				for pos, resolver := range resolvers {
					buff := key[pos*4 : (pos+1)*4]
					id := binary.BigEndian.Uint32(buff)
					record[pos] = resolver.Get(id)
				}
				val_f := (float64(val) / conv_factor) + min_value
				record[resolver_len] = strconv.FormatFloat(val_f, 'f', -1, 64)
				err := csv_writer.Write(record)
				check(err)
				if itr.Next() != nil {
					break
				}
			}
			if err != nil {
				return err
			}
			return nil
		})
		csv_writer.Flush()
		return err
	})
	return err
}
