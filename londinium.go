package londinium

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/couchbase/vellum"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const CHUNK_SIZE = 1000000 // Must be < 2^32

type Schema []string
type Frame struct {
	Schema      []string
	KeyColumns  []string
	ValueColumn string
	Data        map[string][]string
	err error
}
type Indexer struct {
	Index  bytes.Buffer
	Values []uint32
	Name   string
	err error
}

func NewFrame(schema Schema) *Frame {
	data := make(map[string][]string)
	key_columns := schema[:len(schema)-1]
	value_column := schema[len(schema)-1]
	return &Frame{schema, key_columns, value_column, data, nil}
}

func (self *Frame) Len() int {
	return len(self.Data[self.ValueColumn])
}

func (self *Frame) Width() int {
	return len(self.Data)
}

func (self *Frame) KeyWidth() int {
	return len(self.KeyColumns)
}


// Netstring encoding
func Encode(items ...string) ([]byte, error) {
	// TODO create a Encoder type that holds the buffer and accept multi calls to Write (or WriteString)
	var buffer bytes.Buffer
	for _, item := range items {
		head := []byte(fmt.Sprintf("%d:", len(item)))
		tail := []byte(",")
		_, err := buffer.Write(head)
		_, err := buffer.Write(item)
		_, err := buffer.Write(tail)
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

// Netstring decoding
func Decode(buf *bytes.Buffer) ([]string, error) {
	head, err := buf.ReadBytes(byte(':'))
	if err == io.EOF {
		return make([]string, 0), nil
	}
	// Read header giving item size
	length, err := strconv.ParseInt(string(head[:len(head)-1]), 10, 32)
	if err != nil {
		return nil, err
	}

	// Read payload
	payload := make([]byte, length)
	_, err = io.ReadFull(buf, payload)
	if err != nil {
		return nil, err
	}

	res := []string{string(payload)}

	// Read end delimiter
	delim, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if delim != byte(',') {
		panic("Unexpected end of stream")
	}

	tail, err := Decode(buf)
	if err != nil {
		return nil, err
	}

	return append(res, tail...), nil
}

func loadCsv(fh io.Reader, schema Schema, frame_chan chan *Frame) {
	// Read a csv and feed the frame_chan with frames of size
	// CHUNK_SIZE or less
	r := csv.NewReader(fh)
	headers, err := r.Read()
	if err != nil {
		frame_chan <- &Frame{err: err}
		return
	}

	for {
		frame := NewFrame(schema)
		pos := 0
		for {
			pos += 1
			if pos > CHUNK_SIZE {
				frame_chan <- frame
				break
			}
			record, err := r.Read()
			if err == io.EOF {
				// Send trailing rows and stop
				frame_chan <- frame
				close(frame_chan)
				return
			}
			if err != nil {
				frame_chan <- &Frame{err: err}
				return
			}

			for pos, header := range headers {
				frame.Data[header] = append(frame.Data[header], record[pos])
			}
		}
	}

}

func saveFrame(bkt *bolt.Bucket, frame *Frame) error {
	// Compute index for every column of the frame, save those and
	// save the fst
	inbox := make(chan *Indexer, frame.Len())
	for _, header := range frame.KeyColumns {
		go func(header string) {
			indexer := buildIndex(frame.Data[header])
			indexer.Name = header
			inbox <- indexer
		}(header)
	}

	// Wait for every index to be created and save them
	reverse_map := make(map[string][]uint32)
	for i := 0; i < frame.KeyWidth(); i++ {
		rev := <- inbox
		if rev.err != nil {
			return rev.err
		}
		// err := bkt.Put(key, rev.Index.Bytes()) TODO ADD TO ENCODER
		if err != nil {
			return err
		}
		reverse_map[rev.Name] = rev.Values
	}

	// TODO: some column may only need 2 bytes (aka uint16)
	var fst bytes.Buffer
	builder, err := vellum.New(&fst, nil)
	if err != nil {
		return err
	}

	key_len := len(reverse_map) * 4
	values := frame.Data[frame.ValueColumn]
	for row := 0; row < frame.Len(); row++ {
		key := make([]byte, key_len)
		for pos, colname := range frame.KeyColumns {
			buff := key[pos*4 : (pos+1)*4]
			binary.BigEndian.PutUint32(buff, reverse_map[colname][row])
		}
		weight, err := strconv.ParseFloat(values[row], 64)
		if err != nil {
			return err
		}

		err = builder.Insert(key, uint64(weight*1000))
		if err != nil {
			return err
		}

	}
	builder.Close()


	key = bkt.NextSequence()
	payload, err := Encode(rev.Index.Bytes(), fst.Bytes()) // TODO USE ENCODER
	err = bkt.Put(key, payload)
	return err
}

func buildIndex(arr []string) (*Indexer) {
	// Sort input
	tmp := make([]string, len(arr))
	reverse := make([]uint32, len(arr))
	copy(tmp, arr)
	sort.Strings(tmp)

	// Build index in-memory
	var idx bytes.Buffer
	builder, err := vellum.New(&idx, nil)
		if err != nil {
		return &Indexer{err: err}
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
		return &Indexer{err: err}
	}


	// Use index to compute reverse array
	fst, err := vellum.Load(idx.Bytes())
		if err != nil {
		return &Indexer{err: err}
	}

	for pos, item := range arr {
		id, exists, err := fst.Get([]byte(item))
		if err != nil {
			return &Indexer{err: err}
		}

		if !exists {
			panic("Fatal!")
		}
		// TODO assert array len is less than 2^32
		reverse[pos] = uint32(id)
	}
	return &Indexer{idx, reverse, "", nil}
}

func Basename(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n >= 0 {
		return s[:n]
	}
	return s
}


func CreateLabel(name string, columns []string) error {
    // DB organisation:
    //
    // - label_1
    //   - schema: netstring list of cols
    //   - frames:
    //     - 0x1: frame-blob (netsting of chunks)
    //     - ...
    //     - 0x3f6: fst
    //   - frame-starts:
    //     - key_1: 0x1
    //     - ...
    //     - key_n: 0x3f6: fst
    //   - frame-ends:
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

	db, err := bolt.Open("test.db", 0600, nil)
	if err != nil {
		return err
	}

	// Transaction closure
	err = db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
		schema, err := Encode(columns...)
		if err != nil {
			return err
		}
		if err = bkt.Put([]byte("schema"), schema); err != nil {
			// Save schema
			return err
		} else if _, err = tx.CreateBucket([]byte("frames")); err != nil {
			// Create frame bucket
			return err
		} else if _, err = tx.CreateBucket([]byte("frames-starts")); err != nil {
			// Create starts & ends bucket
			return err
		}
		_, err = tx.CreateBucket([]byte("frames-ends"))
		return err
	})
	return err
}


func Write(label string, csv_stream io.Reader) error {
	var schema *Schema
	db, err := bolt.Open("test.db", 0600, nil)
	err = db.View(func(tx *bolt.Tx) error {
		bkt, err := tx.Bucket([]byte(label))
		if err != nil {
			return err
		}
		value := bkt.Get([]byte("schema"))
		columns, err := Decode(bytes.NewBuffer(value))
		schema = &Schema(columns)
		return err
	})
	if err != nil {
		return err
	}

	// Load csv and fill chan with chunks
	frame_chan := make(chan *Frame)
	var fr *Frame
	go loadCsv(csv_stream, *schema, frame_chan)
	pos := 0

	// Transaction closure
	err = db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.Bucket([]byte(label))
		frame_bkt, err := bkt.Bucket([]byte("frames"))

		// Create a bucket.
		for fr = range frame_chan {
			if fr.err != nil {
				return fr.err
			}
			saveFrame(frame_bkt, fr)
			pos++
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = db.Close()
	return err
}

func Read() error {
	// Load csv and fill chan with chunks
	// Connect db
	if len(os.Args) < 2 {
		log.Fatal("Not enough arguments")
	}
	label := os.Args[1]
	db, err := bolt.Open("test.db", 0600, nil)
	if err != nil {
		return err
	}

	// Transaction closure
	err = db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		bkt := tx.Bucket([]byte(label))
		err = bkt.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %v.\n", k, len(v))
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	return err
}
