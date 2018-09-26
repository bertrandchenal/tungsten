package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/couchbase/vellum"
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"encoding/binary"
	"strconv"
	"time"
)

const CHUNK_SIZE = 1000000 // Must be < 2^32

type Schema []string
type Frame struct {
	Schema []string
	KeyColumns []string
	ValueColumn string
	Data map[string][]string
}
type Indexer struct {
	Name string
	Index bytes.Buffer
	Values []uint32
}

func NewFrame(schema Schema) *Frame {
	data := make(map[string][]string)
	key_columns := schema[:len(schema) - 1]
	value_column := schema[len(schema) - 1]
	return &Frame{schema, key_columns, value_column, data}
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


func check(err error) {
	if err != nil {
		panic(err)
	}
}



// Netstring encoding
func Encode(items ...string) string {
	var buffer bytes.Buffer
	for _, item := range items {
		buffer.WriteString(fmt.Sprintf("%d:%s,", len(item), item))
	}
	return buffer.String()
}

func Decode(input string) ([]string, error) {

	var res []string
	var buf = bytes.NewBuffer(input)
	lengthToken, err := buf.ReadBytes(":")
	if err == io.EOF {
		break
	} else {
		return nil, err
	}
	// TODO
}

func loadCsv(filename string, schema Schema, frame_chan chan *Frame) {
	// Read a csv and feed the frame_chan with frames of size
	// CHUNK_SIZE or less
	fh, err := os.Open(filename)
	check(err)
	defer fh.Close()
	r := csv.NewReader(fh)
	headers, err := r.Read()
	check(err)

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
			check(err)

			for pos, header := range(headers){
				frame.Data[header] = append(frame.Data[header], record[pos])
			}
		}

	}
}


func saveFrame(bkt *bolt.Bucket, frame *Frame, label string) {
	// Compute index for every column of the frame, save those and
	// save the fst
	inbox := make(chan *Indexer, frame.Len())
	for _, header := range(frame.KeyColumns) {
		go func(header string) {
			idx, reverse := buildIndex(frame.Data[header])
			inbox <- &Indexer{header, idx, reverse}
		}(header)
	}

	// Wait for every index to be created and save them
	reverse_map := make(map[string][]uint32)
	for i:=0; i < frame.KeyWidth(); i++ {
		rev := <- inbox
		label := rev.Name + "-idx-" +  strconv.Itoa(i)
		err := bkt.Put([]byte(label), rev.Index.Bytes())
		check(err)

		reverse_map[rev.Name] = rev.Values
	}

	// Join every columns in one big key
	// TODO: some column may only need 2 bytes (aka uint16)
	var fst bytes.Buffer
	builder, err := vellum.New(&fst, nil)
	check(err)
	key_len := len(reverse_map) * 4
	values := frame.Data[frame.ValueColumn]
	for row := 0; row < frame.Len(); row++ {
		key := make([]byte, key_len)
		for pos, colname := range(frame.KeyColumns) {
			buff := key[pos * 4:(pos+1) * 4]
			binary.BigEndian.PutUint32(buff, reverse_map[colname][row])
		}
		weight, err := strconv.ParseFloat(values[row], 64)
		check(err)
		err = builder.Insert(key, uint64(weight * 1000))
		check(err)
	}
	builder.Close()
	err = bkt.Put([]byte(label), fst.Bytes())
	check(err)
}


func buildIndex(arr []string) (bytes.Buffer, []uint32){
	// Sort input
	tmp := make([]string, len(arr))
	reverse := make([]uint32, len(arr))
	copy(tmp, arr)
	sort.Strings(tmp)

	// Build index in-memory
	var idx bytes.Buffer
	builder, err := vellum.New(&idx, nil)
	check(err)
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
	check(err)

	// Use index to compute reverse array
	fst, err := vellum.Load(idx.Bytes())
	check(err)
	for pos, item := range arr {
		id, exists, err := fst.Get([]byte(item))
		check(err)
		if !exists {
			panic("Fatal!")
		}
		// TODO assert array len is less than 2^32
		reverse[pos] = uint32(id)
	}
	return idx, reverse
}


func Basename(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n >= 0 {
		return s[:n]
		}
	return s
}


func write() {

    // TODO db organisation:
    //
    // Bucket  | content
    // --------+--------
    // :schema:| label -> list of cols (last one being the values)
    // --------+--------
    // Label_i | rev -> list of fst blobs
    //
    // List are netstring encoded, label cannot start with a :

	if len(os.Args) < 4 {
		log.Fatal("Not enough arguments")
	}
	input_file := os.Args[1]
	schema := Schema(os.Args[2:])
	println("Load " + input_file)
	name := Basename(input_file)

	// Load csv and fill chan with chunks
	frame_chan := make(chan *Frame)
	var fr *Frame
	go 	loadCsv(os.Args[1], schema, frame_chan)
	pos := 0

	// Create db
	db, err := bolt.Open("test.db", 0600, nil)
	check(err)
	// Transaction closure
	err = db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		bkt, err := tx.CreateBucketIfNotExists([]byte("default"))
		check(err)
		for fr = range(frame_chan) {
			saveFrame(bkt, fr, name + "-" + strconv.Itoa(pos))
			pos++
		}
		return nil
	})
	check(err)
	err = db.Close()
	check(err)
}


func read() {
	// if len(os.Args) < 4 {
	// 	log.Fatal("Not enough arguments")
	// }

	// Load csv and fill chan with chunks
	// Connect db
	db, err := bolt.Open("test.db", 0600, nil)
	check(err)
	// Transaction closure
	err = db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		bkt := tx.Bucket([]byte("default"))
		err = bkt.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", k, len(v))
			return nil
		})
		check(err)
		return nil
	})
	check(err)

}

func main() {
	start := time.Now()
	// write()
	read()
	elapsed := time.Since(start)
	log.Printf("Done (%s)", elapsed)
}
