package main

import (
	"bytes"
	"encoding/csv"
	// "fmt"
	"github.com/couchbase/vellum"
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


func saveFrame(frame *Frame, filename string) {
	// Compute index for every column of the frame, save those and
	// save the fst
	inbox := make(chan *Indexer, frame.Len())
	for pos, header := range(frame.KeyColumns) {
		go func(header string) {
			idx_name := filename + "-idx-" +  strconv.Itoa(pos)
			_, reverse := buildIndex(frame.Data[header], idx_name)
			inbox <- &Indexer{header, reverse}
		}(header)
	}

	// Wait for every index to be saved
	reverse_map := make(map[string][]uint32)
	for i:=0; i < frame.KeyWidth(); i++ {
		rev := <- inbox
		reverse_map[rev.Name] = rev.Values
	}

	// Join every columns in one big key
	// TODO: some column may only need 2 bytes (aka uint16)
	fh, err := os.Create(filename + ".fst")
	check(err)
	builder, err := vellum.New(fh, nil)
	key_len := len(reverse_map) * 4
	values := frame.Data[frame.ValueColumn]
	for row := 0; row < frame.Len(); row++ {
		key := make([]byte, key_len)
		for pos, colname := range(frame.KeyColumns) {
			buff := key[pos * 4:(pos+1) * 4]
			binary.BigEndian.PutUint32(buff, reverse_map[colname][row])
		}
		weight, err := strconv.Atoi(values[row])
		check(err)
		err = builder.Insert(key, uint64(weight))
		check(err)
	}
	builder.Close()
}


func buildIndex(arr []string, name string) (bytes.Buffer, []uint32){
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
	fh, err := os.Create(name + ".fst")
	check(err)
	defer fh.Close()
	// _, err = idx.WriteTo(fh)
	_, err = fh.Write(idx.Bytes())
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


func main() {
	start := time.Now()
	if len(os.Args) < 4 {
		log.Fatal("Not enough arguments")
	}
	input_file := os.Args[1]
	schema := Schema(os.Args[2:])
	println("Load " + input_file)
	name := Basename(input_file)

	frame_chan := make(chan *Frame)
	var fr *Frame
	go 	loadCsv(os.Args[1], schema, frame_chan)
	pos := 0
	for fr = range(frame_chan) {
		saveFrame(fr, name + "-" + strconv.Itoa(pos))
		pos++
	}
	elapsed := time.Since(start)
	log.Printf("Done (%s)", elapsed)
}
