package main

import (
	"bitbucket.org/bertrandchenal/londinium"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/etcd-io/bbolt"
	"github.com/bclicn/color"
	"io"
	"os"
	"log"
	"time"
	"strings"
)

func unravel(err error) {
	if err != nil {
		fmt.Println(color.Red(err.Error()))
		os.Exit(1)
	}
}

func main() {
	start := time.Now()
	db_name := flag.String("db", "ldm.db", "Database file")
	file := flag.String("f", "", "Input/Output file")
	create_label := flag.String("c", "", "New label")
	write_label := flag.String("w", "", "Write label")
	read_label := flag.String("r", "", "Read label")
	info := flag.String("i", "", "Print label info")
	schema := flag.String("s", "", "Schema")
	flag.Parse()



	if *db_name == "" {
		*db_name = "ldm.db"
	}
	db, err := bbolt.Open(*db_name, 0600, nil)
	if err != nil {
		unravel(err)
	}

	// Create label
	if *create_label != "" {
		if *schema == "" {
			log.Fatal("Missing -s argument")
		}
		csvReader := csv.NewReader(bytes.NewBuffer([]byte(*schema)))
		columns, err := csvReader.Read()
		if err == nil {
			err = londinium.CreateLabel(db, *create_label, columns)
		}
		unravel(err)
	} else if *write_label != "" {
		var fh io.ReadCloser
		if *file == "" {
			fh = os.Stdin
		} else {
			fh, err = os.Open(*file)
			unravel(err)
		}
		defer fh.Close()
		err = londinium.Write(db, *write_label, fh)
		unravel(err)
	} else if *read_label != "" {
		var fh io.WriteCloser
		if *file == "" {
			fh = os.Stdout
		} else {
			fh, err = os.OpenFile(*file, os.O_RDWR|os.O_CREATE, 0755)
			unravel(err)
		}
		defer fh.Close()
		err := londinium.Read(db, *read_label, fh)
		unravel(err)		
	} else if *info != "" {
		schema, err := londinium.GetSchema(db, *info)
		unravel(err)
		fmt.Println("Schema:", strings.Join(*schema, ", "))
	}

	elapsed := time.Since(start)
	log.Printf("Done (%s)", elapsed)
}
