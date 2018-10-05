package main

import (
	"bitbucket.org/bertrandchenal/londinium"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/bclicn/color"
	"os"
	"log"
	"time"
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
	read_label := flag.String("r", "", "Read label")
	write_label := flag.String("w", "", "Write label")
	create_label := flag.String("c", "", "New label")
	schema := flag.String("s", "", "Schema")
	flag.Parse()

	fmt.Println(*file, *db_name, "" == *read_label, *write_label, *create_label)

	if *db_name == "" {
		*db_name = "ldm.db"
	}
	db, err := bolt.Open(*db_name, 0600, nil)
	if err != nil {
		unravel(err)
	}

	// Create label
	 if *read_label != "" {
		 fmt.Println("Read not implemented )-:")
	 } else if *create_label != "" {
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
		if *file == "" {
			fh := os.Stdin
			err := londinium.Write(db, *write_label, fh)
			unravel(err)
		} else {
			fh, err := os.Open(*file)
			unravel(err)
			defer fh.Close()
			err = londinium.Write(db, *write_label, fh)
			unravel(err)
		}
	}

	elapsed := time.Since(start)
	log.Printf("Done (%s)", elapsed)
}
