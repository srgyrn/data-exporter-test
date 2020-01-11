package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/srgyrn/data-exporter-test/parser"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	dataFileName    = "data.tsv"
	imdbDataFileUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"
)

func main() {
	timeStarted := time.Now()

	if _, err := os.Stat("data.tsv"); os.IsNotExist(err) {
		data := downloadData()
		extractAndSaveDataFile(data)
	}

	dataFile, err := os.Open(dataFileName)

	if !errors.Is(err, nil) {
		log.Fatal("Failed to open file")
		return

	}

	reader := parser.NewParser(bufio.NewReader(dataFile))
	a := reader.Parse(10)

	fmt.Println("Completed in: " + time.Since(timeStarted).String())

	fmt.Println(a)
}

// Download data file from imdb
func downloadData() io.Reader {
	res, err := http.Get(imdbDataFileUrl)
	if !errors.Is(err, nil) {
		log.Fatal("Failed to get file from imdb", err)
	}
	defer res.Body.Close()

	return res.Body
}

// File downloaded is a gzip file. It has to be extracted before saving it to the disk
func extractAndSaveDataFile(r io.Reader) {
	gr, err := gzip.NewReader(r)
	if !errors.Is(err, nil) {
		log.Fatal("Failed to gunzip file", err)
	}
	defer gr.Close()

	var buf bytes.Buffer
	buf.ReadFrom(gr)

	err = ioutil.WriteFile(dataFileName, buf.Bytes(), os.ModePerm)
	if !errors.Is(err, nil) {
		log.Fatal("Failed to write file", err)
	}
}
