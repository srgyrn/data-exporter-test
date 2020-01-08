package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

const (
	dataFileName = "data.tsv"
	imdbDataFileUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"
)

func main() {
	if _, err := os.Stat("data.zip"); os.IsNotExist(err) {
		data := downloadData()
		extractAndSaveDataFile(data)
	}

	dataFile, err := os.Open(dataFileName)

	if !errors.Is(err, nil) {
		log.Fatal("Failed to open file")
		return
	}

	fmt.Print(dataFile)
}

func downloadData() io.Reader {
	res, err := http.Get(imdbDataFileUrl)
	if !errors.Is(err, nil) {
		log.Fatal("Failed to get file from imdb", err)
	}
	defer res.Body.Close()

	return res.Body
}

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
