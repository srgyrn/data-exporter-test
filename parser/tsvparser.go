package parser

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
)

type Parser struct {
	reader *csv.Reader
}

// representation of tsv data in code
type dataModel struct {
	titleType      string
	primaryTitle   string
	originalTitle  string
	isAdult        bool
	startYear      int
	endYear        int
	runtimeMinutes int
	genres         []string
}

// NewParser is the constructor of the tsv parser. It takes the reader of a tsv file
func NewParser(r io.Reader) *Parser {
	rdr := csv.NewReader(r)
	rdr.Comma = '\t'

	return &Parser{rdr}
}

//TODO: check if lineCountToRead exceeds actual lineCount, return an error if so.
// TODO: if lineCountToRead == 0, set total number of lines to wg.Add
// Parse, reads the number of lines set in lineCountToRead, creates a dataModel slice with the data and returns it
func (p *Parser) Parse(lineCountToRead int) []dataModel {
	var lineCount int
	var models []dataModel
	var wg sync.WaitGroup
	wg.Add(lineCountToRead)

	for ; ; lineCount++ {
		// The first line is titles of the fields so skip it
		if lineCount == 0 {
			continue
		}

		record, err := p.reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if lineCountToRead > 0 && lineCount > lineCountToRead {
			break
		}

		go func() {
			isAdult, _ := strconv.ParseBool(record[4])
			startYear, _ := strconv.Atoi(record[5])

			var endYear, runtimeMinutes int
			if record[6] != "\\N" {
				endYear, _ = strconv.Atoi(record[6])
			}

			if record[7] != "\\N" {
				runtimeMinutes, _ = strconv.Atoi(record[7])
			}

			temp := dataModel{
				titleType:      record[1],
				primaryTitle:   record[2],
				originalTitle:  record[3],
				isAdult:        isAdult,
				startYear:      startYear,
				endYear:        endYear,
				runtimeMinutes: runtimeMinutes,
				genres:         strings.Split(record[8], ","),
			}

			models = append(models, temp)
			wg.Done()
		}()
	}

	wg.Wait()
	return models
}
