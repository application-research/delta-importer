package main

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Dataset struct {
	Dataset string `json:"dataset"`
	Address string `json:"address"`
	Dir     string `json:"dir"`
}

// Read the datasets file and return a slice of Dataset structs
func ReadInDatasetsFromFile(fileName string) []Dataset {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Error reading datasets file: %s", err)
	}

	var datasets []Dataset
	err = json.Unmarshal(data, &datasets)
	if err != nil {
		log.Fatalf("Error unmarshalling datasets JSON:", err)
	}

	return datasets
}

// Returns a list of all the car files in a given directory
func (d *Dataset) CarFilePaths() []string {
	files, err := ioutil.ReadDir(d.Dir)
	if err != nil {
		log.Errorf("error reading directory %s: %v", d.Dir, err)
		return nil
	}

	var fileNames []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".car") {
			fileNames = append(fileNames, filepath.Join(d.Dir, f.Name()))
		}
	}

	return fileNames
}

// Returns the path to a car file in the dataset given a piece cid
func (d *Dataset) GenerateCarFileName(pieceCid string) string {
	return filepath.Join(d.Dir, pieceCid+".car")
}
