package main

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Dataset struct {
	Dataset             string          `json:"dataset"`
	Address             string          `json:"address"`
	Dir                 string          `json:"dir"`
	Ignore              bool            `json:"ignore,omitempty"`
	alreadyImportedCids map[string]bool `json:"omitempty"`
}

// Read the datasets file and return a map of Dataset structs keyed by their Address
func ReadInDatasetsFromFile(fileName string) map[string]Dataset {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("error reading datasets file: %s", err)
	}

	var datasets []Dataset
	err = json.Unmarshal(data, &datasets)
	if err != nil {
		log.Fatalf("datasets file is in incorrect format: %v", err)
	}

	datasetMap := make(map[string]Dataset)
	for _, dataset := range datasets {
		if _, exists := datasetMap[dataset.Address]; exists {
			log.Fatalf("duplicate address '%s' found in datasets file", err)
		}
		dataset.alreadyImportedCids = make(map[string]bool)
		datasetMap[dataset.Address] = dataset
	}

	return datasetMap
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

// Get deals that are already imported/completed and save them
// Will only execute once - returns immediately if the list is already populated
func (d *Dataset) PopulateCidsCompleted(boost *BoostConnection) {
	// Only populate once
	if len(d.alreadyImportedCids) != 0 {
		return
	}

	completedDeals := boost.GetDealsCompleted(d.Address)
	log.Debugf("found %d completed deals for dataset %s", len(completedDeals), d.Dataset)

	for _, deal := range completedDeals {
		d.alreadyImportedCids[deal.PieceCid] = true
	}
}

func (d *Dataset) IsCidCompleted(pieceCid string) bool {
	_, exists := d.alreadyImportedCids[pieceCid]
	return exists
}
