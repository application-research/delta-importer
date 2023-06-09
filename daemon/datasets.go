package daemon

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	svc "github.com/application-research/delta-importer/services"
	util "github.com/application-research/delta-importer/util"
	log "github.com/sirupsen/logrus"
)

type Dataset struct {
	Dataset             string          `json:"dataset"`
	Addresses           []string        `json:"address"`
	Dir                 string          `json:"dir"`
	Ignore              bool            `json:"ignore,omitempty"`
	alreadyImportedCids map[string]bool `json:"omitempty"`
}

// Read the datasets file and return a map of Dataset structs keyed by their Dataset name
func ReadInDatasetsFromFile(fileName string) map[string]Dataset {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		if err.Error() == "open "+fileName+": no such file or directory" {
			fmt.Println(">> delta-importer can't seem to find the " + util.Purple + "datasets.json" + util.Reset + " file. it should be located at " + util.Cyan + fileName + util.Reset + ". please populate this file and try again. see the README for more information.")
			os.Exit(1)
		} else {
			log.Fatalf("error reading datasets file at %s", fileName)
		}
	}

	var datasets []Dataset
	err = json.Unmarshal(data, &datasets)
	if err != nil {
		log.Fatalf("datasets file is in incorrect format: %v", err)
	}

	datasetMap := make(map[string]Dataset)
	for _, dataset := range datasets {
		if dataset.Ignore {
			continue
		}

		if _, exists := datasetMap[dataset.Dataset]; exists {
			log.Fatalf("duplicate dataset name '%s' found in datasets file", dataset.Dataset)
		}
		dataset.alreadyImportedCids = make(map[string]bool)
		datasetMap[dataset.Dataset] = dataset
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
func (d *Dataset) PopulateAlreadyImportedCids(boost *svc.BoostConnection) {
	// Only populate once
	if len(d.alreadyImportedCids) != 0 {
		return
	}

	var completedDeals svc.BoostDeals

	for _, addr := range d.Addresses {
		cd := boost.GetDealsCompleted(addr)
		completedDeals = append(completedDeals, cd...)

	}
	log.Debugf("found %d completed deals for dataset %s", len(completedDeals), d.Dataset)

	for _, deal := range completedDeals {
		d.alreadyImportedCids[deal.PieceCid] = true
	}

	inProgressDeals := boost.GetDealsInPipeline()
	log.Debugf("found %d in-progress deals", len(inProgressDeals))
	for _, deal := range inProgressDeals {
		d.alreadyImportedCids[deal.PieceCid] = true
	}
}

func (d *Dataset) IsCidAlreadyImported(pieceCid string) bool {
	_, exists := d.alreadyImportedCids[pieceCid]
	return exists
}
