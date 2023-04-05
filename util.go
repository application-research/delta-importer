package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Mapping from client address -> dataset slug -> find in the folder
func GenerateCarFileName(base_directory string, pieceCid string, sourceAddr string) string {
	datasetSlug := viper.GetString(sourceAddr)
	if datasetSlug == "" {
		log.Errorf("unrecognized dataset from addr %s\n", sourceAddr)
		return ""
	}

	return base_directory + "/" + datasetSlug + "/" + pieceCid + ".car"
}

func CarExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		log.Tracef("error finding car file %s: %s", path, err)
		return false
	}
	return true
}
