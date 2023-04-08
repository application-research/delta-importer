package main

import (
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		log.Tracef("error finding car file %s: %s", path, err)
		return false
	}
	return true
}

// Returns filename, without extension, given a path
func FileNameFromPath(path string) string {
	fileName := filepath.Base(path)
	fileExt := filepath.Ext(fileName)
	return fileName[0 : len(fileName)-len(fileExt)]
}
