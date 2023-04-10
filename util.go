package main

import (
	"math"
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

const FILECOIN_GENESIS_UNIX_EPOCH = 1598306400

func UnixToHeight(unixEpoch int64) int64 {
	return int64(math.Floor(float64(unixEpoch-FILECOIN_GENESIS_UNIX_EPOCH) / 30))
}

func HeightToUnix(height int64) int64 {
	return (height * 30) + FILECOIN_GENESIS_UNIX_EPOCH
}
