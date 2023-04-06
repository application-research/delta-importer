package main

import (
	"os"

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
