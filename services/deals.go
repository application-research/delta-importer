package services

import (
	"regexp"
	"strconv"

	"github.com/application-research/delta-importer/util"

	log "github.com/sirupsen/logrus"
)

type DealsResponseJson struct {
	Data DealsResponseData `json:"deals"`
}

type DealsResponseData struct {
	Deals Deals `json:"deals"`
}

type Deals []Deal

type Deal struct {
	ID              string     `json:"ID"`
	Message         string     `json:"Message"`
	PieceCid        string     `json:"PieceCid"`
	IsOffline       bool       `json:"IsOffline"`
	ClientAddress   string     `json:"ClientAddress"`
	Checkpoint      string     `json:"Checkpoint"`
	StartEpoch      BoostEpoch `json:"StartEpoch"`
	InboundFilePath string     `json:"InboundFilePath"`
	Err             string     `json:"Err"`
}

type BoostEpoch struct {
	TypeName string `json:"__typename"`
	Value    string `json:"n"`
}

func (epoch *BoostEpoch) IntoUnix() int64 {
	i, err := strconv.ParseInt(epoch.Value, 10, 64)
	if err != nil {
		log.Error("could not parse epoch: " + err.Error())
		return 0
	}

	return util.HeightToUnix(i)
}

// checks if there are failed deals in a given array of deals
func (ds Deals) HasMismatchedCommPErrors() bool {
	failed := false
	re, err := regexp.Compile(`.*commp mismatch.*`)
	if err != nil {
		log.Error("could not compile regex: " + err.Error())
		return false
	}

	for _, d := range ds {
		isCommpMismatch := re.MatchString(d.Err)

		if isCommpMismatch {
			failed = true
			break
		}
	}

	return failed
}

func (ds Deals) ReadyForImport() []Deal {
	var toImport []Deal

	for _, d := range ds {
		if d.Checkpoint == "Accepted" && d.IsOffline {
			toImport = append(toImport, d)
		}
	}

	return toImport
}
