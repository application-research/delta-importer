package main

import (
	"encoding/json"
	"regexp"

	log "github.com/sirupsen/logrus"
)

func UnmarshalDeals(data []byte) (Deals, error) {
	var r Deals
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *Deals) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type Deals struct {
	Data Data `json:"data"`
}

type Data struct {
	Deals DealsClass `json:"deals"`
}

type DealsClass struct {
	Deals []Deal `json:"deals"`
}

type Deal struct {
	ID              string `json:"ID"`
	CreatedAt       string `json:"CreatedAt"`
	Message         string `json:"Message"`
	PieceCid        string `json:"PieceCid"`
	IsOffline       bool   `json:"IsOffline"`
	ClientAddress   string `json:"ClientAddress"`
	Checkpoint      string `json:"Checkpoint"`
	InboundFilePath string `json:"InboundFilePath"`
	Err             string `json:"Err"`
}

// checks if there are failed deals in a given array of deals
func HasFailedDeals(ds []Deal) bool {
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
