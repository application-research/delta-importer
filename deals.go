package main

import "encoding/json"

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
