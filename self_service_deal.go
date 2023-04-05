package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type DDMApi struct {
	baseUrl string
	authKey string
}

func NewDDMApi(baseUrl string, authKey string) *DDMApi {
	return &DDMApi{baseUrl: baseUrl, authKey: authKey}
}

// Request DDM for a deal for the provided dataset
func (d *DDMApi) RequestDealForDataset(dataset string) (string, error) {
	resp, closer, err := d.getRequest("/by-dataset/" + dataset)

	if err != nil {
		return "", fmt.Errorf("could not get deal for dataset %s: %v", dataset, err)
	}
	defer closer()

	return string(resp), nil
}

// Request DDM for a deal for the provided cid
func (d *DDMApi) RequestDealForCid(cid string) (string, error) {
	resp, closer, err := d.getRequest("/by-cid/" + cid)

	if err != nil {
		return "", fmt.Errorf("could not get deal for cid %s: %v", cid, err)
	}
	defer closer()

	return string(resp), nil
}

func (d *DDMApi) getRequest(url string) ([]byte, func() error, error) {
	req, err := http.NewRequest(http.MethodGet, d.baseUrl+url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("could not construct http request %v", err)
	}

	req.Header.Set("X-DELTA-AUTH", d.authKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("could not execute http request %v", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("error in http call %d : %s", resp.StatusCode, body)
	}

	if err != nil {
		return nil, nil, err
	}

	return body, resp.Body.Close, nil
}
