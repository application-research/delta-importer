package services

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

type DDMApi struct {
	baseUrl string
	authKey string
}

func NewDDMApi(baseUrl string, authKey string) *DDMApi {
	return &DDMApi{baseUrl: baseUrl, authKey: authKey}
}

type SelfServiceResponse struct {
	Cid string `json:"cid"`
}

// Request DDM for a deal for the provided dataset
// returns the piece CID of the deal if successful
func (d *DDMApi) RequestDealForDataset(dataset string, delayStartEpoch uint, advanceEndEpoch uint) (string, error) {
	delayStart := strconv.FormatUint(uint64(delayStartEpoch), 10)
	advanceEnd := strconv.FormatUint(uint64(advanceEndEpoch), 10)

	resp, closer, err := d.getRequest("/by-dataset/" + dataset + "?start_epoch_delay=" + delayStart + "&end_epoch_advance=" + advanceEnd)

	if err != nil {
		return "", fmt.Errorf("could not get deal for dataset %s: %v", dataset, err)
	}
	defer closer()

	var selfServiceResponse SelfServiceResponse
	err = json.Unmarshal(resp, &selfServiceResponse)
	if err != nil {
		return "", fmt.Errorf("could not unmarshal response %s: %v", resp, err)
	}

	return selfServiceResponse.Cid, nil
}

// Request DDM for a deal for the provided cid
// returns the piece CID of the deal if successful
func (d *DDMApi) RequestDealForCid(cid string, delayStartEpoch uint, advanceEndEpoch uint) (string, error) {
	delayStart := strconv.FormatUint(uint64(delayStartEpoch), 10)
	advanceEnd := strconv.FormatUint(uint64(advanceEndEpoch), 10)

	resp, closer, err := d.getRequest("/by-cid/" + cid + "?start_epoch_delay=" + delayStart + "&end_epoch_advance=" + advanceEnd)

	if err != nil {
		return "", fmt.Errorf("could not get deal for cid %s: %v", cid, err)
	}
	defer closer()

	var selfServiceResponse SelfServiceResponse
	err = json.Unmarshal(resp, &selfServiceResponse)
	if err != nil {
		return "", fmt.Errorf("could not unmarshal response %s: %v", resp, err)
	}

	return selfServiceResponse.Cid, nil
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
