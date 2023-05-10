package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/urfave/cli/v2"
)

var CLIConnectFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "url",
		Usage:       "url to connect to delta-importer api on",
		Aliases:     []string{"u"},
		Value:       "http://127.0.0.1:1313",
		DefaultText: "http://127.0.0.1:1313",
	},
}

type CmdProcessor struct {
	diUrl string
}

func NewCmdProcessor(c *cli.Context) (*CmdProcessor, error) {
	url := c.String("url")

	err := healthCheck(url)

	if err != nil {
		return nil, fmt.Errorf("unable to communicate with delta-importer daemon: %s", err)
	}

	return &CmdProcessor{
		diUrl: url,
	}, nil
}

// Verify that DI API is reachable
func healthCheck(ddmUrl string) error {
	req, err := http.NewRequest(http.MethodGet, ddmUrl+"/api/v1/health", nil)
	if err != nil {
		return fmt.Errorf("could not construct http request %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("could not make http request %s", err)
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return err
		}

		return fmt.Errorf(string(body))
	}

	return err
}

func (c *CmdProcessor) MakeRequest(method string, url string, raw []byte) ([]byte, func() error, error) {
	req, err := http.NewRequest(method, c.diUrl+url, bytes.NewBuffer(raw))
	if err != nil {
		return nil, nil, fmt.Errorf("could not construct http request %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("could not make http request %s", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return body, resp.Body.Close, nil
}
