package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/urfave/cli/v2"
)

const MIN_SEALING_TIME = time.Duration(4 * time.Hour)

type Config struct {
	BoostAddress     string
	BoostAPIKey      string
	BoostPort        string
	BoostGqlPort     string
	DatasetsFilename string
	Debug            bool
	MaxConcurrent    uint
	Interval         uint
	Mode             Mode
	DDMURL           string
	DDMToken         string
	DbDir            string
	Log              string
}

type Mode string

const (
	ModeDefault     Mode = "default"
	ModePullCID     Mode = "pull-cid"
	ModePullDataset Mode = "pull-dataset"
)

// Pull flags out of cli context and create a config object
func CreateConfig(cctx *cli.Context) (Config, error) {

	config := Config{
		BoostAddress:     cctx.String("boost-url"),
		BoostAPIKey:      cctx.String("boost-auth-token"),
		Debug:            cctx.Bool("debug"),
		DatasetsFilename: cctx.String("datasets"),
		BoostGqlPort:     cctx.String("boost-gql-port"),
		BoostPort:        cctx.String("boost-port"),
		MaxConcurrent:    cctx.Uint("max_concurrent"),
		Interval:         cctx.Uint("interval"),
		Mode:             Mode(cctx.String("mode")),
		DDMURL:           cctx.String("ddm-api"),
		DDMToken:         cctx.String("ddm-token"),
		Log:              cctx.String("log"),
		DbDir:            cctx.String("dbdir"),
	}

	// Validation

	// 1. Validate Mode
	validModes := []Mode{ModeDefault, ModePullCID, ModePullDataset}
	isValidMode := false
	for _, mode := range validModes {
		if config.Mode == mode {
			isValidMode = true
			break
		}
	}
	if !isValidMode {
		return config, errors.New("invalid mode: must be empty, default, pull-cid or pull-dataset")
	}

	// 2. Validate DDMToken and DDMURL when Mode is pull-cid or pull-dataset
	if config.Mode == ModePullCID || config.Mode == ModePullDataset {
		if config.DDMToken == "" {
			return config, errors.New("ddm-token must be supplied when mode is pull-cid or pull-dataset")
		}
		if config.DDMURL == "" {
			return config, errors.New("ddm-api must be supplied when mode is pull-cid or pull-dataset")
		}
	}

	if config.Debug {
		fmt.Printf("config: %+v", config)
	}

	return config, nil
}
