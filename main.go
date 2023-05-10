package main

import (
	"fmt"
	"os"

	cmd "github.com/application-research/delta-importer/cmd"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var Commit string
var Version string

func main() {

	commands := cmd.SetupCommands()

	app := &cli.App{
		Commands: commands,
		Usage:    "An application to facilitate importing deals into a Filecoin Storage Provider",
		Version:  fmt.Sprintf("%s+git.%s\n", Version, Commit),
		// Flags:    cmd.CLIConnectFlags,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
