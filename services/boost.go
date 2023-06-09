package services

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/application-research/delta-importer/util"
	bapi "github.com/filecoin-project/boost/api"
	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
	"github.com/machinebox/graphql"
	log "github.com/sirupsen/logrus"
)

type BoostConnection struct {
	bapi              bapi.BoostStruct
	bgql              *graphql.Client
	close             jsonrpc.ClientCloser
	stagingDir        string
	deleteAfterImport bool
}

type BoostDeals []Deal

func NewBoostConnection(boostAddress string, boostPort string, gqlPort string, boostAuthToken string, stagingDir string, deleteAfterImport bool) (*BoostConnection, error) {
	headers := http.Header{"Authorization": []string{"Bearer " + boostAuthToken}}
	ctx := context.Background()

	var api bapi.BoostStruct
	close, err := jsonrpc.NewMergeClient(ctx, "http://"+boostAddress+":"+boostPort+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, headers)
	if err != nil {
		return nil, err
	}

	graphqlClient := graphql.NewClient("http://" + boostAddress + ":" + gqlPort + "/graphql/query")
	// Comment in to see detailed gql debugging - produces lots of output
	// graphqlClient.Log = func(s string) { log.Debug(s) }

	bc := &BoostConnection{
		bapi:              api,
		bgql:              graphqlClient,
		close:             close,
		stagingDir:        stagingDir,
		deleteAfterImport: deleteAfterImport,
	}

	return bc, nil
}

func (bc *BoostConnection) Close() {
	bc.close()
}

type ImportResult struct {
	Successful bool
	DealUuid   string
	CommP      string
	FileSize   int64
	Message    string
}

// ImportCar imports a car file into boost
// Returns the deal uuid, commP, and whether the import was successful along with any error message
// If stagingDir is set, the car file will be copied to the staging dir before being imported
func (bc *BoostConnection) ImportCar(ctx context.Context, carFile string, pieceCid string, dealUuid uuid.UUID) ImportResult {
	log.Debugf("importing uuid %v from %v", dealUuid, carFile)
	inStaging := false

	if bc.stagingDir != "" {
		// Copy car file to staging dir
		stagingFile := filepath.Join(bc.stagingDir, pieceCid+".car")
		log.Debugf("copying car file to staging dir %s", stagingFile)
		err := util.CopyFile(carFile, stagingFile)
		if err != nil {
			log.Fatalf("failed to copy car file to staging dir: %s", err)
		}

		carFile = stagingFile
		inStaging = true
	}

	// Always delete from staging dir. If not in staging but `deleteAfterImport` set, then make Boost delete it
	shouldDelete := bc.deleteAfterImport || inStaging

	// Deal proposal by deal uuid (v1.2.0 deal)
	// DeleteAfterImport true if the carfile is in the staging dir, otherwise false
	rej, err := bc.bapi.BoostOfflineDealWithData(ctx, dealUuid, carFile, shouldDelete)
	if err != nil {
		log.Errorf("failed to execute offline deal: %s", err)
		return ImportResult{
			Successful: false,
			DealUuid:   dealUuid.String(),
			CommP:      pieceCid,
			FileSize:   util.FileSize(carFile),
			Message:    err.Error(),
		}
	}
	if rej != nil && rej.Reason != "" {
		log.Errorf("offline deal %s rejected: %s", dealUuid, rej.Reason)
		return ImportResult{
			Successful: false,
			DealUuid:   dealUuid.String(),
			CommP:      pieceCid,
			FileSize:   util.FileSize(carFile),
			Message:    err.Error(),
		}
	}

	log.Printf("offline import for deal UUID "+util.Purple+"%s"+util.Reset+" successful!", dealUuid)

	// Remove the source carfile - staging dir will be taken care of by the `shouldDelete` flag
	if bc.deleteAfterImport && inStaging {
		log.Debugf("deleting car file %s", carFile)
		err = util.DeleteFile(carFile)
		if err != nil {
			log.Errorf("failed to delete car file: %s", err)
		}
	}

	return ImportResult{
		Successful: true,
		DealUuid:   dealUuid.String(),
		CommP:      pieceCid,
		FileSize:   util.FileSize(carFile),
		Message:    "",
	}
}

// Get a deal by its ID
func (bc *BoostConnection) GetDeal(dealID string) (Deal, error) {
	graphqlRequest := graphql.NewRequest(fmt.Sprintf(`
	{
			deals(query: "%s") {
				deals {
					ID
					Message
					PieceCid
					IsOffline
					ClientAddress
					Checkpoint
					StartEpoch
					InboundFilePath
					Err
				}
			}
	}
	`, dealID))

	// ? for some reason, this graphql library is incapable of unmarshaling a single deal - it always returns the zero value
	// Thus, we aren't able to do the simpler `deal (id: %s) {...}` query, as it always results in an empty result
	// This must be a bug, so as a workaround we run a `deals` query using the ID, and take the first (and only) result
	var graphqlResponse DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
		return Deal{}, err
	}

	if len(graphqlResponse.Data.Deals) < 1 {
		return Deal{}, fmt.Errorf("deal %s not found", dealID)
	}

	return graphqlResponse.Data.Deals[0], nil
}

// Get deals that are offiline, in the "accepted" state, and not yet imported
// Clientaddress can be used to filter the deals, but is not required (will return all deals)
// Note: limits to 100 deals
func (bc *BoostConnection) GetDealsAwaitingImport(clientAddress []string) BoostDeals {
	var toImport []Deal

	for _, address := range clientAddress {
		graphqlRequest := graphql.NewRequest(fmt.Sprintf(`
	{
		deals(filter: {Checkpoint: Accepted, IsOffline: true}, query: "%s", limit: 100) {
			deals {
				ID
				Message
				PieceCid
				IsOffline
				ClientAddress
				Checkpoint
				StartEpoch
				InboundFilePath
				Err
			}
		}
	}
	`, address))

		var graphqlResponse DealsResponseJson
		if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
			panic(err)
		}

		for _, deal := range graphqlResponse.Data.Deals {
			// Only check:
			// - Deals where the inbound path has not been set (has not been imported yet)
			// - Deals that are not running CommP verification (this indicates they have already been imported)
			if deal.InboundFilePath == "" && deal.Message != "Verifying Commp" {
				toImport = append(toImport, deal)
			}
		}

	}

	return toImport
}

func (bc *BoostConnection) GetDealsCompleted(clientAddress string) BoostDeals {
	graphqlRequest := graphql.NewRequest(fmt.Sprintf(`
	{
		deals(filter: {Checkpoint: IndexedAndAnnounced}, query: "%s", limit: 1000000) {
			deals {
				ID
				Message
				PieceCid
			}
		}
	}
	`, clientAddress))

	var graphqlResponseCompleted DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponseCompleted); err != nil {
		panic(err)
	}

	var completed []Deal
	for _, deal := range graphqlResponseCompleted.Data.Deals {
		if deal.Message == "Sealer: Proving" {
			completed = append(completed, deal)
		}
	}

	return completed
}

func (bc *BoostConnection) GetDealsInPipeline() BoostDeals {
	var inPipeline []Deal

	// Deals in active sealing PC1/PC2, Verifying CommP etc
	graphqlRequestSealing := graphql.NewRequest(`
	{
		deals(filter: {Checkpoint: IndexedAndAnnounced}, limit: 2000) {
			deals {
				ID
				Message
				PieceCid
			}
		}
	}
	`)
	var graphqlResponseSealing DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequestSealing, &graphqlResponseSealing); err != nil {
		panic(err)
	}
	for _, deal := range graphqlResponseSealing.Data.Deals {
		// Disregard deals that are complete (proving), removed, or failed to terminate as they are not in the pipeline
		if deal.Message != "Sealer: Proving" && deal.Message != "Sealer: Removed" && deal.Message != "Sealer: TerminateFailed" {
			inPipeline = append(inPipeline, deal)
		}
	}

	// Deals awaiting publish
	graphqlRequestTransferred := graphql.NewRequest(`
	{
		deals(filter: {Checkpoint: Transferred}, limit: 2000) {
			deals {
				ID
				Message
				PieceCid
			}
		}
	}
	`)
	var graphqlResponseTransferred DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequestTransferred, &graphqlResponseTransferred); err != nil {
		panic(err)
	}
	for _, deal := range graphqlResponseTransferred.Data.Deals {
		if deal.Message == "Ready to Publish" {
			inPipeline = append(inPipeline, deal)
		}
	}

	// Deals awaiting PSD Confirmation
	graphqlRequestPublished := graphql.NewRequest(`
	{
		deals(filter: {Checkpoint: Published}, limit: 2000) {
			deals {
				ID
				Message
				PieceCid
			}
		}
	}
	`)
	var graphqlResponsePublished DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequestPublished, &graphqlResponsePublished); err != nil {
		panic(err)
	}
	for _, deal := range graphqlResponsePublished.Data.Deals {
		if deal.Message == "Awaiting Publish Confirmation" {
			inPipeline = append(inPipeline, deal)
		}
	}

	// Deals in Adding to Sector
	graphqlRequestPublishConfirmed := graphql.NewRequest(`
		{
			deals(filter: {Checkpoint: PublishConfirmed}, limit: 2000) {
				deals {
					ID
					Message
					PieceCid
				}
			}
		}
		`)
	var graphqlResponsePublishConfirmed DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequestPublishConfirmed, &graphqlResponsePublishConfirmed); err != nil {
		panic(err)
	}
	for _, deal := range graphqlResponsePublishConfirmed.Data.Deals {
		if deal.Message == "Adding to Sector" {
			inPipeline = append(inPipeline, deal)
		}
	}

	return inPipeline
}

// Queries boost for deals that match a given CID - useful to check if there are other failed ones
func (bc *BoostConnection) GetDealsForContent(cid string) Deals {
	graphqlRequest := graphql.NewRequest(fmt.Sprintf(`
	{
		deals(query: "%s", limit: 5) {
			deals {
				ID
				Message
				PieceCid
				IsOffline
				ClientAddress
				Checkpoint
				InboundFilePath
				Err
			}
		}
	}
	`, cid))

	var graphqlResponse DealsResponseJson
	if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
		panic(err)
	}

	return graphqlResponse.Data.Deals
}

// Repeatedly attempts to wait, then query for a CID, returning an error if not found after 3 retries
// Use this after requesting a deal, to allow time for it to be made with Boost
func (bc *BoostConnection) WaitForDeal(pieceCid string) ([]Deal, error) {
	retryCount := 1
	var readyToImport []Deal

whileLoop:
	for {
		if retryCount > 3 {
			return readyToImport, errors.New("deal not made after 3 retries")
		}

		time.Sleep(time.Second * 10 * time.Duration(retryCount))
		// Check to see if the deals has been made
		deals := bc.GetDealsForContent(pieceCid)
		readyToImport = deals.ReadyForImport()

		if len(readyToImport) > 0 {
			break whileLoop
		} else {
			log.Debugf("deal for %d not seen in boost yet. retrying", pieceCid)
			retryCount++
		}
	}

	return readyToImport, nil
}
