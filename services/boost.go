package services

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/delta-importer/util"
	bapi "github.com/filecoin-project/boost/api"
	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
	"github.com/machinebox/graphql"
	log "github.com/sirupsen/logrus"
)

type BoostConnection struct {
	bapi  bapi.BoostStruct
	bgql  *graphql.Client
	close jsonrpc.ClientCloser
}

type BoostDeals []Deal

func NewBoostConnection(boostAddress string, boostPort string, gqlPort string, boostAuthToken string) (*BoostConnection, error) {
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
		bapi:  api,
		bgql:  graphqlClient,
		close: close,
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

func (bc *BoostConnection) ImportCar(ctx context.Context, carFile string, pieceCid string, dealUuid uuid.UUID) ImportResult {
	log.Debugf("importing uuid %v from %v", dealUuid, carFile)

	// Deal proposal by deal uuid (v1.2.0 deal)
	// DeleteAfterImport = false
	rej, err := bc.bapi.BoostOfflineDealWithData(ctx, dealUuid, carFile, false)
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

	// Also account for deals that are awaiting publish confirmation
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

	// Add deals that are awaiting publish confirmation- they will soon start sealing
	for _, deal := range graphqlResponsePublished.Data.Deals {
		if deal.Message == "Awaiting Publish Confirmation" {
			inPipeline = append(inPipeline, deal)
		}
	}

	// Also account for deals that are adding to sector
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

	// Add deals that are Adding to Sector - they will soon start sealing
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
