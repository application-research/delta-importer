package main

import (
	"context"
	"fmt"
	"net/http"

	bapi "github.com/filecoin-project/boost/api"
	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
	"github.com/machinebox/graphql"
	log "github.com/sirupsen/logrus"
)

type BoostConnection struct {
	bapi   bapi.BoostStruct
	bgql   *graphql.Client
	closer jsonrpc.ClientCloser
}

type BoostDeals []Deal

func NewBoostConnection(boostAddress string, boostPort string, gqlPort string, boostAuthToken string) (*BoostConnection, error) {
	headers := http.Header{"Authorization": []string{"Bearer " + boostAuthToken}}
	ctx := context.Background()

	var api bapi.BoostStruct
	closer, err := jsonrpc.NewMergeClient(ctx, "http://"+boostAddress+":"+boostPort+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, headers)
	if err != nil {
		return nil, err
	}

	graphqlClient := graphql.NewClient("http://" + boostAddress + ":" + gqlPort + "/graphql/query")

	bc := &BoostConnection{
		bapi:   api,
		bgql:   graphqlClient,
		closer: closer,
	}

	return bc, nil
}

func (bc *BoostConnection) Close() {
	bc.closer()
}

func (bc *BoostConnection) ImportCar(ctx context.Context, carFile string, dealUuid uuid.UUID) bool {
	// Deal proposal by deal uuid (v1.2.0 deal)
	rej, err := bc.bapi.BoostOfflineDealWithData(ctx, dealUuid, carFile)
	if err != nil {
		log.Errorf("failed to execute offline deal: %w", err)
		return false
	}
	if rej != nil && rej.Reason != "" {
		log.Errorf("offline deal %s rejected: %s", dealUuid, rej.Reason)
		return false
	}

	log.Printf("Offline deal import for v1.2.0 deal %s scheduled for execution \n", dealUuid)

	return true
}

func (bc *BoostConnection) GetDealsAwaitingImport() BoostDeals {
	graphqlRequest := graphql.NewRequest(`
	{
		deals(filter: {Checkpoint: Accepted, IsOffline: true}, limit: 300) {
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
	`)
	var graphqlResponse Data
	if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
		panic(err)
	}

	var toImport []Deal

	for _, deal := range graphqlResponse.Deals.Deals {
		// Only check:
		// - Deals where the inbound path has not been set (has not been imported yet)
		if deal.InboundFilePath == "" {
			toImport = append(toImport, deal)
		}
	}

	return toImport
}

func (bc *BoostConnection) GetDealsInPipeline() BoostDeals {
	graphqlRequest := graphql.NewRequest(`
	{
		deals(filter: {Checkpoint: IndexedAndAnnounced}, limit: 2000) {
			deals {
				ID
				Message
			}
		}
	}
	`)
	var graphqlResponse Data
	if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
		panic(err)
	}

	var inPipeline []Deal
	for _, deal := range graphqlResponse.Deals.Deals {
		// Disregard deals that are complete (proving)
		if deal.Message != "Sealer: Proving" {
			inPipeline = append(inPipeline, deal)
		}
	}

	return inPipeline
}

// Queries boost for deals that match a given CID - useful to check if there are other failed ones
func (bc *BoostConnection) GetDealsForContent(cid string) []Deal {
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

	var graphqlResponse Data
	if err := bc.bgql.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
		panic(err)
	}

	return graphqlResponse.Deals.Deals
}
