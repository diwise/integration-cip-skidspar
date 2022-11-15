package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/test"
	"github.com/matryer/is"
)

func TestUpdateEntities(t *testing.T) {
	is := is.New(t)

	status := Status{
		Ski: map[string]struct {
			Active          bool   "json:\"isActive\""
			ExternalID      string "json:\"externalId\""
			LastPreparation string "json:\"lastPreparation\""
		}{
			"Kallaspåret:123": {
				Active:          true,
				ExternalID:      "123",
				LastPreparation: "2021-12-17T16:54:02Z",
			},
		},
	}

	var fragmentJSON string

	cbClient := &test.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			fragmentBytes, _ := json.Marshal(fragment)
			fragmentJSON = string(fragmentBytes)
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	returnStoredEntity := func(externalID string) (StoredEntity, error) {

		return StoredEntity{
			ID:                  "Kallaspåret:123",
			DateLastPreparation: "",
			Status:              "",
		}, nil
	}

	err := UpdateEntitiesInBroker(context.Background(), &status, cbClient, returnStoredEntity)
	is.NoErr(err)
	is.Equal(len(cbClient.MergeEntityCalls()), 1)
	is.True(strings.Contains(fragmentJSON, "2021-12-17T16:54:02Z"))
}

//test what happens if active is true or false, test for when no property has changed
