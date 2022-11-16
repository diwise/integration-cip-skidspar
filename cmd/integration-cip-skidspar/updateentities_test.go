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

func TestUpdateEntitiesWithNewPreparationDate(t *testing.T) {
	is, status, storedEntity := testSetup(t, true, "123", "2021-12-17T16:54:02Z", "Kallaspåret:123", "", "")

	var fragmentJSON string

	cbClient := &test.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			fragmentBytes, _ := json.Marshal(fragment)
			fragmentJSON = string(fragmentBytes)
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	returnStoredEntity := func(externalID string) (StoredEntity, error) {
		return storedEntity, nil
	}

	err := UpdateEntitiesInBroker(context.Background(), &status, cbClient, returnStoredEntity)
	is.NoErr(err)
	is.Equal(len(cbClient.MergeEntityCalls()), 1)
	is.True(strings.Contains(fragmentJSON, "2021-12-17T16:54:02Z"))
}

func TestUpdateEntitiesWithNoStatusChange(t *testing.T) {
	is, status, storedEntity := testSetup(t, false, "123", "2021-12-17T16:54:02Z", "Kallaspåret:123", "", "closed")

	var fragmentJSON string

	cbClient := &test.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			fragmentBytes, _ := json.Marshal(fragment)
			fragmentJSON = string(fragmentBytes)
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	returnStoredEntity := func(externalID string) (StoredEntity, error) {
		return storedEntity, nil
	}

	err := UpdateEntitiesInBroker(context.Background(), &status, cbClient, returnStoredEntity)
	is.NoErr(err)
	is.Equal(len(cbClient.MergeEntityCalls()), 1)
	is.True(!strings.Contains(fragmentJSON, "status: \"closed\"")) // status should not be included as it has not changed
}

func TestUpdateEntitiesWithNoPropertiesChanged(t *testing.T) {
	is, status, storedEntity := testSetup(t, false, "123", "2021-12-17T16:54:02Z", "Kallaspåret:123", "2021-12-17T16:54:02Z", "closed")

	cbClient := &test.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	returnStoredEntity := func(externalID string) (StoredEntity, error) {
		return storedEntity, nil
	}

	err := UpdateEntitiesInBroker(context.Background(), &status, cbClient, returnStoredEntity)
	is.NoErr(err)
	is.Equal(len(cbClient.MergeEntityCalls()), 0) // nothing has changed, so MergeEntity should not have been called
}

func testSetup(t *testing.T, active bool, externalID, lastPrepared, storedID, storedLastPreparation, storedStatus string) (*is.I, Status, StoredEntity) {
	is := is.New(t)

	status := Status{
		Ski: map[string]struct {
			Active          bool   "json:\"isActive\""
			ExternalID      string "json:\"externalId\""
			LastPreparation string "json:\"lastPreparation\""
		}{
			"Kallaspåret:123": {
				Active:          active,
				ExternalID:      externalID,
				LastPreparation: lastPrepared,
			},
		},
	}

	se := StoredEntity{
		ID:                  storedID,
		DateLastPreparation: storedLastPreparation,
		Status:              storedStatus,
	}

	return is, status, se

}
