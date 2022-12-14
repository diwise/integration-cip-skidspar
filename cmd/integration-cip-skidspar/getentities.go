package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type entityDTO struct {
	ID                  string `json:"id"`
	DateLastPreparation struct {
		Type  string `json:"@type"`
		Value string `json:"@value"`
	} `json:"dateLastPreparation"`
	Status string `json:"status"`
}

func getExerciseTrails(ctx context.Context, brokerURL, tenant, trailIDFormat string, storeEntity func(entity StoredEntity)) error {
	return getEntities(ctx, brokerURL, tenant, trailIDFormat, "ExerciseTrail", storeEntity)
}

func getSportsFields(ctx context.Context, brokerURL, tenant, sportsfieldIDFormat string, storeEntity func(entity StoredEntity)) error {
	return getEntities(ctx, brokerURL, tenant, sportsfieldIDFormat, "SportsField", storeEntity)
}

func getEntities(ctx context.Context, brokerURL, tenant, entityPrefixFormat, entityType string, storeEntity func(entity StoredEntity)) error {

	logger := logging.GetFromContext(ctx)

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	url := fmt.Sprintf(brokerURL+"/ngsi-ld/v1/entities?type=%s&limit=500&options=keyValues", entityType)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err.Error())
	}

	req.Header.Add("Accept", "application/ld+json")
	req.Header.Add("Link", entities.LinkHeader)

	if tenant != "default" {
		req.Header.Add("NGSILD-Tenant", tenant)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %s", err.Error())
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %s", err.Error())
	}

	if resp.StatusCode >= http.StatusBadRequest {
		reqbytes, _ := httputil.DumpRequest(req, false)
		respbytes, _ := httputil.DumpResponse(resp, false)

		logger.Error().Str("request", string(reqbytes)).Str("response", string(respbytes)).Msg("request failed")
		return fmt.Errorf("request failed")
	}

	if resp.StatusCode != http.StatusOK {
		contentType := resp.Header.Get("Content-Type")
		return fmt.Errorf("context source returned status code %d (content-type: %s, body: %s)", resp.StatusCode, contentType, string(respBody))
	}

	entities := []entityDTO{}

	err = json.Unmarshal(respBody, &entities)
	if err != nil {
		return fmt.Errorf("failed to unmarshal entities: %s", err.Error())
	}

	for _, ntt := range entities {
		entity := StoredEntity{
			ID:                  ntt.ID,
			DateLastPreparation: ntt.DateLastPreparation.Value,
			Status:              ntt.Status,
		}

		storeEntity(entity)
	}

	return nil
}
