package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var storedIDFormats = make(map[string]string)

func GetSportsFields(ctx context.Context, brokerURL string, tenant string) error {
	var err error

	logger := logging.GetFromContext(ctx)

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, brokerURL+"/ngsi-ld/v1/entities?type=SportsField&limit=1000&options=keyValues", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err.Error())
	}

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

	type entityDTO struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}

	sportsfields := []entityDTO{}

	err = json.Unmarshal(respBody, &sportsfields)
	if err != nil {
		return fmt.Errorf("failed to unmarshal sportsfields")
	}

	for _, sf := range sportsfields {
		sportsfieldID := strings.TrimPrefix(sf.ID, sportsFieldIDFormat)
		sfType, exists := storedIDFormats[sportsfieldID]
		if !exists {
			storedIDFormats[sportsfieldID] = sf.Type
		}
		fmt.Printf("stored type %s for id %s", sfType, sf.ID)
	}

	fmt.Printf("stored IDs and type: \n %s\n", storedIDFormats)

	return nil

}

func GetExerciseTrails(ctx context.Context, brokerURL string, tenant string) error {
	var err error

	logger := logging.GetFromContext(ctx)

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, brokerURL+"/ngsi-ld/v1/entities?type=ExerciseTrails&limit=1000&options=keyValues", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err.Error())
	}

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

	type entityDTO struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}

	exercisetrails := []entityDTO{}

	err = json.Unmarshal(respBody, &exercisetrails)
	if err != nil {
		return fmt.Errorf("failed to unmarshal exercise trails")
	}

	for _, sf := range exercisetrails {
		sportsfieldID := strings.TrimPrefix(sf.ID, sportsFieldIDFormat)
		sfType, exists := storedIDFormats[sportsfieldID]
		if !exists {
			storedIDFormats[sportsfieldID] = sf.Type
		}
		fmt.Printf("stored type %s for id %s", sfType, sf.ID)
	}

	fmt.Printf("stored IDs and type: \n %s\n", storedIDFormats)

	return nil

}
