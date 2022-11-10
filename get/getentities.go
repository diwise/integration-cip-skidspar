package get

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

func EntitiesFromContextBroker(ctx context.Context, brokerURL, tenant string, entityTypes map[string]string) (map[string]string, error) {
	logger := logging.GetFromContext(ctx)

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	storedEntities := make(map[string]string)

	for format, entityType := range entityTypes {
		url := fmt.Sprintf(brokerURL+"/ngsi-ld/v1/entities?type=%s&limit=1000&options=keyValues", entityType)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %s", err.Error())
		}

		req.Header.Add("Link", entities.LinkHeader)

		if tenant != "default" {
			req.Header.Add("NGSILD-Tenant", tenant)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %s", err.Error())
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body: %s", err.Error())
		}

		if resp.StatusCode >= http.StatusBadRequest {
			reqbytes, _ := httputil.DumpRequest(req, false)
			respbytes, _ := httputil.DumpResponse(resp, false)

			logger.Error().Str("request", string(reqbytes)).Str("response", string(respbytes)).Msg("request failed")
			return nil, fmt.Errorf("request failed")
		}

		if resp.StatusCode != http.StatusOK {
			contentType := resp.Header.Get("Content-Type")
			return nil, fmt.Errorf("context source returned status code %d (content-type: %s, body: %s)", resp.StatusCode, contentType, string(respBody))
		}

		type entityDTO struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}

		entities := []entityDTO{}

		err = json.Unmarshal(respBody, &entities)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal entities: %s", err.Error())
		}

		for _, sf := range entities {
			entityID := strings.TrimPrefix(sf.ID, format)
			_, exists := storedEntities[entityID]
			if !exists {
				storedEntities[entityID] = sf.Type
			}
			fmt.Printf("stored type %s for id %s", sf.Type, sf.ID)
		}

		fmt.Printf("stored %d IDs", len(storedEntities))
	}

	return storedEntities, nil

}
