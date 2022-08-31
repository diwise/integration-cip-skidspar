package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

const serviceName string = "integration-cip-skidspar"

var tracer = otel.Tracer(serviceName + "/main")

func main() {

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	brokerURL := env.GetVariableOrDie(logger, "CONTEXT_BROKER_URL", "a valid context broker URL")

	brokerTenant := env.GetVariableOrDefault(logger, "CONTEXT_BROKER_TENANT", "default")
	brokerClientDebug := env.GetVariableOrDefault(logger, "CONTEXT_BROKER_CLIENT_DEBUG", "false")

	cbClient := client.NewContextBrokerClient(brokerURL, client.Tenant(brokerTenant), client.Debug(brokerClientDebug))

	location := env.GetVariableOrDie(logger, "LS_LOCATION", "a valid location for längdspår.se")
	apiKey := env.GetVariableOrDie(logger, "LS_API_KEY", "a valid api key for längdspår.se")

	trailIDFormat := env.GetVariableOrDefault(logger, "NGSI_TRAILID_FORMAT", "%s")

	do(ctx, location, apiKey, cbClient, trailIDFormat)
}

func do(ctx context.Context, location, apiKey string, cbClient client.ContextBrokerClient, trailIDFormat string) {

	var err error

	ctx, span := tracer.Start(ctx, "get-langdspar")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, logging.GetFromContext(ctx), ctx)

	url := fmt.Sprintf("https://xn--lngdspr-5wao.se/api/locations/%s/routes-status.json?apiKey=%s", location, apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create http request")
		return
	}

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		logger.Error().Err(err).Msg("failed to request trail status update")
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error().Msgf("loading data from %s failed with status %d", url, resp.StatusCode)
		return
	}

	status := struct {
		Ski map[string]struct {
			Active          bool   `json:"isActive"`
			ExternalID      string `json:"externalId"`
			LastPreparation string `json:"lastPreparation"`
		} `json:"Ski"`
	}{}

	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &status)

	ngsiLinkHeader := fmt.Sprintf("<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"", entities.DefaultContextURL)

	for k, v := range status.Ski {
		if v.ExternalID != "" {

			trailID := fmt.Sprintf(trailIDFormat, v.ExternalID)
			logger.Info().Msgf("found trail status for %s (%s)", trailID, k)

			headers := map[string][]string{
				"Accept": {"application/ld+json"},
				"Link":   {ngsiLinkHeader},
			}
			entity, err := cbClient.RetrieveEntity(ctx, trailID, headers)
			if err != nil {
				if errors.Is(err, ngsierrors.ErrNotFound) {
					logger.Info().Msg("no such trail in broker")
				} else {
					logger.Error().Err(err).Msgf("failed to retrieve %s", trailID)
				}
				continue
			}

			hasChangedStatus := false
			lastKnownPreparation := ""
			currentStatus := map[bool]string{true: "open", false: "closed"}[v.Active]

			// TODO: Replace this with some clever use of generics to retrieve property values
			err = entity.ForEachAttribute(func(attrType, attrName string, contents any) {
				if attrName == "status" {
					savedStatus, ok := contents.(string)
					if ok && currentStatus != savedStatus {
						hasChangedStatus = true
					}
				} else if attrName == "dateLastPreparation" {
					b, _ := json.Marshal(contents)
					property := struct {
						Value struct {
							Timestamp string `json:"@value"`
						} `json:"value"`
					}{}
					err = json.Unmarshal(b, &property)
					if err != nil {
						logger.Error().Err(err).Msg("failed to unmarshal date time property")
					} else {
						lastKnownPreparation = property.Value.Timestamp
					}
				}
			})

			if err != nil {
				logger.Error().Err(err).Msg("failed to iterate over entity attributes")
				continue
			}

			props := []entities.EntityDecoratorFunc{}

			if hasChangedStatus {
				logger.Info().Msgf("trail has changed status to %s", currentStatus)
				props = append(props, decorators.Text("status", currentStatus))
			}

			lastPrep, err := time.Parse(time.RFC3339, v.LastPreparation)
			if err != nil {
				logger.Warn().Err(err).Msgf("failed to parse trail preparation timestamp for %s", trailID)
			} else {
				prepTime := lastPrep.Format(time.RFC3339)
				if lastKnownPreparation != prepTime {
					logger.Info().Msg("last known preparation has changed")
					props = append(props, decorators.DateTime("dateLastPreparation", prepTime))
				}
			}

			if len(props) > 0 {
				fragment, _ := entities.NewFragment(props...)

				headers = map[string][]string{"Content-Type": {"application/ld+json"}}
				_, err := cbClient.MergeEntity(ctx, trailID, fragment, headers)
				if err != nil {
					logger.Error().Err(err).Msgf("failed to merge entity %s", trailID)
				}
			} else {
				logger.Info().Msg("neither status nor preparation time has changed")
			}

			time.Sleep(1 * time.Second)
		}
	}
}
