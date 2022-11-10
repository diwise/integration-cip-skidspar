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
	"github.com/diwise/integration-cip-skidspar/get"
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

	brokerURL := env.GetVariableOrDie(logger, "CONTEXT_BROKER_URL", "a valid context broker URL")

	brokerTenant := env.GetVariableOrDefault(logger, "CONTEXT_BROKER_TENANT", "default")
	brokerClientDebug := env.GetVariableOrDefault(logger, "CONTEXT_BROKER_CLIENT_DEBUG", "false")

	cbClient := client.NewContextBrokerClient(brokerURL, client.Tenant(brokerTenant), client.Debug(brokerClientDebug))

	location := env.GetVariableOrDie(logger, "LS_LOCATION", "a valid location for längdspår.se")
	apiKey := env.GetVariableOrDie(logger, "LS_API_KEY", "a valid api key for längdspår.se")

	trailIDFormat := env.GetVariableOrDefault(logger, "NGSI_TRAILID_FORMAT", "%s")
	sportsfieldIDFormat := env.GetVariableOrDefault(logger, "NGSI_SPORTSFIELDID_FORMAT", "%s")

	typeFormats := make(map[string]string)

	if trailIDFormat != "" {
		typeFormats[trailIDFormat] = "ExerciseTrails"
	}
	if sportsfieldIDFormat != "" {
		typeFormats[sportsfieldIDFormat] = "SportsField"
	}

	entities, err := get.EntitiesFromContextBroker(ctx, brokerURL, brokerTenant, typeFormats)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get sportsfields from broker")
	}

	do(ctx, location, apiKey, cbClient, typeFormats, entities)

	logger.Info().Msg("running cleanup ...")
	cleanup()

	time.Sleep(5 * time.Second)
	logger.Info().Msg("done")
}

func do(ctx context.Context, location, apiKey string, cbClient client.ContextBrokerClient, typeFormats, entities map[string]string) {
	var err error

	ctx, span := tracer.Start(ctx, "integrate-status-from-langdspar")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, logging.GetFromContext(ctx), ctx)

	status, err := getEntityStatus(ctx, location, apiKey)
	if err != nil {
		logger.Error().Err(err).Msg("failed to retrieve entity status")
		return
	}

	err = updateEntitiesInBroker(ctx, status, cbClient, typeFormats, entities)
	if err != nil {
		logger.Error().Err(err).Msg("failed to update entity statuses in broker")
	}
}

type Status struct {
	Ski map[string]struct {
		Active          bool   `json:"isActive"`
		ExternalID      string `json:"externalId"`
		LastPreparation string `json:"lastPreparation"`
	} `json:"Ski"`
}

func getEntityStatus(ctx context.Context, location, apiKey string) (*Status, error) {
	var err error

	ctx, span := tracer.Start(ctx, "get-langdspar-status")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	url := fmt.Sprintf("https://xn--lngdspr-5wao.se/api/locations/%s/routes-status.json?apiKey=%s", location, apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		err = fmt.Errorf("failed to create http request: %s", err.Error())
		return nil, err
	}

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		err = fmt.Errorf("failed to request entity status update: %s", err.Error())
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("loading data from %s failed with status %d", url, resp.StatusCode)
		return nil, err
	}

	status := &Status{}

	body, _ := io.ReadAll(resp.Body)

	err = json.Unmarshal(body, status)

	if err != nil {
		err = fmt.Errorf("failed to unmarshal entity status: %s", err.Error())
		return nil, err
	}

	return status, nil
}

func updateEntitiesInBroker(ctx context.Context, status *Status, cbClient client.ContextBrokerClient, typeFormats string) error {

	var err error

	ctx, span := tracer.Start(ctx, "update-broker-status")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	logger := logging.GetFromContext(ctx)

	ngsiLinkHeader := fmt.Sprintf("<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"", entities.DefaultContextURL)

	for k, v := range status.Ski {
		if v.ExternalID != "" {
			entityID := typeFormat + v.ExternalID

			logger.Info().Msgf("found preparation status for %s (%s)", entityID, k)

			headers := map[string][]string{
				"Accept": {"application/ld+json"},
				"Link":   {ngsiLinkHeader},
			}
			entity, err := cbClient.RetrieveEntity(ctx, entityID, headers)
			if err != nil {
				if errors.Is(err, ngsierrors.ErrNotFound) {
					logger.Info().Msg("no such entity in broker")
				} else {
					logger.Error().Err(err).Msgf("failed to retrieve %s", entityID)
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
				logger.Info().Msgf("entity has changed status to %s", currentStatus)
				props = append(props, decorators.Text("status", currentStatus))
			}

			if v.LastPreparation != "" {
				lastPrep, err := time.Parse(time.RFC3339, v.LastPreparation)
				if err != nil {
					logger.Warn().Err(err).Msgf("failed to parse entity preparation timestamp for %s", entityID)
				} else {
					prepTime := lastPrep.Format(time.RFC3339)
					if lastKnownPreparation != prepTime {
						logger.Info().Msg("last known preparation has changed")
						props = append(props, decorators.DateTime("dateLastPreparation", prepTime))
					}
				}
			}

			if len(props) > 0 {
				fragment, _ := entities.NewFragment(props...)

				headers = map[string][]string{"Content-Type": {"application/ld+json"}}
				_, err := cbClient.MergeEntity(ctx, entityID, fragment, headers)
				if err != nil {
					logger.Error().Err(err).Msgf("failed to merge entity %s", entityID)
				}
			} else {
				logger.Info().Msg("neither status nor preparation time has changed")
			}

			time.Sleep(1 * time.Second)
		}
	}

	return nil
}
