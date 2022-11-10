package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/integration-cip-skidspar/get"
	"github.com/diwise/integration-cip-skidspar/models"
	"github.com/diwise/integration-cip-skidspar/update"
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

	do(ctx, location, apiKey, cbClient, entities)

	logger.Info().Msg("running cleanup ...")
	cleanup()

	time.Sleep(5 * time.Second)
	logger.Info().Msg("done")
}

func do(ctx context.Context, location, apiKey string, cbClient client.ContextBrokerClient, entities map[string]models.StoredEntity) {
	var err error

	ctx, span := tracer.Start(ctx, "integrate-status-from-langdspar")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, logging.GetFromContext(ctx), ctx)

	status, err := getEntityStatus(ctx, location, apiKey)
	if err != nil {
		logger.Error().Err(err).Msg("failed to retrieve entity status")
		return
	}

	err = update.EntitiesInBroker(ctx, status, cbClient, entities)
	if err != nil {
		logger.Error().Err(err).Msg("failed to update entity statuses in broker")
	}
}

func getEntityStatus(ctx context.Context, location, apiKey string) (*models.Status, error) {
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

	status := &models.Status{}

	body, _ := io.ReadAll(resp.Body)

	err = json.Unmarshal(body, status)

	if err != nil {
		err = fmt.Errorf("failed to unmarshal entity status: %s", err.Error())
		return nil, err
	}

	return status, nil
}
