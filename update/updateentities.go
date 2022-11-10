package update

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/integration-cip-skidspar/models"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
)

const serviceName string = "integration-cip-skidspar"

var tracer = otel.Tracer(serviceName + "/update")

func EntitiesInBroker(ctx context.Context, status *models.Status, cbClient client.ContextBrokerClient, storedEntities map[string]models.StoredEntity) error {

	var err error

	ctx, span := tracer.Start(ctx, "update-broker-status")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	logger := logging.GetFromContext(ctx)

	ngsiLinkHeader := fmt.Sprintf("<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"", entities.DefaultContextURL)

	for k, v := range status.Ski {
		if v.ExternalID != "" && v.ExternalID == storedEntities[v.ExternalID].ID {
			entityID := storedEntities[v.ExternalID].Format + v.ExternalID

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
