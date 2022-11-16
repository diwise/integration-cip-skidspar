package main

import (
	"context"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
)

func UpdateEntitiesInBroker(ctx context.Context, status *Status, cbClient client.ContextBrokerClient, entityStatus func(string) (StoredEntity, error)) error {
	var err error

	ctx, span := tracer.Start(ctx, "update-broker-status")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	logger := logging.GetFromContext(ctx)

	for k, v := range status.Ski {
		if v.ExternalID != "" {
			storedEntity, err := entityStatus(v.ExternalID)
			if err != nil {
				logger.Warn().Err(err).Msgf("entity %s not found", k)
				continue
			}

			logger.Info().Msgf("found preparation status for %s (%s)", v.ExternalID, k)

			hasChangedStatus := false
			currentStatus := map[bool]string{true: "open", false: "closed"}[v.Active]
			lastKnownPreparation := storedEntity.DateLastPreparation

			if storedEntity.Status != "" && storedEntity.Status != currentStatus {
				hasChangedStatus = true
			}

			props := []entities.EntityDecoratorFunc{}

			if hasChangedStatus {
				logger.Info().Msgf("entity has changed status to %s", currentStatus)
				props = append(props, decorators.Text("status", currentStatus))
			}

			if v.LastPreparation != "" && v.LastPreparation != lastKnownPreparation {
				lastPrep, err := time.Parse(time.RFC3339, v.LastPreparation)
				if err != nil {
					logger.Warn().Err(err).Msgf("failed to parse entity preparation timestamp for %s", storedEntity.ID)
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

				headers := map[string][]string{"Content-Type": {"application/ld+json"}}
				_, err := cbClient.MergeEntity(ctx, storedEntity.ID, fragment, headers)
				if err != nil {
					logger.Error().Err(err).Msgf("failed to merge entity %s", storedEntity.ID)
					return err
				}
			} else {
				logger.Info().Msg("neither status nor preparation time has changed")
			}

			time.Sleep(1 * time.Second)

		}
	}

	return err
}
