package main

type Status struct {
	Ski map[string]struct {
		Active          bool   `json:"isActive"`
		ExternalID      string `json:"externalId"`
		LastPreparation string `json:"lastPreparation"`
	} `json:"Ski"`
}
