package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestGetEntitiesFromCtxBroker(t *testing.T) {
	is := is.New(t)
	server := setupMockServiceThatReturns(http.StatusOK)

	types := map[string]string{
		"sportsfieldsFormat":  "SportsField",
		"exercisetrailFormat": "ExerciseTrail",
	}

	entities, err := GetEntitiesFromContextBroker(context.Background(), server.URL, "default", types)
	is.NoErr(err)
	is.Equal(len(entities), 2)
}

func setupMockServiceThatReturns(responseCode int, headers ...func(w http.ResponseWriter)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RequestURI(), "SportsField") {
			for _, applyHeaderTo := range headers {
				applyHeaderTo(w)
			}

			w.WriteHeader(responseCode)
			w.Write([]byte(sportsfieldsTestData))

		} else if strings.Contains(r.URL.RequestURI(), "ExerciseTrail") {
			for _, applyHeaderTo := range headers {
				applyHeaderTo(w)
			}

			w.WriteHeader(responseCode)
			w.Write([]byte(exerciseTrailsTestData))
		}
	}))
}

const sportsfieldsTestData string = `[{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"id":"urn:ngsi-ld:SportsField:se:sundsvall:facilities:796","category":{"type":"Property","value":["skating","floodlit","ice-rink"]},"dateCreated":{"type":"Property","value":{"@type":"DateTime","@value":"2019-10-15T16:15:32Z"}},"dateModified":{"type":"Property","value":{"@type":"DateTime","@value":"2021-12-17T16:54:02Z"}},"description":{"type":"Property","value":"7-manna grusplan intill skolan. Vintertid spolas och snöröjs isbanan en gång i veckan."},"location":{"type":"GeoProperty","value":{"type":"MultiPolygon","coordinates":[[[[17.428771593881844,62.42103804538807],[17.428785133659883,62.421037809376244],[17.428821575900738,62.42048396661722],[17.428101436027845,62.42046508568337],[17.428025378913084,62.42103219129709],[17.428365400350206,62.421045125144],[17.428690864217362,62.421045739009976],[17.428771593881844,62.42103804538807]]]]}},"name":{"type":"Property","value":"Skolans grusplan och isbana"},"source":{"type":"Property","value":"http://127.0.0.1:60519/get/796"},"type":"SportsField"}]`

const exerciseTrailsTestData string = `[{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonl"],"areaServed":"Motionsspår Södra spårområdet","category":["floodlit","ski-classic","ski-skate"],"dateCreated":{"@type":"DateTime","@value":"2019-01-23T09:19:21Z"},"dateLastPreparation":{"@type":"DateTime","@value":"2022-04-27T04:07:15Z"},"dateModified":{"@type":"DateTime","@value":"2022-04-19T21:07:15Z"},"description":"Motionsspår med 3 meter bred asfalt för rullskidor, samt 1,5 meter bred grusbädd för promenad\\/löpning\\/cykling. Vintertid enbart skidåkning, med 3 meter skateyta och dubbla klassiska spår. Konstsnöbeläggs.","id":"urn:ngsi-ld:ExerciseTrail:se:sundsvall:facilities:650","length":0.9,"difficulty":0.5,"paymentRequired":"yes","location":{"type":"LineString","coordinates":[[17.308707,62.366359],[17.308765,62.366428],[17.308771,62.366531],[17.308721,62.366609],[17.308607,62.366663],[17.308441,62.366694],[17.308383,62.366694],[17.306906,62.366586],[17.306088,62.366397],[17.305202,62.36618],[17.305029,62.366122],[17.305029,62.366122],[17.304897,62.366023],[17.304829,62.365974],[17.304692,62.365921],[17.304495,62.365868],[17.304495,62.365868],[17.304302,62.365838],[17.30413,62.365807],[17.304103,62.365803],[17.303955,62.365777],[17.303795,62.365758],[17.303445,62.365722],[17.303445,62.365722],[17.303193,62.365681],[17.303048,62.365625],[17.302889,62.365539],[17.302652,62.365347],[17.302417,62.365164],[17.302352,62.365114],[17.302342,62.36508],[17.302342,62.36508],[17.302349,62.36497],[17.302389,62.364906],[17.302561,62.36475],[17.302561,62.36475],[17.302733,62.364548],[17.302854,62.364399],[17.302945,62.364329],[17.303098,62.364281],[17.303098,62.364281],[17.303135,62.364279],[17.303293,62.364284],[17.303298,62.364287],[17.303378,62.364292],[17.303378,62.364292],[17.303461,62.364392],[17.303484,62.364483],[17.303446,62.364708],[17.303448,62.364766],[17.303445,62.364809],[17.303441,62.364884],[17.303458,62.36506],[17.303504,62.365172],[17.303635,62.365288],[17.304366,62.365658],[17.30484,62.365838],[17.305007,62.365893],[17.305228,62.365953],[17.305228,62.365953],[17.305378,62.365992],[17.305508,62.366025],[17.305609,62.366038],[17.305609,62.366038],[17.306497,62.366135],[17.307302,62.366214],[17.307726,62.366252],[17.308231,62.366291],[17.308619,62.366329],[17.308707,62.366359]]},"name":"Motion 1 km Kallaspåret","source":"https://api.sundsvall.se/facilities/2.1/get/650","status":"closed","type":"ExerciseTrail"}]`
