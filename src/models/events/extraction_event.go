package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type ExtractionEvent struct {
	Timestamp time.Time `json:"timestamp"`
	URL       string    `json:"url"`
	Domain    string    `json:"domain"`
	Source    string    `json:"source"`
}

func NewExtractionEventFromMessage(timestamp time.Time, message string, headers map[string]string) (*ExtractionEvent, error) {
	var jsonData map[string]interface{}
	err := json.Unmarshal([]byte(message), &jsonData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling json data: %s", err)
	}

	url, ok := jsonData["url"].(string)
	if !ok {
		return nil, fmt.Errorf("error getting url from json data: %s", message)
	}

	domain, err := helpers.GetDomainFromJSON(jsonData, message)
	if err != nil {
		return nil, err
	}

	source, ok := jsonData["source"].(string)
	if !ok {
		log.Printf("error getting source from json data: %s", message)
		source = ""
	}

	return &ExtractionEvent{
		Timestamp: timestamp,
		URL:       url,
		Domain:    domain,
		Source:    source,
	}, nil
}

func (e *ExtractionEvent) String() string {
	printRecord := map[string]interface{}{
		"timestamp": e.Timestamp.Format(time.RFC3339),
		"url":       e.URL,
		"domain":    e.Domain,
		"source":    e.Source,
	}

	b, err := json.MarshalIndent(printRecord, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
