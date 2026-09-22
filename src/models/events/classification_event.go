package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type ClassificationEvent struct {
	Timestamp      time.Time `json:"timestamp"`
	URL            string    `json:"url"`
	Domain         string    `json:"domain"`
	Classification string    `json:"classification"`
	Confidence     float32   `json:"confidence"`
}

func NewClassificationEventFromMessage(timestamp time.Time, message string, headers map[string]string) (*ClassificationEvent, error) {
	var jsonData map[string]interface{}
	err := json.Unmarshal([]byte(message), &jsonData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling json data: %s", err)
	}

	url, ok := jsonData["url"].(string)
	if !ok {
		return nil, fmt.Errorf("error getting url from json data: %s", message)
	}

	domain, ok := jsonData["domain"].(string)
	if !ok {
		return nil, fmt.Errorf("error getting domain from json data: %s", message)
	}

	classification, ok := jsonData["classification"].(string)
	if !ok {
		return nil, fmt.Errorf("error getting classification from json data: %s", message)
	}

	confidenceValue, ok := jsonData["confidence"].(float64)
	if !ok {
		return nil, fmt.Errorf("error getting confidence from json data: %s", message)
	}

	return &ClassificationEvent{
		Timestamp:      timestamp,
		URL:            url,
		Domain:         domain,
		Classification: classification,
		Confidence:     float32(confidenceValue),
	}, nil
}

func (e *ClassificationEvent) String() string {
	printRecord := map[string]interface{}{
		"timestamp":      e.Timestamp.Format(time.RFC3339),
		"url":            e.URL,
		"classification": e.Classification,
		"confidence":     e.Confidence,
	}

	b, err := json.MarshalIndent(printRecord, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
