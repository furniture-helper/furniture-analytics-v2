package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type MinimizerEvent struct {
	Timestamp     time.Time `json:"timestamp"`
	URL           string    `json:"url"`
	Domain        string    `json:"domain"`
	OriginalSize  int64     `json:"original_size"`
	MinimizedSize int64     `json:"minimized_size"`
}

func NewMinimizerEventFromMessage(timestamp time.Time, message string, headers map[string]string) (*MinimizerEvent, error) {
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

	originalSize, ok := jsonData["original_size_bytes"].(float64)
	if !ok {
		return nil, fmt.Errorf("error getting original_size from json data: %s", message)
	}

	minimizedSize, ok := jsonData["minimized_size_bytes"].(float64)
	if !ok {
		return nil, fmt.Errorf("error getting minimized_size from json data: %s", message)
	}

	return &MinimizerEvent{
		Timestamp:     timestamp,
		URL:           url,
		Domain:        domain,
		OriginalSize:  int64(originalSize),
		MinimizedSize: int64(minimizedSize),
	}, nil
}

func (e *MinimizerEvent) String() string {
	printRecord := map[string]interface{}{
		"timestamp":      e.Timestamp.Format(time.RFC3339),
		"url":            e.URL,
		"original_size":  e.OriginalSize,
		"minimized_size": e.MinimizedSize,
	}

	b, err := json.MarshalIndent(printRecord, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
