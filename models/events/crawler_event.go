package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type CrawlerEventStatus int

const (
	Success CrawlerEventStatus = iota
	Failure
)

func (s CrawlerEventStatus) String() string {
	switch s {
	case Success:
		return "success"
	case Failure:
		return "failure"
	default:
		return "unknown"
	}
}

type CrawlerEvent struct {
	Timestamp time.Time          `json:"timestamp"`
	URL       string             `json:"url"`
	Domain    string             `json:"domain"`
	Duration  time.Duration      `json:"duration"`
	Status    CrawlerEventStatus `json:"status"`
	Error     *string            `json:"error,omitempty"`
	Host      string             `json:"host"`
	Region    string             `json:"region"`
}

func NewCrawlerEventFromMessage(timestamp time.Time, message string, headers map[string]string) (*CrawlerEvent, error) {
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

	durationFloat, ok := jsonData["duration"].(float64)
	if !ok {
		return nil, fmt.Errorf("error getting duration from json data: %s", message)
	}
	duration := time.Duration(durationFloat) * time.Millisecond

	statusString, ok := jsonData["status"].(string)
	if !ok {
		return nil, fmt.Errorf("error getting status from json data: %s", message)
	}
	var status CrawlerEventStatus
	switch statusString {
	case "success":
		status = Success
	case "failure":
		status = Failure
	default:
		return nil, fmt.Errorf("error getting status from json data: %s", message)
	}

	var errorMsg *string
	if status == Failure {
		errStr, ok := jsonData["error"].(string)
		if !ok {
			return nil, fmt.Errorf("error getting error message from json data: %s", message)
		}
		errorMsg = &errStr
	}

	host, ok := headers["host"]
	if !ok {
		return nil, fmt.Errorf("error getting host from headers: %v", headers)
	}

	region, ok := headers["region"]
	if !ok {
		return nil, fmt.Errorf("error getting region from headers: %v", headers)
	}

	return &CrawlerEvent{
		Timestamp: timestamp,
		URL:       url,
		Domain:    domain,
		Duration:  duration,
		Status:    status,
		Error:     errorMsg,
		Host:      host,
		Region:    region,
	}, nil
}

func (e *CrawlerEvent) String() string {
	printRecord := map[string]interface{}{
		"timestamp": e.Timestamp.Format(time.RFC3339),
		"url":       e.URL,
		"domain":    e.Domain,
		"duration":  e.Duration.String(),
		"status":    e.Status.String(),
		"host":      e.Host,
		"region":    e.Region,
	}
	if e.Error != nil {
		printRecord["error"] = *e.Error
	}
	b, err := json.MarshalIndent(printRecord, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
