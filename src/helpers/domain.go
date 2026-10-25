package helpers

import (
	"fmt"
	"net/url"
	"strings"
)

func GetDomainFromJSON(jsonData map[string]interface{}, message string) (string, error) {
	if domainValue, ok := jsonData["domain"].(string); ok {
		domain := strings.TrimSpace(domainValue)
		if domain != "" && !strings.EqualFold(domain, "unknown") {
			return domain, nil
		}
	}

	urlValue, ok := jsonData["url"].(string)
	if !ok {
		return "", fmt.Errorf("error getting domain from json data: %s", message)
	}

	parsedURL, err := url.Parse(urlValue)
	if err != nil {
		return "", fmt.Errorf("error getting domain from json data: %s", message)
	}

	domain := strings.TrimSpace(parsedURL.Hostname())
	if domain == "" {
		return "", fmt.Errorf("error getting domain from json data: %s", message)
	}

	return domain, nil
}
