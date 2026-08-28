package config

import (
	"os"
	"strings"
)

func GetKafkaBrokers() []string {
	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersEnv == "" {
		return []string{"localhost:9092"}
	}
	brokers := strings.Split(brokersEnv, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return brokers
}
