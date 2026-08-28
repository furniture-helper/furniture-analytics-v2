package kafkaservice

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaService struct {
	brokers []string
	topic   string
	groupID string
	reader  *kafka.Reader
}

type KafkaMessage struct {
	Message   string
	Timestamp time.Time
	Headers   map[string]string
}

func NewKafkaService(brokers []string, topic string, groupID string) *KafkaService {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: time.Second,
		MinBytes:       1,
		MaxBytes:       100 << 20,              // Increased to 100MB to handle large payload chunks
		MaxWait:        500 * time.Millisecond, // Increased from 100ms to allow better batching
		StartOffset:    kafka.FirstOffset,
		//Logger:         kafka.LoggerFunc(func(format string, args ...interface{}) { log.Printf("[kafka] "+format, args...) }),
		ErrorLogger: kafka.LoggerFunc(func(format string, args ...interface{}) { log.Printf("[kafka-error] "+format, args...) }),
	})

	return &KafkaService{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
		reader:  reader,
	}
}

func (k *KafkaService) Close() error {
	if k.reader == nil {
		return nil
	}
	return k.reader.Close()
}

func (k *KafkaService) GetMessages(ctx context.Context, size int) ([]*KafkaMessage, error) {
	if size <= 0 {
		return nil, nil
	}

	messages := make([]*KafkaMessage, 0, size)
	for len(messages) < size {
		readCtx := ctx
		cancel := func() {}
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var c context.CancelFunc
			readCtx, c = context.WithTimeout(ctx, 10*time.Second)
			cancel = c
		}

		message, err := k.reader.ReadMessage(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			return messages, err
		}

		headers := make(map[string]string)
		for _, header := range message.Headers {
			headers[header.Key] = string(header.Value)
		}

		messages = append(messages, &KafkaMessage{
			Message:   string(message.Value),
			Timestamp: message.Time,
			Headers:   headers,
		})
	}

	return messages, nil
}
