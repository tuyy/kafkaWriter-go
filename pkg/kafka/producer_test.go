package kafka

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestProducerWithLib(t *testing.T) {
	w := &kafka.Writer{
		Addr:     kafka.TCP("dev-tuyy0-cassandra001-ncl.nfra.io:9092"),
		Topic:    "mytest1",
		Balancer: &kafka.LeastBytes{},
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close w:", err)
	}
}

func TestProducer(t *testing.T) {
	brokers := []string{"dev-tuyy0-cassandra001-ncl.nfra.io:9092"}

	p := NewProducer("mytest1", brokers...)
	defer p.FlushAndClose()

	for i := 1; i <= 100; i++ {
		v := fmt.Sprintf("%d hello world!", i)
		err := p.WriteMsg(nil, []byte(v), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}
