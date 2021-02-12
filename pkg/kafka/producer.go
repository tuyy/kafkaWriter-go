package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

type Producer struct {
	w          *kafka.Writer
	buf        []kafka.Message
	maxBufSize int
}

const maxBufferSize = 4096

func NewProducer(topic string, brokers ...string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		maxBufSize: maxBufferSize,
	}
}

func (p *Producer) SetMaxBufSize(bufSize int) {
	p.maxBufSize = bufSize
}

func (p *Producer) WriteMsg(key, value []byte, headers map[string][]byte) error {
	kafkaHeaders := ConvertToKafkaHeader(headers)

	p.buf = append(p.buf, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
	})

	if len(p.buf) > p.maxBufSize {
		return p.Flush()
	}
	return nil
}

func ConvertToKafkaHeader(headers map[string][]byte) []kafka.Header {
	newHeaders := make([]kafka.Header, len(headers))

	i := 0
	for k, v := range headers {
		header := kafka.Header{Key: k, Value: v}
		newHeaders[i] = header
		i++
	}
	return newHeaders
}

func (p *Producer) Flush() error {
	err := p.w.WriteMessages(context.Background(), p.buf...)
	if err != nil {
		log.Fatal(err)
	}

	p.buf = p.buf[:0]
	return nil
}

func (p *Producer) FlushAndClose() {
	if err := p.Flush(); err != nil {
		log.Fatal("failed to flush w:", err)
	}

	if err := p.w.Close(); err != nil {
		log.Fatal("failed to close w:", err)
	}
}
