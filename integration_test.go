package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// amqpURI returns connection string. By default, it connects to localhost without going through the router.
func amqpURI() string {
	if v := os.Getenv("AMQP_URI"); v != "" {
		return v
	}
	return "amqp://guest:guest@localhost:5672/"
}

// tryConnect attempts to connect with short timeout; returns connection or nil with error.
func tryConnect(uri string, timeout time.Duration) (*amqp.Connection, error) {
	connCh := make(chan *amqp.Connection, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := amqp.Dial(uri)
		if err != nil {
			errCh <- err
			return
		}
		connCh <- conn
	}()

	select {
	case c := <-connCh:
		return c, nil
	case err := <-errCh:
		return nil, err
	case <-time.After(timeout):
		return nil, errors.New("connection timeout")
	}
}

func randomName(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

func TestRabbitMQ_PublishConsume_WithHeadersAndLargeBodies(t *testing.T) {
	uri := amqpURI()

	// Short, fail-fast connection attempt; if cannot connect and AMQP_URI not set, skip to avoid CI failures.
	conn, err := tryConnect(uri, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to RabbitMQ at %s: %v", uri, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	exName := randomName("it_exchange")
	qName := randomName("it_queue")
	routingKey := randomName("it_key")

	// 1. Declare exchange, queue and binding
	if err := ch.ExchangeDeclare(exName, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("ExchangeDeclare: %v", err)
	}
	q, err := ch.QueueDeclare(qName, true, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	if err := ch.QueueBind(q.Name, routingKey, exName, false, nil); err != nil {
		t.Fatalf("QueueBind: %v", err)
	}

	// 2. Publish 3 messages with various headers and sizes
	// small
	smallBody := []byte("hello")
	// medium ~64KB
	mediumBody := make([]byte, 64*1024)
	for i := range mediumBody {
		mediumBody[i] = byte(i % 251)
	}
	// large ~300KB (will be split across frames by client lib)
	largeBody := make([]byte, 300*1024)
	for i := range largeBody {
		largeBody[i] = byte((i * 3) % 251)
	}

	// Common headers with variety of types
	now := time.Now().UTC().Truncate(time.Second) // second precision for equality
	commonHeaders := amqp.Table{
		"hdr_string": "value",
		"hdr_int":    int32(42),
		"hdr_bool":   true,
		"hdr_float":  float32(3.14),
		"hdr_time":   now,
		"hdr_bytes":  []byte{0, 1, 2, 3, 254, 255},
		"hdr_nested": amqp.Table{"n1": "x", "n2": int16(7)},
		"hdr_null":   nil,
	}

	type msg struct {
		body          []byte
		headers       amqp.Table
		contentType   string
		deliveryMode  uint8
		correlationID string
		messageID     string
	}

	msgs := []msg{
		{smallBody, cloneTable(commonHeaders, amqp.Table{"size": "small"}), "text/plain", amqp.Transient, "corr-1", "msg-1"},
		{mediumBody, cloneTable(commonHeaders, amqp.Table{"size": "medium"}), "application/octet-stream", amqp.Persistent, "corr-2", "msg-2"},
		{largeBody, cloneTable(commonHeaders, amqp.Table{"size": "large"}), "application/octet-stream", amqp.Persistent, "corr-3", "msg-3"},
	}

	for i, m := range msgs {
		pub := amqp.Publishing{
			Headers:       m.headers,
			ContentType:   m.contentType,
			DeliveryMode:  m.deliveryMode,
			CorrelationId: m.correlationID,
			MessageId:     m.messageID,
			Timestamp:     now,
			Body:          m.body,
		}
		if err := ch.PublishWithContext(context.Background(), exName, routingKey, false, false, pub); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// 3. Consume and validate
	consumeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	deliveries, err := ch.ConsumeWithContext(consumeCtx, q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	received := make([]amqp.Delivery, 0, 3)
	for len(received) < 3 {
		select {
		case d, ok := <-deliveries:
			if !ok {
				t.Fatalf("deliveries channel closed prematurely; got %d messages", len(received))
			}
			received = append(received, d)
		case <-consumeCtx.Done():
			t.Fatalf("timeout waiting for messages; got %d", len(received))
		}
	}

	// Validate order and content
	for i, d := range received {
		exp := msgs[i]
		if d.ContentType != exp.contentType {
			t.Fatalf("msg %d content-type got %s want %s", i, d.ContentType, exp.contentType)
		}
		if d.DeliveryMode != exp.deliveryMode {
			t.Fatalf("msg %d delivery-mode got %d want %d", i, d.DeliveryMode, exp.deliveryMode)
		}
		if d.CorrelationId != exp.correlationID {
			t.Fatalf("msg %d correlation-id got %s want %s", i, d.CorrelationId, exp.correlationID)
		}
		if d.MessageId != exp.messageID {
			t.Fatalf("msg %d message-id got %s want %s", i, d.MessageId, exp.messageID)
		}
		if !equalBytes(d.Body, exp.body) {
			t.Fatalf("msg %d body mismatch: len got %d want %d", i, len(d.Body), len(exp.body))
		}
		if !equalHeaders(d.Headers, exp.headers) {
			t.Fatalf("msg %d headers mismatch: got %#v want %#v", i, d.Headers, exp.headers)
		}
	}

	// Cleanup
	_ = ch.QueueUnbind(q.Name, routingKey, exName, nil)
	_, _ = ch.QueueDelete(q.Name, false, false, false)
	_ = ch.ExchangeDelete(exName, false, false)
}

func cloneTable(base amqp.Table, extra amqp.Table) amqp.Table {
	out := amqp.Table{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalHeaders(a, b amqp.Table) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if !equalHeaderVal(va, vb) {
			return false
		}
	}
	return true
}

func equalHeaderVal(a, b any) bool {
	switch av := a.(type) {
	case []byte:
		bv, ok := b.([]byte)
		if !ok {
			return false
		}
		return equalBytes(av, bv)
	case amqp.Table:
		bv, ok := b.(amqp.Table)
		if !ok {
			return false
		}
		return equalHeaders(av, bv)
	case time.Time:
		bv, ok := b.(time.Time)
		if !ok {
			return false
		}
		// allow second precision comparison
		return av.Equal(bv)
	default:
		return av == b
	}
}
