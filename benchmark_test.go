package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultQueueName     = "benchmark_queue"
	defaultExchangeName  = ""
	defaultRoutingKey    = "benchmark_queue"
	defaultMessageSize   = 1024 // 1KB
	defaultTestDuration  = 10 * time.Second
	defaultMsgPerSecond  = 1000
	defaultConsumerCount = 1
)

var (
	// Command-line flags for benchmark configuration
	amqpURI        = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	queueName      = flag.String("queue", defaultQueueName, "Queue name")
	exchangeName   = flag.String("exchange", defaultExchangeName, "Exchange name")
	routingKey     = flag.String("routing-key", defaultRoutingKey, "Routing key")
	messageSize    = flag.Int("message-size", defaultMessageSize, "Message size in bytes")
	testDuration   = flag.Duration("duration", defaultTestDuration, "Test duration")
	msgPerSecond   = flag.Int("rate", defaultMsgPerSecond, "Target message rate per second")
	consumerCount  = flag.Int("consumers", defaultConsumerCount, "Number of consumer goroutines")
	publisherCount = flag.Int("publishers", 1, "Number of publisher goroutines")
	durable        = flag.Bool("durable", false, "Durable queue")
	autoDelete     = flag.Bool("auto-delete", true, "Auto-delete queue")
	exclusive      = flag.Bool("exclusive", false, "Exclusive queue")
	mandatory      = flag.Bool("mandatory", false, "Mandatory publishing")
	immediate      = flag.Bool("immediate", false, "Immediate publishing")
	persistent     = flag.Bool("persistent", false, "Persistent messages")
	confirmPublish = flag.Bool("confirm", false, "Use publisher confirms")
	consumeAutoAck = flag.Bool("auto-ack", true, "Auto-ack consumed messages")
	printInterval  = flag.Duration("print-interval", 1*time.Second, "Statistics print interval")
)

// BenchmarkAMQPProxy runs a benchmark test for the AMQP proxy
func BenchmarkAMQPProxy(b *testing.B) {
	// Parse flags
	flag.Parse()

	// Skip in short mode
	if testing.Short() {
		b.Skip("Skipping in short mode")
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), *testDuration)
	defer cancel()

	// Connect to RabbitMQ
	conn, err := amqp.Dial(*amqpURI)
	if err != nil {
		b.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare a queue
	q, err := ch.QueueDeclare(
		*queueName,  // name
		*durable,    // durable
		*autoDelete, // delete when unused
		*exclusive,  // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		b.Fatalf("Failed to declare a queue: %v", err)
	}

	// Enable publisher confirms if requested
	if *confirmPublish {
		if err := ch.Confirm(false); err != nil {
			b.Fatalf("Failed to put channel into confirm mode: %v", err)
		}
	}

	// Create a random message payload of the specified size
	payload := make([]byte, *messageSize)
	rand.Read(payload)

	// Statistics
	var (
		publishCount int64
		consumeCount int64
		errorCount   int64
		startTime    = time.Now()
		statsMutex   sync.Mutex
		latencies    = make([]time.Duration, 0, *msgPerSecond*int(testDuration.Seconds()))
		confirms     = make(chan amqp.Confirmation, *msgPerSecond)
		wg           sync.WaitGroup
	)

	// Set up confirms channel if needed
	if *confirmPublish {
		ch.NotifyPublish(confirms)
	}

	// Start consumers
	for i := 0; i < *consumerCount; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			// Create a consumer channel
			consumerCh, err := conn.Channel()
			if err != nil {
				log.Printf("Consumer %d failed to open channel: %v", consumerID, err)
				atomic.AddInt64(&errorCount, 1)
				return
			}
			defer consumerCh.Close()

			// Start consuming
			msgs, err := consumerCh.Consume(
				q.Name,                                 // queue
				fmt.Sprintf("consumer-%d", consumerID), // consumer
				*consumeAutoAck,                        // auto-ack
				*exclusive,                             // exclusive
				false,                                  // no-local
				false,                                  // no-wait
				nil,                                    // args
			)
			if err != nil {
				log.Printf("Consumer %d failed to register consumer: %v", consumerID, err)
				atomic.AddInt64(&errorCount, 1)
				return
			}

			// Consume messages until context is done
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-msgs:
					if !ok {
						return
					}

					// Calculate latency
					if timestamp, err := time.Parse(time.RFC3339Nano, string(msg.Headers["timestamp"].(string))); err == nil {
						latency := time.Since(timestamp)
						statsMutex.Lock()
						latencies = append(latencies, latency)
						statsMutex.Unlock()
					}

					// Acknowledge message if not auto-ack
					if !*consumeAutoAck {
						if err := msg.Ack(false); err != nil {
							log.Printf("Consumer %d failed to acknowledge message: %v", consumerID, err)
							atomic.AddInt64(&errorCount, 1)
						}
					}

					atomic.AddInt64(&consumeCount, 1)
				}
			}
		}(i)
	}

	// Start publishers
	for i := 0; i < *publisherCount; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()

			// Create a publisher channel
			pubCh, err := conn.Channel()
			if err != nil {
				log.Printf("Publisher %d failed to open channel: %v", publisherID, err)
				atomic.AddInt64(&errorCount, 1)
				return
			}
			defer pubCh.Close()

			// Enable publisher confirms if requested
			if *confirmPublish {
				if err := pubCh.Confirm(false); err != nil {
					log.Printf("Publisher %d failed to put channel into confirm mode: %v", publisherID, err)
					atomic.AddInt64(&errorCount, 1)
					return
				}
			}

			// Calculate the interval between messages to achieve the desired rate
			interval := time.Second / time.Duration(*msgPerSecond / *publisherCount)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			// Publish messages until context is done
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Create message with timestamp header for latency measurement
					timestamp := time.Now()
					headers := amqp.Table{"timestamp": timestamp.Format(time.RFC3339Nano)}

					// Set message properties
					var deliveryMode uint8 = 1 // non-persistent
					if *persistent {
						deliveryMode = 2 // persistent
					}

					// Publish message
					err := pubCh.PublishWithContext(
						ctx,
						*exchangeName, // exchange
						*routingKey,   // routing key
						*mandatory,    // mandatory
						*immediate,    // immediate
						amqp.Publishing{
							Headers:         headers,
							ContentType:     "application/octet-stream",
							ContentEncoding: "",
							DeliveryMode:    deliveryMode,
							Priority:        0,
							CorrelationId:   "",
							ReplyTo:         "",
							Expiration:      "",
							MessageId:       fmt.Sprintf("%d-%d", publisherID, atomic.LoadInt64(&publishCount)),
							Timestamp:       timestamp,
							Type:            "",
							UserId:          "",
							AppId:           "amqp-proxy-benchmark",
							Body:            payload,
						},
					)

					if err != nil {
						log.Printf("Publisher %d failed to publish message: %v", publisherID, err)
						atomic.AddInt64(&errorCount, 1)
					} else {
						atomic.AddInt64(&publishCount, 1)
					}

					// Wait for confirmation if enabled
					if *confirmPublish {
						select {
						case confirm := <-confirms:
							if !confirm.Ack {
								log.Printf("Publisher %d received nack", publisherID)
								atomic.AddInt64(&errorCount, 1)
							}
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}(i)
	}

	// Print statistics periodically
	statsTicker := time.NewTicker(*printInterval)
	defer statsTicker.Stop()

	// Wait for test to complete or context to be done
	for {
		select {
		case <-ctx.Done():
			// Test duration reached, print final stats
			printStats(startTime, &publishCount, &consumeCount, &errorCount, latencies)

			// Wait for all goroutines to finish
			wg.Wait()

			// Test completed successfully
			return

		case <-statsTicker.C:
			// Print periodic stats
			printStats(startTime, &publishCount, &consumeCount, &errorCount, latencies)
		}
	}
}

// printStats calculates and prints benchmark statistics
func printStats(startTime time.Time, publishCount, consumeCount, errorCount *int64, latencies []time.Duration) {
	elapsed := time.Since(startTime)
	pubCount := atomic.LoadInt64(publishCount)
	conCount := atomic.LoadInt64(consumeCount)
	errCount := atomic.LoadInt64(errorCount)

	pubRate := float64(pubCount) / elapsed.Seconds()
	conRate := float64(conCount) / elapsed.Seconds()

	fmt.Printf("Elapsed: %v, Published: %d (%.2f msg/s), Consumed: %d (%.2f msg/s), Errors: %d\n",
		elapsed.Round(time.Millisecond),
		pubCount, pubRate,
		conCount, conRate,
		errCount)

	// Calculate latency statistics if we have data
	if len(latencies) > 0 {
		var sum time.Duration
		var min, max time.Duration = latencies[0], latencies[0]

		for _, lat := range latencies {
			sum += lat
			if lat < min {
				min = lat
			}
			if lat > max {
				max = lat
			}
		}

		avg := sum / time.Duration(len(latencies))
		fmt.Printf("Latency (min/avg/max): %v/%v/%v\n",
			min.Round(time.Microsecond),
			avg.Round(time.Microsecond),
			max.Round(time.Microsecond))
	}
}

// TestMain is used to parse flags for the benchmark
func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
