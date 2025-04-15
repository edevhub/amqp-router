# AMQP Router

A lightweight AMQP protocol router written in Go, inspired by [CloudAMQP's amqproxy](https://github.com/cloudamqp/amqproxy).

## Overview

This AMQP router sits between AMQP clients and AMQP servers (like RabbitMQ), intelligently routing AMQP protocol frames between them. It can be used to:

- Reduce the number of connections to the AMQP server by connection pooling
- Add an additional layer of security
- Monitor AMQP traffic
- Route messages to different upstream servers based on configurable rules
- Provide high availability by routing to multiple upstream servers

## Features

- AMQP 0-9-1 protocol support
- Intelligent message routing between clients and servers
- Connection pooling to reduce load on the AMQP server
- Configurable maximum number of upstream connections
- Dynamic routing based on message properties
- Graceful shutdown
- Detailed logging

## Installation

### From Source

```bash
git clone https://github.com/edevhub/amqp-router.git
cd amqp-router
go build
```

### Using Go Install

```bash
go install github.com/edevhub/amqp-router@latest
```

## Usage

### Basic Usage

```bash
./amqp-router --listen 0.0.0.0:5672
```

### Command Line Options

- `--listen`: Address to listen on (default: "0.0.0.0:5672")
- `--verbose`: Enable verbose logging (default: false)

### Examples

Listen on all interfaces, port 5672:

```bash
./amqp-router --listen 0.0.0.0:5672
```

Enable verbose logging:

```bash
./amqp-router --listen 0.0.0.0:5672 --verbose
```

Configure routing rules (see documentation for details):

```bash
./amqp-router --listen 0.0.0.0:5672 --config routing-rules.yaml
```

## Architecture

The router consists of several components:

1. **Main Application**: Handles command-line arguments and starts the router
2. **Router Server**: Manages the lifecycle of the router, including starting, stopping, and handling connections
3. **Connection Management**: Handles client connections and manages routing to appropriate servers
4. **AMQP Protocol Handler**: Handles the AMQP protocol specifics, including protocol header exchange, frame parsing, and routing
5. **Routing Engine**: Determines how to route messages based on configurable rules

## Development

### Running Tests

```bash
go test ./...
```

### Building

```bash
go build
```

## Docker Support

### Using Docker

A Dockerfile is provided to build and run the AMQP router in a container:

```bash
# Build the Docker image
docker build -t amqp-router .

# Run the container
docker run -p 5672:5672 amqp-router --listen 0.0.0.0:5672
```

### Using Docker Compose

A docker-compose.yml file is provided to run both the AMQP router and RabbitMQ:

```bash
# Start the services
docker-compose up -d

# Check the status
docker-compose ps

# Stop the services
docker-compose down
```

This will start:
- RabbitMQ on port 5673 (management UI on port 15672)
- AMQP Router on port 5672

You can access the RabbitMQ management UI at http://localhost:15672 (username: guest, password: guest)

## Benchmarking

A benchmark tool is included to test the performance of the AMQP router. It publishes messages to RabbitMQ through the router and consumes them, measuring throughput, latency, and routing efficiency.

```bash
# Run the benchmark with default settings
go test -bench=BenchmarkAMQPRouter -v

# Run with custom settings
go test -bench=BenchmarkAMQPRouter -v -- -rate=5000 -duration=30s -message-size=512 -consumers=4 -publishers=2
```

Available flags:

- `-uri`: AMQP URI (default: "amqp://guest:guest@localhost:5672/")
- `-queue`: Queue name (default: "benchmark_queue")
- `-exchange`: Exchange name (default: "")
- `-routing-key`: Routing key (default: "benchmark_queue")
- `-message-size`: Message size in bytes (default: 1024)
- `-duration`: Test duration (default: 10s)
- `-rate`: Target message rate per second (default: 1000)
- `-consumers`: Number of consumer goroutines (default: 1)
- `-publishers`: Number of publisher goroutines (default: 1)
- `-durable`: Durable queue (default: false)
- `-auto-delete`: Auto-delete queue (default: true)
- `-exclusive`: Exclusive queue (default: false)
- `-mandatory`: Mandatory publishing (default: false)
- `-immediate`: Immediate publishing (default: false)
- `-persistent`: Persistent messages (default: false)
- `-confirm`: Use publisher confirms (default: false)
- `-auto-ack`: Auto-ack consumed messages (default: true)
- `-print-interval`: Statistics print interval (default: 1s)

The benchmark reports:
- Message publishing rate (messages per second)
- Message consumption rate (messages per second)
- Latency statistics (min/avg/max)
- Error count

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- Inspired by [CloudAMQP's amqproxy](https://github.com/cloudamqp/amqproxy)
