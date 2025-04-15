FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod ./

# Copy the source code
COPY . .

# Build the application
RUN go build -o amqp-router .

# Use a minimal alpine image for the final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/amqp-router .

# Expose the default AMQP port
EXPOSE 5672

# Set the entrypoint
ENTRYPOINT ["/app/amqp-router"]
