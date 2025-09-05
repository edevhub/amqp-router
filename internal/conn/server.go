package conn

import "github.com/edevhub/amqp-router/internal/amqp091"

type ServerHandler struct {
	clients map[string]*amqp091.Connection
	server  *amqp091.Connection
}
