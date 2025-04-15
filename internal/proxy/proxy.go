package proxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/edevhub/amqp-router/internal/amqp091"
	amqpconn "github.com/edevhub/amqp-router/internal/conn"
	"log"
	"net"
	"sync"
)

type Server struct {
	addr string

	listener net.Listener
	conns    map[net.Conn]struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
}

func NewServer(addr string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[net.Conn]struct{}),
	}
}

// Start begins listening for connections and proxying them to the upstream server
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start listener on %s: %w", s.addr, err)
	}

	log.Printf("Listening on %s", s.addr)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() error {
	s.cancel()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("error closing listener: %w", err)
		}
	}

	s.mu.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()

	// Wait for all goroutines to finish
	s.wg.Wait()
	return nil
}

// acceptLoop accepts incoming connections and handles them
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
			}()

			if err = s.handleConnection(c); err != nil {
				if !errors.Is(err, net.ErrClosed) {
					log.Printf("Error handling connection: %v", err)
				}
			}
		}(conn)
	}
}

// handleConnection processes a client connection
func (s *Server) handleConnection(conn net.Conn) error {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Handle the connection with AMQP-specific logic
	errCh := make(chan error, 1)
	go func() {
		err := handleAMQPConnection(amqpconn.NewConnection(conn))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

func handleAMQPConnection(conn *amqpconn.Connection) error {
	defer conn.Close()

	pkgs, err := conn.Init()
	if err != nil {
		return fmt.Errorf("failed to init AMQP connection: %w", err)
	}

	p := <-pkgs
	if p.Err != nil {
		return fmt.Errorf("failed to read AMQP RPC frame: %w", err)
	}
	log.Printf("Received AMQP RPC frame: %+v", p.Frame.(*amqp091.MethodFrame).Method)
	log.Printf("AMQP connection stable. Aborting.")
	return nil
}
