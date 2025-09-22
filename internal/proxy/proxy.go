package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/edevhub/amqp-router/internal/amqp091"
	"github.com/edevhub/amqp-router/internal/backend"
	"github.com/edevhub/amqp-router/internal/transport"
)

type Server struct {
	addr   string
	logger *slog.Logger

	listener  net.Listener
	conns     map[net.Conn]struct{}
	backends  *backend.Pool
	transport chan *transport.Package

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

func NewServer(ctx context.Context, addr string, backends *backend.Pool, logger *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(ctx)
	return &Server{
		addr:     addr,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		conns:    make(map[net.Conn]struct{}),
		backends: backends,
	}
}

// Start begins listening for connections and proxying them to the upstream server
func (s *Server) Start() error {
	var err error

	s.transport = make(chan *transport.Package)
	back, err := s.backends.GetDefault()
	if err != nil {
		return fmt.Errorf("failed to connect to the default backend: %w", err)
	}
	go s.connectBackend(back)

	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start listener on %s: %w", s.addr, err)
	}
	s.logger.Info(fmt.Sprintf("Listening on %s", s.addr))

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
		l := s.logger.With(
			slog.String("remote", conn.RemoteAddr().String()),
			slog.String("local", conn.LocalAddr().String()),
		)
		l.Debug("Closing connection")
		if err := conn.Close(); err != nil {
			l.Error("Error closing connection")
		}
	}
	s.mu.Unlock()

	// Wait for all goroutines to finish
	s.wg.Wait()
	close(s.transport)
	s.backends.Close()
	return nil
}

// acceptLoop accepts incoming connections and handles them
func (s *Server) acceptLoop() {
	// closes external group added in the Start() method
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Error("Failed to accept new connection", slog.Any("error", err))
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
					s.logger.Error("Error handling connection", slog.Any("error", err))
				}
				s.logger.Error("Error handling AMQP connection", slog.Any("error", err))
			}
		}(conn)
	}
}

// handleConnection processes a client connection
func (s *Server) handleConnection(conn net.Conn) error {
	amqpConn := amqp091.NewConnection(conn, s.logger.WithGroup("amqp"))
	pkgs, err := amqpConn.Handle(s.ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := amqpConn.Close(); cerr != nil {
			s.logger.Warn("Error closing AMQP connection", slog.Any("error", cerr))
		}
	}()
	for pkg := range pkgs {
		if pkg == nil {
			return nil
		}
		if pkg.Err != nil {
			// TODO: Handle mapping errors and send back to client instead of dropping the connection
			return pkg.Err
		}
		s.logger.Debug("Received AMQP package", slog.Any("package", pkg))
		select {
		case <-s.ctx.Done():
			return nil
		default:
			s.logger.Debug("Sending package to the default backend", slog.Any("package", pkg))
			s.transport <- pkg
		}
	}
	return nil
}

func (s *Server) connectBackend(back *backend.AMQP091) {
	if err := back.Connect(s.ctx, s.transport); err != nil {
		s.logger.Error("Connection to the default backend failed", slog.Any("error", err))
	}
}
