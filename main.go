package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/edevhub/amqp-router/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", "0.0.0.0:5672", "Address to listen on")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	var llevel slog.Level
	if *verbose {
		llevel = slog.LevelDebug
	} else {
		llevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: llevel,
	}))
	p := proxy.NewServer(*listenAddr, logger)

	// Start the proxy in a goroutine
	go func() {
		if err := p.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Handle a graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("Received signal, shutting down", slog.Any("signal", sig))

	// Stop the proxy
	if err := p.Stop(); err != nil {
		fatal(logger, fmt.Errorf("error stopping router: %w", err))
	}

	logger.Info("Router stopped")
}

func fatal(l *slog.Logger, err error) {
	l.Error("Fatal error", slog.Any("error", err))
	os.Exit(1)
}
