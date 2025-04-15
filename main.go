package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/edevhub/amqp-router/internal/proxy"
)

func main() {
	// Parse command line flags
	listenAddr := flag.String("listen", "0.0.0.0:5672", "Address to listen on")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	// Configure logging
	if *verbose {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	} else {
		log.SetFlags(log.Ldate | log.Ltime)
	}

	log.Printf("Starting AMQP router on %s", *listenAddr)

	p := proxy.NewServer(*listenAddr)

	// Start the proxy in a goroutine
	go func() {
		if err := p.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("Received signal %v, shutting down...\n", sig)

	// Stop the proxy
	if err := p.Stop(); err != nil {
		log.Fatalf("Error stopping proxy: %v", err)
	}

	log.Println("Router stopped successfully")
}
