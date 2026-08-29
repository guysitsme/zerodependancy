// Chronos — main entry point.
// Starts the persistence store, rollup engine, TCP server, and WebSocket gateway.
package main

import (
	"flag"
	"fmt"
	"os"

	"chronos/persistence"
	"chronos/rollup"
	"chronos/server"
)

func main() {
	dataDir := flag.String("data", "data", "directory for series data files")
	flag.Parse()

	// ── Open the persistence store (Chunk 2)
	store, err := persistence.Open(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open store: %v\n", err)
		os.Exit(1)
	}

	// ── Create the rollup engine (Chunk 3)
	engine := rollup.NewEngine(store, *dataDir)

	// ── Start the TCP server (Chunk 4) in a goroutine
	tcpSrv := server.NewTCPServer(engine)
	go func() {
		if err := tcpSrv.ListenAndServe(); err != nil {
			fmt.Fprintf(os.Stderr, "TCP server error: %v\n", err)
			os.Exit(1)
		}
	}()

	// ── Start the WebSocket gateway (for the browser UI) — blocks main
	wsSrv := server.NewWSServer(tcpSrv)
	if err := wsSrv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "WebSocket server error: %v\n", err)
		os.Exit(1)
	}
}
