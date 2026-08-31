// Chronos — main entry point.
// Starts the persistence store, rollup engine, TCP server, and WebSocket gateway.
// Optional: --ingest flag enables free public-API data ingestion.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"chronos/persistence"
	"chronos/rollup"
	"chronos/server"
)

func main() {
	dataDir := flag.String("data", "data", "directory for series data files")
	ingest := flag.Bool("ingest", true, "enable background ingestion from free public APIs (weather, crypto, forex)")
	demo := flag.Bool("demo", os.Getenv("CHRONOS_DEMO_MODE") == "1" || os.Getenv("CHRONOS_DEMO_MODE") == "true", "enable synthetic demo ingestion mode (no external APIs required)")
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
	tcpSrv := server.NewTCPServer(engine, *dataDir)
	go func() {
		if err := tcpSrv.ListenAndServe(); err != nil {
			fmt.Fprintf(os.Stderr, "TCP server error: %v\n", err)
			os.Exit(1)
		}
	}()

	// ── Start free-API or synthetic demo ingestion if enabled
	if *ingest {
		server.StartIngest(tcpSrv, server.IngestConfig{
			Weather:  true,
			Crypto:   true,
			Forex:    true,
			DemoMode: *demo,
		})
		if *demo {
			fmt.Fprintln(os.Stdout, "Chronos ingest: running in SYNTHETIC DEMO mode (offline, smooth curves)")
		} else {
			fmt.Fprintln(os.Stdout, "Chronos ingest: polling Open-Meteo, CoinGecko, Frankfurter…")
		}
	}

	// ── Graceful shutdown handler: flush all in-memory buffers and open rollup accumulators on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Fprintf(os.Stdout, "\nChronos: received %v — flushing all series buffers and rollup accumulators…\n", sig)
		engine.Flush()
		store.FlushAllSeries()
		fmt.Fprintln(os.Stdout, "Chronos: all data flushed. Goodbye.")
		os.Exit(0)
	}()

	// ── Start the WebSocket gateway + REST API (for the browser UI) — blocks main
	wsSrv := server.NewWSServer(tcpSrv, *dataDir)
	if err := wsSrv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "WebSocket server error: %v\n", err)
		os.Exit(1)
	}
}
