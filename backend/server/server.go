// Package server implements Chunk 4: the TCP line-protocol server and the
// WebSocket gateway that lets the browser UI connect.
// Protocol spec: §7.1–7.5 of the implementation report.
package server

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"chronos/config"
	"chronos/rollup"
)

// ── Subscriber map (TAIL & ALERT) ─────────────────────────────────────────────

type subscriber struct {
	conn net.Conn
	w    *bufio.Writer
}

type broker struct {
	mu   sync.Mutex
	subs map[string][]*subscriber
	all  []*subscriber
}

func newBroker() *broker {
	return &broker{
		subs: make(map[string][]*subscriber),
		all:  make([]*subscriber, 0),
	}
}

func (b *broker) register(series string, s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[series] = append(b.subs[series], s)
}

func (b *broker) unregister(series string, s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	list := b.subs[series]
	for i, sub := range list {
		if sub == s {
			b.subs[series] = append(list[:i], list[i+1:]...)
			return
		}
	}
}

func (b *broker) registerClient(s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.all = append(b.all, s)
}

func (b *broker) unregisterClient(s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, sub := range b.all {
		if sub == s {
			b.all = append(b.all[:i], b.all[i+1:]...)
			return
		}
	}
}

// Notify pushes a "ts,value\n" line to every TAIL subscriber for series.
func (b *broker) Notify(series string, ts uint64, value float64) {
	line := fmt.Sprintf("%d,%.6g\n", ts, value)
	b.mu.Lock()
	defer b.mu.Unlock()
	alive := b.subs[series][:0]
	for _, sub := range b.subs[series] {
		if _, err := sub.w.WriteString(line); err != nil {
			sub.conn.Close()
			continue
		}
		if err := sub.w.Flush(); err != nil {
			sub.conn.Close()
			continue
		}
		alive = append(alive, sub)
	}
	b.subs[series] = alive
}

// BroadcastAlert broadcasts an ALERT event line to all connected clients.
func (b *broker) BroadcastAlert(trig AlertTrigger) {
	line := fmt.Sprintf("ALERT %s,%s,%.6g,%s,%.6g,%d\n",
		trig.RuleID, trig.Series, trig.Value, trig.Operator, trig.Threshold, trig.Timestamp)
	b.mu.Lock()
	defer b.mu.Unlock()
	alive := b.all[:0]
	for _, sub := range b.all {
		if _, err := sub.w.WriteString(line); err != nil {
			sub.conn.Close()
			continue
		}
		if err := sub.w.Flush(); err != nil {
			sub.conn.Close()
			continue
		}
		alive = append(alive, sub)
	}
	b.all = alive
}

// ── TCP Server ────────────────────────────────────────────────────────────────

var seriesRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// TCPServer listens for raw TCP connections and speaks the Chronos line protocol.
type TCPServer struct {
	engine *rollup.Engine
	broker *broker
	alerts *AlertManager
}

// NewTCPServer constructs a TCPServer backed by the given Engine and persistence dataDir.
func NewTCPServer(engine *rollup.Engine, dataDir string) *TCPServer {
	b := newBroker()
	am := NewAlertManager(dataDir, func(trig AlertTrigger) {
		b.BroadcastAlert(trig)
	})
	return &TCPServer{engine: engine, broker: b, alerts: am}
}

// WritePoint writes a point to the engine, notifies TAIL subscribers, and evaluates alerts.
func (srv *TCPServer) WritePoint(series string, ts uint64, value float64) error {
	if err := srv.engine.WritePoint(series, ts, value); err != nil {
		return err
	}
	srv.broker.Notify(series, ts, value)
	if srv.alerts != nil {
		srv.alerts.Check(series, ts, value)
	}
	return nil
}

// ListenAndServe blocks, accepting connections on config.TCPPort.
func (srv *TCPServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", config.TCPPort)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Chronos TCP server listening on %s\n", config.TCPPort)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
			continue
		}
		go srv.handleConn(conn)
	}
}

func (srv *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	w := bufio.NewWriter(conn)

	sub := &subscriber{conn: conn, w: w}
	srv.broker.registerClient(sub)
	defer srv.broker.unregisterClient(sub)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "WRITE":
			srv.handleWrite(parts, w)

		case "QUERY":
			srv.handleQuery(parts, w)

		case "TAIL":
			// TAIL keeps this connection open; return when client disconnects.
			srv.handleTail(parts, conn, w, sc)
			return

		case "BENCHMARK":
			srv.handleBenchmark(w)

		default:
			fmt.Fprintf(w, "ERR unknown command: %s\n", parts[0])
			w.Flush()
		}
	}
}

// ── Command handlers ──────────────────────────────────────────────────────────

// WRITE <series> <timestamp> <value>
func (srv *TCPServer) handleWrite(parts []string, w *bufio.Writer) {
	defer w.Flush()
	if len(parts) != 4 {
		fmt.Fprintln(w, "ERR usage: WRITE <series> <timestamp> <value>")
		return
	}
	series := parts[1]
	if !seriesRe.MatchString(series) {
		fmt.Fprintln(w, "ERR invalid series name: use [a-zA-Z0-9_]")
		return
	}
	ts, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		fmt.Fprintln(w, "ERR invalid timestamp: not an integer")
		return
	}
	value, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		fmt.Fprintln(w, "ERR invalid value: not a number")
		return
	}
	if err := srv.WritePoint(series, ts, value); err != nil {
		fmt.Fprintf(w, "ERR %v\n", err)
		return
	}
	fmt.Fprintln(w, "OK")
}

// QUERY <series> <start> <end>
func (srv *TCPServer) handleQuery(parts []string, w *bufio.Writer) {
	defer w.Flush()
	if len(parts) != 4 {
		fmt.Fprintln(w, "ERR usage: QUERY <series> <start_ts> <end_ts>")
		return
	}
	series := parts[1]
	if !seriesRe.MatchString(series) {
		fmt.Fprintln(w, "ERR invalid series name")
		return
	}
	start, err1 := strconv.ParseUint(parts[2], 10, 64)
	end, err2 := strconv.ParseUint(parts[3], 10, 64)
	if err1 != nil || err2 != nil {
		fmt.Fprintln(w, "ERR invalid timestamp")
		return
	}
	if start >= end {
		fmt.Fprintln(w, "ERR start must be less than end")
		return
	}

	result, err := srv.engine.Query(series, start, end)
	if err != nil {
		fmt.Fprintf(w, "ERR %v\n", err)
		return
	}

	switch result.Tier {
	case rollup.TierRaw:
		for _, p := range result.Raw {
			fmt.Fprintf(w, "%d,%.6g\n", p.TS, p.Value)
		}
	default: // Hourly or Daily rollup
		for _, r := range result.Rollups {
			fmt.Fprintf(w, "%d,%.6g,%.6g,%.6g,%d\n",
				r.WindowStart, r.Avg, r.Min, r.Max, r.Count)
		}
	}
	fmt.Fprintln(w, "END")
}

// TAIL <series>
func (srv *TCPServer) handleTail(parts []string, conn net.Conn, w *bufio.Writer, sc *bufio.Scanner) {
	if len(parts) != 2 {
		fmt.Fprintln(w, "ERR usage: TAIL <series>")
		w.Flush()
		return
	}
	series := parts[1]
	if !seriesRe.MatchString(series) {
		fmt.Fprintln(w, "ERR invalid series name")
		w.Flush()
		return
	}

	sub := &subscriber{conn: conn, w: w}
	srv.broker.register(series, sub)
	defer srv.broker.unregister(series, sub)

	// Block until client disconnects (scanner will return false when conn closes).
	for sc.Scan() {
		// Ignore any further commands sent while tailing.
	}
}

// BENCHMARK — returns compression stats for the UI's benchmark panel.
func (srv *TCPServer) handleBenchmark(w *bufio.Writer) {
	defer w.Flush()
	result := compressionBenchmark()
	fmt.Fprintf(w, "%d,%d,%.2f\n", result.RawBytes, result.CompressedBytes, result.Ratio)
}

// ── WebSocket Gateway ─────────────────────────────────────────────────────────
// The browser cannot open raw TCP sockets, so we expose a WebSocket endpoint
// that speaks the same line protocol over a text-frame WebSocket connection.
// This uses only net/http from the stdlib — no third-party WS library.

// WSServer wraps TCPServer to serve the same protocol over WebSocket.
type WSServer struct {
	tcp     *TCPServer
	dataDir string
}

// NewWSServer creates a WebSocket gateway backed by the same engine as srv.
func NewWSServer(tcp *TCPServer, dataDir string) *WSServer {
	return &WSServer{tcp: tcp, dataDir: dataDir}
}

// ListenAndServe blocks on config.WSPort serving the WebSocket upgrade handler,
// REST API endpoints, and UI static files.
func (ws *WSServer) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.handleUpgrade)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		fmt.Fprintln(w, "OK")
	})

	// ── REST API routes (/api/v1/*)
	registerRESTRoutes(mux, ws.tcp, ws.dataDir)

	// ── Serve UI static files if the ui directory is found
	uiCandidates := []string{"ui", "../ui", "../../ui"}
	for _, dir := range uiCandidates {
		if stat, err := os.Stat(dir); err == nil && stat.IsDir() {
			if _, err := os.Stat(dir + "/index.html"); err == nil {
				fs := http.FileServer(http.Dir(dir))
				mux.Handle("/", fs)
				fmt.Fprintf(os.Stdout, "Chronos Dashboard UI served from %s on http://localhost%s\n", dir, config.WSPort)
				break
			}
		}
	}

	fmt.Fprintf(os.Stdout, "Chronos WebSocket gateway + REST API listening on %s\n", config.WSPort)
	return http.ListenAndServe(config.WSPort, mux)
}

// handleUpgrade performs the RFC 6455 WebSocket handshake and then pipes
// each WebSocket text frame through the same handleConn logic.
func (ws *WSServer) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, bufrw, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, "WebSocket upgrade failed", http.StatusBadRequest)
		return
	}
	// Wrap the hijacked connection so handleConn can work on it transparently.
	wsc := newWSConn(conn, bufrw)
	ws.tcp.handleConn(wsc)
}
