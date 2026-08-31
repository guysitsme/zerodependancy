package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"chronos/persistence"
	"chronos/rollup"
)

// helper: create a test TCP server backed by a temp store, return the server
// and a function to connect to it. The server listens on a random port.
func newTestTCPServer(t *testing.T) (*TCPServer, string, func() net.Conn, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "chronos_server_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	store, err := persistence.Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Open: %v", err)
	}
	engine := rollup.NewEngine(store, dir)
	srv := NewTCPServer(engine, dir)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()

	connect := func() net.Conn {
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		return conn
	}

	cleanup := func() {
		ln.Close()
		os.RemoveAll(dir)
	}

	return srv, dir, connect, cleanup
}

// send a command and read the response line(s) until we get END or a single response.
func sendCmd(conn net.Conn, cmd string) ([]string, error) {
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(conn, "%s\n", cmd)
	scanner := bufio.NewScanner(conn)
	var lines []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lines = append(lines, line)
		if line == "END" || line == "OK" || strings.HasPrefix(line, "ERR") {
			break
		}
	}
	return lines, scanner.Err()
}

// ── Test 1: Well-formed WRITE command returns OK ────────────────────────────

func TestWriteCommandOK(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	lines, err := sendCmd(conn, "WRITE test_metric 1735000000 42.5")
	if err != nil {
		t.Fatalf("sendCmd: %v", err)
	}
	if len(lines) == 0 || lines[0] != "OK" {
		t.Fatalf("expected OK, got %v", lines)
	}
}

// ── Test 2: Well-formed QUERY returns data then END ─────────────────────────

func TestQueryCommand(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	// Write some points first
	for i := 0; i < 5; i++ {
		sendCmd(conn, fmt.Sprintf("WRITE qtest %d %f", 1735000000+i, float64(i)+10.0))
	}

	// Query them back
	lines, err := sendCmd(conn, "QUERY qtest 1735000000 1735000010")
	if err != nil {
		t.Fatalf("sendCmd: %v", err)
	}
	// Should have data lines + END
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines (data + END), got %d: %v", len(lines), lines)
	}
	if lines[len(lines)-1] != "END" {
		t.Errorf("expected last line to be END, got %q", lines[len(lines)-1])
	}
}

// ── Test 3: Malformed WRITE returns ERR without crashing ────────────────────

func TestMalformedWriteReturnsERR(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	cases := []string{
		"WRITE",                           // missing all fields
		"WRITE test_series",               // missing ts and value
		"WRITE test_series notanumber 42", // invalid timestamp
		"WRITE test_series 1735000000",    // missing value
	}

	for _, cmd := range cases {
		lines, err := sendCmd(conn, cmd)
		if err != nil {
			t.Fatalf("sendCmd(%q): %v", cmd, err)
		}
		if len(lines) == 0 || !strings.HasPrefix(lines[0], "ERR") {
			t.Errorf("cmd %q: expected ERR, got %v", cmd, lines)
		}
	}
}

// ── Test 4: Unknown command returns ERR ─────────────────────────────────────

func TestUnknownCommandReturnsERR(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	lines, err := sendCmd(conn, "FOOBAR arg1 arg2")
	if err != nil {
		t.Fatalf("sendCmd: %v", err)
	}
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "ERR") {
		t.Errorf("expected ERR for unknown command, got %v", lines)
	}
}

// ── Test 5: Invalid series name rejected ────────────────────────────────────

func TestInvalidSeriesNameRejected(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	// Series with spaces/special chars should be rejected
	lines, err := sendCmd(conn, "WRITE bad-series! 1735000000 42")
	if err != nil {
		t.Fatalf("sendCmd: %v", err)
	}
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "ERR") {
		t.Errorf("expected ERR for invalid series name, got %v", lines)
	}
}

// ── Test 6: BENCHMARK command returns valid response ────────────────────────

func TestBenchmarkCommand(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "BENCHMARK\n")

	// BENCHMARK returns a single CSV line (no END terminator), read one line directly
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatal("expected benchmark response, got nothing")
	}
	line := strings.TrimSpace(scanner.Text())

	// Response format: rawBytes,compBytes,ratio
	parts := strings.Split(line, ",")
	if len(parts) != 3 {
		t.Errorf("expected 3-part CSV response, got %q", line)
	}
}

// ── Test 7: QUERY with start >= end returns ERR ─────────────────────────────

func TestQueryInvalidRange(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	lines, err := sendCmd(conn, "QUERY test_series 1735000000 1735000000")
	if err != nil {
		t.Fatalf("sendCmd: %v", err)
	}
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "ERR") {
		t.Errorf("expected ERR for start >= end, got %v", lines)
	}
}

// ── Test 8: Multiple writes then query returns correct count ────────────────

func TestWriteAndQueryRoundtrip(t *testing.T) {
	_, _, connect, cleanup := newTestTCPServer(t)
	defer cleanup()

	conn := connect()
	defer conn.Close()

	writeCount := 10
	baseTS := uint64(2_000_000_000)

	for i := 0; i < writeCount; i++ {
		lines, _ := sendCmd(conn, fmt.Sprintf("WRITE roundtrip_test %d %.2f", baseTS+uint64(i), float64(i)*1.5))
		if len(lines) == 0 || lines[0] != "OK" {
			t.Fatalf("write %d: expected OK, got %v", i, lines)
		}
	}

	lines, err := sendCmd(conn, fmt.Sprintf("QUERY roundtrip_test %d %d", baseTS, baseTS+uint64(writeCount)))
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	// Count data lines (everything except "END")
	dataLines := 0
	for _, l := range lines {
		if l != "END" {
			dataLines++
		}
	}
	if dataLines != writeCount {
		t.Errorf("expected %d data lines, got %d (lines: %v)", writeCount, dataLines, lines)
	}
}
