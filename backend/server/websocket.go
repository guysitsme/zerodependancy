package server

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

var _ net.Conn = (*wsConn)(nil) // compile-time interface check

// ─── wsConn wraps a hijacked HTTP connection as a net.Conn ───────────────────
// This lets us reuse handleConn() unchanged for WebSocket clients.

type wsConn struct {
	conn  net.Conn
	bufrw *bufio.ReadWriter
}

func (c *wsConn) Read(b []byte) (int, error)              { return c.bufrw.Read(b) }
func (c *wsConn) Write(b []byte) (int, error)              { return c.bufrw.Write(b) }
func (c *wsConn) Close() error                             { return c.conn.Close() }
func (c *wsConn) LocalAddr() net.Addr                      { return c.conn.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr                     { return c.conn.RemoteAddr() }
func (c *wsConn) SetDeadline(t time.Time) error             { return c.conn.SetDeadline(t) }
func (c *wsConn) SetReadDeadline(t time.Time) error         { return c.conn.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error        { return c.conn.SetWriteDeadline(t) }

// ─── RFC 6455 handshake ──────────────────────────────────────────────────────

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// upgradeToWebSocket performs the RFC 6455 opening handshake using only stdlib.
// Returns the hijacked net.Conn and its buffered r/w pair on success.
func upgradeToWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, *bufio.ReadWriter, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, nil, fmt.Errorf("not a websocket request")
	}
	key := r.Header.Get("Sec-Websocket-Key")
	if key == "" {
		return nil, nil, fmt.Errorf("missing Sec-Websocket-Key")
	}

	// Compute accept key (RFC 6455 §4.2.2).
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("ResponseWriter does not support hijacking")
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, nil, err
	}

	// Send 101 Switching Protocols.
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n" +
		"Access-Control-Allow-Origin: *\r\n" +
		"\r\n"
	if _, err := bufrw.WriteString(resp); err != nil {
		conn.Close()
		return nil, nil, err
	}
	if err := bufrw.Flush(); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, bufrw, nil
}
