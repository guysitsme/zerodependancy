package server

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var _ net.Conn = (*wsConn)(nil) // compile-time interface check

// ─── wsConn wraps a hijacked HTTP connection as a net.Conn with RFC 6455 framing
// This lets handleConn() work identically on TCP sockets and WebSockets.

type wsConn struct {
	conn      net.Conn
	bufrw     *bufio.ReadWriter
	readBuf   []byte
	writeLock sync.Mutex
}

func newWSConn(conn net.Conn, bufrw *bufio.ReadWriter) *wsConn {
	return &wsConn{
		conn:  conn,
		bufrw: bufrw,
	}
}

// Read reads and unmasks one or more RFC 6455 frames into b.
func (c *wsConn) Read(b []byte) (int, error) {
	for len(c.readBuf) == 0 {
		// Read 2-byte frame header
		var hdr [2]byte
		if _, err := io.ReadFull(c.bufrw, hdr[:]); err != nil {
			return 0, err
		}

		opcode := hdr[0] & 0x0F
		masked := (hdr[1] & 0x80) != 0
		payloadLen := uint64(hdr[1] & 0x7F)

		if opcode == 0x8 { // Close frame
			// Echo close frame
			c.writeLock.Lock()
			_ = c.bufrw.WriteByte(0x88)
			_ = c.bufrw.WriteByte(0x00)
			_ = c.bufrw.Flush()
			c.writeLock.Unlock()
			return 0, io.EOF
		}

		if payloadLen == 126 {
			var ext [2]byte
			if _, err := io.ReadFull(c.bufrw, ext[:]); err != nil {
				return 0, err
			}
			payloadLen = uint64(binary.BigEndian.Uint16(ext[:]))
		} else if payloadLen == 127 {
			var ext [8]byte
			if _, err := io.ReadFull(c.bufrw, ext[:]); err != nil {
				return 0, err
			}
			payloadLen = binary.BigEndian.Uint64(ext[:])
		}

		var maskKey [4]byte
		if masked {
			if _, err := io.ReadFull(c.bufrw, maskKey[:]); err != nil {
				return 0, err
			}
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.bufrw, payload); err != nil {
			return 0, err
		}

		if masked {
			for i := uint64(0); i < payloadLen; i++ {
				payload[i] ^= maskKey[i%4]
			}
		}

		if opcode == 0x9 { // Ping frame -> send Pong
			c.writeLock.Lock()
			_ = c.bufrw.WriteByte(0x8A) // Pong
			_ = c.bufrw.WriteByte(byte(len(payload)))
			_, _ = c.bufrw.Write(payload)
			_ = c.bufrw.Flush()
			c.writeLock.Unlock()
			continue
		}

		// Ensure newline so bufio.Scanner processes each frame line by line
		if len(payload) > 0 && payload[len(payload)-1] != '\n' {
			payload = append(payload, '\n')
		}
		c.readBuf = payload
	}

	n := copy(b, c.readBuf)
	c.readBuf = c.readBuf[n:]
	return n, nil
}

// Write frames the given payload as an RFC 6455 text frame (0x81).
func (c *wsConn) Write(b []byte) (int, error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	var hdr []byte
	n := len(b)

	if n <= 125 {
		hdr = []byte{0x81, byte(n)}
	} else if n <= 65535 {
		hdr = make([]byte, 4)
		hdr[0] = 0x81
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
	} else {
		hdr = make([]byte, 10)
		hdr[0] = 0x81
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
	}

	if _, err := c.bufrw.Write(hdr); err != nil {
		return 0, err
	}
	if _, err := c.bufrw.Write(b); err != nil {
		return 0, err
	}
	if err := c.bufrw.Flush(); err != nil {
		return 0, err
	}
	return n, nil
}

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
