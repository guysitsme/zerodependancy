// Package persistence — wal.go
// Write-Ahead Log (WAL) for 100% crash durability with zero external dependencies.
//
// Every incoming point is written to an append-only binary log (data/<series>/wal.log)
// before being stored in the in-memory write buffer.
// Binary format per record:
//   [0:8]   Timestamp (uint64, big-endian)
//   [8:16]  Value     (float64 IEEE 754 bits, big-endian)
// Total record size: 16 bytes.
//
// When a buffer rotates into a compressed chunk file, the WAL is truncated.
// If the server crashes or restarts, RecoverWAL replays unrotated points back
// into memory so zero data points are lost.
package persistence

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"

	"chronos/compression"
)

const walRecordSize = 16
const walFileName = "wal.log"

// walPath returns the full filesystem path for a series' WAL file.
func walPath(dataDir, series string) string {
	return filepath.Join(dataDir, series, walFileName)
}

// AppendWAL appends a single 16-byte point record to the series WAL file.
func AppendWAL(dataDir, series string, ts uint64, value float64) error {
	dir := filepath.Join(dataDir, series)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	path := walPath(dataDir, series)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf [walRecordSize]byte
	binary.BigEndian.PutUint64(buf[0:8], ts)
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(value))

	_, err = f.Write(buf[:])
	if err != nil {
		return err
	}
	return f.Sync()
}

// TruncateWAL clears the WAL file for a series after its points have been safely
// flushed into a compressed chunk file.
func TruncateWAL(dataDir, series string) error {
	path := walPath(dataDir, series)
	// Truncate or remove if exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	return os.Truncate(path, 0)
}

// RecoverWAL reads and returns all valid unrotated points from the series WAL file.
// If the file does not exist or is empty, it returns nil, nil.
func RecoverWAL(dataDir, series string) ([]compression.Point, error) {
	path := walPath(dataDir, series)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var points []compression.Point
	var buf [walRecordSize]byte

	for {
		_, err := io.ReadFull(f, buf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return points, err
		}

		ts := binary.BigEndian.Uint64(buf[0:8])
		val := math.Float64frombits(binary.BigEndian.Uint64(buf[8:16]))
		points = append(points, compression.Point{TS: ts, Value: val})
	}

	return points, nil
}
