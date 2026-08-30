// Package persistence implements the Chronos Persistence Layer (Chunk 2).
// It handles crash-safe chunk file writes, the snapshot-read index, and
// CRC32 checksums. It depends on the compression package (Chunk 1) for
// encode/decode but on nothing external to the stdlib.
package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"chronos/compression"
	"chronos/config"
)

// ── Errors ────────────────────────────────────────────────────────────────────

type ChecksumMismatchError struct{ File string }

func (e ChecksumMismatchError) Error() string {
	return fmt.Sprintf("persistence: checksum mismatch in %s", e.File)
}

type WriteError struct{ Reason string }

func (e WriteError) Error() string { return "persistence: " + e.Reason }

// ── IndexEntry ────────────────────────────────────────────────────────────────

// IndexEntry describes one data chunk file stored on disk.
type IndexEntry struct {
	FileName   string
	StartTime  uint64
	EndTime    uint64
	PointCount uint32
}

// ── Store ─────────────────────────────────────────────────────────────────────

// Store is the top-level Persistence Layer object. Create one with Open().
type Store struct {
	dataDir string

	// per-series write lock (only one writer per series at a time)
	writeMu sync.Mutex

	// per-series in-memory write buffer (unprotected — only the writer touches it)
	buffers map[string][]compression.Point

	// per-series next chunk ID counter
	nextID map[string]uint32

	// snapshot-read index: atomic pointer to map[series][]IndexEntry
	// Readers take an atomic load; writer does a full swap after building a
	// new copy — no reader ever blocks another.
	indexPtr unsafe.Pointer // *map[string][]IndexEntry
}

// Open opens (or creates) the store rooted at dataDir.
// On startup it rebuilds the index by reading every index.dat, falling back
// to scanning chunk headers if index.dat is missing or corrupt.
func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dataDir: dataDir,
		buffers: make(map[string][]compression.Point),
		nextID:  make(map[string]uint32),
	}

	idx, err := s.rebuildIndex()
	if err != nil {
		return nil, err
	}
	atomic.StorePointer(&s.indexPtr, unsafe.Pointer(&idx))
	return s, nil
}

// ── Index snapshot helpers ────────────────────────────────────────────────────

func (s *Store) loadIndex() map[string][]IndexEntry {
	p := atomic.LoadPointer(&s.indexPtr)
	if p == nil {
		m := make(map[string][]IndexEntry)
		return m
	}
	return *(*map[string][]IndexEntry)(p)
}

func (s *Store) swapIndex(m map[string][]IndexEntry) {
	atomic.StorePointer(&s.indexPtr, unsafe.Pointer(&m))
}

// ── Write path ────────────────────────────────────────────────────────────────

// WritePoint appends a (timestamp, value) point to the named series.
// It is safe to call from multiple goroutines for different series;
// concurrent writes to the SAME series are serialised by writeMu.
func (s *Store) WritePoint(series string, ts uint64, value float64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	buf := s.buffers[series]
	buf = append(buf, compression.Point{TS: ts, Value: value})
	s.buffers[series] = buf

	if shouldRotate(buf) {
		return s.rotate(series)
	}
	return nil
}

func shouldRotate(buf []compression.Point) bool {
	if len(buf) >= config.MaxPointsPerChunk {
		return true
	}
	if len(buf) >= 2 {
		span := int64(buf[len(buf)-1].TS) - int64(buf[0].TS)
		if span >= int64(config.MaxChunkDurationSecs) {
			return true
		}
	}
	return false
}

// rotate flushes the active buffer for series to a new chunk file.
func (s *Store) rotate(series string) error {
	buf := s.buffers[series]
	if len(buf) == 0 {
		return nil
	}

	compressed := compression.Encode(buf)

	// Build header (spec §5.2)
	hdr := make([]byte, config.HeaderSize)
	copy(hdr[0:4], config.ChunkMagic)
	hdr[4] = config.ChunkFormatVersion
	binary.BigEndian.PutUint32(hdr[5:9], uint32(len(buf)))
	binary.BigEndian.PutUint64(hdr[9:17], buf[0].TS)
	binary.BigEndian.PutUint64(hdr[17:25], buf[len(buf)-1].TS)
	binary.BigEndian.PutUint32(hdr[25:29], uint32(len(compressed)))

	full := append(hdr, compressed...)
	checksum := crc32.ChecksumIEEE(full)
	var csBuf [4]byte
	binary.BigEndian.PutUint32(csBuf[:], checksum)
	fileBytes := append(full, csBuf[:]...)

	// Crash-safe write: write to .tmp, fsync, rename.
	chunkID := s.nextChunkID(series)
	seriesDir := filepath.Join(s.dataDir, series)
	if err := os.MkdirAll(seriesDir, 0o755); err != nil {
		return err
	}
	finalName := fmt.Sprintf("chunk_%04d.dat", chunkID)
	finalPath := filepath.Join(seriesDir, finalName)
	tmpPath := finalPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if _, err := f.Write(fileBytes); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	// Build new index snapshot and publish it atomically.
	oldIdx := s.loadIndex()
	newIdx := make(map[string][]IndexEntry, len(oldIdx)+1)
	for k, v := range oldIdx {
		newIdx[k] = v
	}
	newEntry := IndexEntry{
		FileName:   finalName,
		StartTime:  buf[0].TS,
		EndTime:    buf[len(buf)-1].TS,
		PointCount: uint32(len(buf)),
	}
	newIdx[series] = append(newIdx[series], newEntry)

	// Persist index.dat atomically.
	if err := s.writeIndexFile(series, seriesDir, newIdx[series]); err != nil {
		return err
	}
	s.swapIndex(newIdx)
	s.buffers[series] = s.buffers[series][:0]
	return nil
}

func (s *Store) nextChunkID(series string) uint32 {
	s.nextID[series]++
	return s.nextID[series]
}

// ── Read path ─────────────────────────────────────────────────────────────────

// FindChunks returns filenames of all chunks whose time range overlaps [start, end].
// It operates on an immutable snapshot — never blocks a writer.
func (s *Store) FindChunks(series string, start, end uint64) []string {
	idx := s.loadIndex()
	entries := idx[series]
	var result []string
	for _, e := range entries {
		if e.EndTime >= start && e.StartTime <= end {
			result = append(result, e.FileName)
		}
	}
	return result
}

// ReadChunk reads, verifies, and decompresses a named chunk file for a series.
func (s *Store) ReadChunk(series, fileName string) ([]compression.Point, error) {
	path := filepath.Join(s.dataDir, series, fileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < config.HeaderSize+4 {
		return nil, fmt.Errorf("persistence: file %s too short", fileName)
	}

	// Verify magic.
	if string(data[0:4]) != config.ChunkMagic {
		return nil, fmt.Errorf("persistence: bad magic in %s", fileName)
	}

	payloadLen := int(binary.BigEndian.Uint32(data[25:29]))
	body := data[:config.HeaderSize+payloadLen]
	storedCS := binary.BigEndian.Uint32(data[config.HeaderSize+payloadLen:])
	computedCS := crc32.ChecksumIEEE(body)
	if storedCS != computedCS {
		return nil, ChecksumMismatchError{File: fileName}
	}

	payload := data[config.HeaderSize : config.HeaderSize+payloadLen]
	return compression.Decode(payload)
}

// ── Index persistence ─────────────────────────────────────────────────────────

// writeIndexFile writes index.dat for a series atomically (temp+rename).
func (s *Store) writeIndexFile(series, seriesDir string, entries []IndexEntry) error {
	var sb strings.Builder
	for _, e := range entries {
		fmt.Fprintf(&sb, "%s,%d,%d,%d\n", e.FileName, e.StartTime, e.EndTime, e.PointCount)
	}
	data := []byte(sb.String())
	tmpPath := filepath.Join(seriesDir, "index.dat.tmp")
	finalPath := filepath.Join(seriesDir, "index.dat")

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmpPath, finalPath)
}

var chunkFileRe = regexp.MustCompile(`^chunk_(\d{4})\.dat$`)

// rebuildIndex builds the in-memory index by reading every series subdirectory.
// For each series it tries index.dat first; on failure it scans chunk headers.
func (s *Store) rebuildIndex() (map[string][]IndexEntry, error) {
	idx := make(map[string][]IndexEntry)

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return idx, nil
		}
		return nil, err
	}

	for _, de := range entries {
		if !de.IsDir() {
			continue
		}
		series := de.Name()
		seriesDir := filepath.Join(s.dataDir, series)

		loaded, err := s.loadIndexFile(seriesDir)
		if err != nil {
			// Fall back to scanning chunk headers.
			loaded, err = s.scanChunkHeaders(series, seriesDir)
			if err != nil {
				return nil, err
			}
		}
		idx[series] = loaded

		// Update nextID counter.
		for _, e := range loaded {
			m := chunkFileRe.FindStringSubmatch(e.FileName)
			if m != nil {
				id, _ := strconv.ParseUint(m[1], 10, 32)
				if uint32(id) > s.nextID[series] {
					s.nextID[series] = uint32(id)
				}
			}
		}
	}
	return idx, nil
}

func (s *Store) loadIndexFile(seriesDir string) ([]IndexEntry, error) {
	f, err := os.Open(filepath.Join(seriesDir, "index.dat"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []IndexEntry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			return nil, fmt.Errorf("bad index line: %q", line)
		}
		start, err1 := strconv.ParseUint(parts[1], 10, 64)
		end, err2 := strconv.ParseUint(parts[2], 10, 64)
		count, err3 := strconv.ParseUint(parts[3], 10, 32)
		if err1 != nil || err2 != nil || err3 != nil {
			return nil, fmt.Errorf("bad index line: %q", line)
		}
		entries = append(entries, IndexEntry{
			FileName:   parts[0],
			StartTime:  start,
			EndTime:    end,
			PointCount: uint32(count),
		})
	}
	return entries, sc.Err()
}

func (s *Store) scanChunkHeaders(series, seriesDir string) ([]IndexEntry, error) {
	dirEntries, err := os.ReadDir(seriesDir)
	if err != nil {
		return nil, err
	}

	var entries []IndexEntry
	for _, de := range dirEntries {
		name := de.Name()
		if !chunkFileRe.MatchString(name) {
			continue
		}
		path := filepath.Join(seriesDir, name)
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		hdr := make([]byte, config.HeaderSize)
		_, err = io.ReadFull(f, hdr)
		f.Close()
		if err != nil || string(hdr[0:4]) != config.ChunkMagic {
			continue
		}
		count := binary.BigEndian.Uint32(hdr[5:9])
		start := binary.BigEndian.Uint64(hdr[9:17])
		end := binary.BigEndian.Uint64(hdr[17:25])
		entries = append(entries, IndexEntry{
			FileName:   name,
			StartTime:  start,
			EndTime:    end,
			PointCount: count,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StartTime < entries[j].StartTime
	})
	// Rebuild index.dat from scanned headers.
	_ = s.writeIndexFile(series, seriesDir, entries)
	return entries, nil
}

// FlushSeries force-rotates the active buffer for a series, writing any
// buffered points to disk. Used at shutdown or by tests.
func (s *Store) FlushSeries(series string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.rotate(series)
}
