// Package rollup implements Chunk 3: hourly/daily rollup accumulators and
// the tier-selection query planner. It depends on the persistence package
// (Chunk 2) for raw point retrieval.
package rollup

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"chronos/compression"
	"chronos/config"
	"chronos/persistence"
)

// ── RollupRecord (spec §6.1) ──────────────────────────────────────────────────

// RollupRecord holds one precomputed aggregate window.
// Binary layout on disk: 8+8+8+8+4 = 36 bytes, fixed-width, big-endian.
type RollupRecord struct {
	WindowStart uint64
	Avg         float64
	Min         float64
	Max         float64
	Count       uint32
}

const rollupRecordSize = 36

func marshalRecord(r RollupRecord) []byte {
	buf := make([]byte, rollupRecordSize)
	binary.BigEndian.PutUint64(buf[0:8], r.WindowStart)
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(r.Avg))
	binary.BigEndian.PutUint64(buf[16:24], math.Float64bits(r.Min))
	binary.BigEndian.PutUint64(buf[24:32], math.Float64bits(r.Max))
	binary.BigEndian.PutUint32(buf[32:36], r.Count)
	return buf
}

func unmarshalRecord(buf []byte) RollupRecord {
	return RollupRecord{
		WindowStart: binary.BigEndian.Uint64(buf[0:8]),
		Avg:         math.Float64frombits(binary.BigEndian.Uint64(buf[8:16])),
		Min:         math.Float64frombits(binary.BigEndian.Uint64(buf[16:24])),
		Max:         math.Float64frombits(binary.BigEndian.Uint64(buf[24:32])),
		Count:       binary.BigEndian.Uint32(buf[32:36]),
	}
}

// ── HourlyAccumulator (spec §6.2) ─────────────────────────────────────────────

type hourlyAccumulator struct {
	windowStart uint64
	sum         float64
	min         float64
	max         float64
	count       uint32
}

func newAccumulator() hourlyAccumulator {
	return hourlyAccumulator{min: math.Inf(1), max: math.Inf(-1)}
}

// ── QueryResult ───────────────────────────────────────────────────────────────

// ResultTier describes which data tier was used to serve a query.
type ResultTier int

const (
	TierRaw    ResultTier = iota
	TierHourly ResultTier = iota
	TierDaily  ResultTier = iota
)

// QueryResult is the union type returned by Engine.Query.
type QueryResult struct {
	Tier    ResultTier
	Raw     []compression.Point
	Rollups []RollupRecord
}

// ── Engine ────────────────────────────────────────────────────────────────────

// Engine wraps the persistence Store and adds rollup management + query planning.
type Engine struct {
	store   *persistence.Store
	dataDir string

	mu           sync.Mutex
	accumulators map[string]*hourlyAccumulator
}

// NewEngine creates an Engine over an already-opened Store.
func NewEngine(store *persistence.Store, dataDir string) *Engine {
	return &Engine{
		store:        store,
		dataDir:      dataDir,
		accumulators: make(map[string]*hourlyAccumulator),
	}
}

// WritePoint writes a point and updates the hourly accumulator. This is the
// method Chunk 4 calls — it is the top of the write path.
func (e *Engine) WritePoint(series string, ts uint64, value float64) error {
	if err := e.store.WritePoint(series, ts, value); err != nil {
		return err
	}
	e.mu.Lock()
	e.updateAccumulator(series, ts, value)
	e.mu.Unlock()
	return nil
}

// updateAccumulator updates the in-memory hourly bucket (spec §6.2).
// Must be called with e.mu held.
func (e *Engine) updateAccumulator(series string, ts uint64, value float64) {
	bucketStart := (ts / 3600) * 3600

	acc, ok := e.accumulators[series]
	if !ok {
		a := newAccumulator()
		a.windowStart = bucketStart
		e.accumulators[series] = &a
		acc = &a
	}

	if acc.windowStart != bucketStart {
		// Hour boundary crossed — finalize the old bucket.
		if acc.count > 0 {
			rec := RollupRecord{
				WindowStart: acc.windowStart,
				Avg:         acc.sum / float64(acc.count),
				Min:         acc.min,
				Max:         acc.max,
				Count:       acc.count,
			}
			_ = e.appendHourlyRecord(series, rec)
			e.maybeFinalizeDailyRecord(series, acc.windowStart)
		}
		fresh := newAccumulator()
		fresh.windowStart = bucketStart
		*acc = fresh
	}

	acc.sum += value
	if value < acc.min {
		acc.min = value
	}
	if value > acc.max {
		acc.max = value
	}
	acc.count++
}

// appendHourlyRecord appends a RollupRecord to the series' hourly rollup file.
func (e *Engine) appendHourlyRecord(series string, rec RollupRecord) error {
	path := filepath.Join(e.dataDir, series, "hourly_rollup.dat")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(marshalRecord(rec))
	return err
}

// maybeFinalizeDailyRecord checks whether the 24th hour of a day has just
// been closed, and if so, writes a daily RollupRecord (spec §6.3).
func (e *Engine) maybeFinalizeDailyRecord(series string, hourWindowStart uint64) {
	dayStart := (hourWindowStart / 86400) * 86400
	if hourWindowStart != dayStart+23*3600 {
		return // not the last hour of the day
	}
	hourly, err := e.readHourlyRecordsForDay(series, dayStart)
	if err != nil || len(hourly) != 24 {
		return
	}
	var totalCount uint32
	var totalWeightedSum float64
	dayMin := math.Inf(1)
	dayMax := math.Inf(-1)
	for _, r := range hourly {
		totalCount += r.Count
		totalWeightedSum += r.Avg * float64(r.Count)
		if r.Min < dayMin {
			dayMin = r.Min
		}
		if r.Max > dayMax {
			dayMax = r.Max
		}
	}
	daily := RollupRecord{
		WindowStart: dayStart,
		Avg:         totalWeightedSum / float64(totalCount),
		Min:         dayMin,
		Max:         dayMax,
		Count:       totalCount,
	}
	path := filepath.Join(e.dataDir, series, "daily_rollup.dat")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	f.Write(marshalRecord(daily)) //nolint:errcheck
}

// ── Query Planner (spec §6.4) ─────────────────────────────────────────────────

// Query selects the appropriate data tier and returns results (spec §6.4).
func (e *Engine) Query(series string, start, end uint64) (QueryResult, error) {
	rangeWidth := int64(end) - int64(start)

	if rangeWidth >= int64(config.DailyThresholdSecs) {
		recs, err := e.readRollupFile(series, "daily_rollup.dat", start, end)
		if err == nil && len(recs) > 0 {
			return QueryResult{Tier: TierDaily, Rollups: recs}, nil
		}
	}

	if rangeWidth >= int64(config.HourlyThresholdSecs) {
		recs, err := e.readRollupFile(series, "hourly_rollup.dat", start, end)
		if err == nil && len(recs) > 0 {
			return QueryResult{Tier: TierHourly, Rollups: recs}, nil
		}
	}

	// Raw data path.
	chunkFiles := e.store.FindChunks(series, start, end)
	var allPoints []compression.Point
	for _, fname := range chunkFiles {
		pts, err := e.store.ReadChunk(series, fname)
		if err != nil {
			// Log and skip rather than abort the whole query.
			fmt.Fprintf(os.Stderr, "warn: skipping chunk %s: %v\n", fname, err)
			continue
		}
		allPoints = append(allPoints, pts...)
	}

	// Merge unrotated in-memory buffer points
	bufPts := e.store.GetBufferedPoints(series)
	allPoints = append(allPoints, bufPts...)

	// Filter to exact range.
	filtered := allPoints[:0]
	for _, p := range allPoints {
		if p.TS >= start && p.TS <= end {
			filtered = append(filtered, p)
		}
	}
	return QueryResult{Tier: TierRaw, Raw: filtered}, nil
}

// GetAllSeriesMeta returns metadata across all series in the engine.
func (e *Engine) GetAllSeriesMeta() []persistence.SeriesMeta {
	return e.store.GetAllSeriesMeta()
}

// ── Rollup file helpers ───────────────────────────────────────────────────────

func (e *Engine) readRollupFile(series, filename string, start, end uint64) ([]RollupRecord, error) {
	path := filepath.Join(e.dataDir, series, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data)%rollupRecordSize != 0 {
		return nil, fmt.Errorf("rollup file %s has invalid size %d", filename, len(data))
	}
	var recs []RollupRecord
	for i := 0; i+rollupRecordSize <= len(data); i += rollupRecordSize {
		r := unmarshalRecord(data[i : i+rollupRecordSize])
		if r.WindowStart+3600 > start && r.WindowStart <= end {
			recs = append(recs, r)
		}
	}
	return recs, nil
}

func (e *Engine) readHourlyRecordsForDay(series string, dayStart uint64) ([]RollupRecord, error) {
	all, err := e.readRollupFile(series, "hourly_rollup.dat", dayStart, dayStart+86399)
	if err != nil {
		return nil, err
	}
	var filtered []RollupRecord
	for _, r := range all {
		if r.WindowStart >= dayStart && r.WindowStart < dayStart+86400 {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}
