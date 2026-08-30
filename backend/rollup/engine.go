package rollup

import (
	"errors"
	"math"
	"sync"
)

// ---------- Config (mirror these from config.go once it exists there) ----------

const (
	HourlyThresholdSeconds = 3 * 3600    // 3 hours
	DailyThresholdSeconds  = 2 * 86400   // 2 days
)

// ---------- Data structures ----------

// RollupRecord is a finalized, immutable aggregate over a fixed window.
type RollupRecord struct {
	WindowStart uint64
	Avg         float64
	Min         float64
	Max         float64
	Count       uint32
}

// HourlyAccumulator is the in-memory, per-series running aggregate
// for the currently-open hour bucket.
type HourlyAccumulator struct {
	WindowStart uint64
	Sum         float64
	Min         float64
	Max         float64
	Count       uint32
}

// Point mirrors the (timestamp, value) pair used across chunks.
type Point struct {
	Timestamp uint64
	Value     float64
}

// QueryResult is what query() returns — either raw points or rollup rows.
type QueryResultKind int

const (
	KindRaw QueryResultKind = iota
	KindRollup
)

type QueryResult struct {
	Kind    QueryResultKind
	Points  []Point        // populated when Kind == KindRaw
	Rollups []RollupRecord // populated when Kind == KindRollup
}

// ---------- Errors ----------

var ErrLateArrival = errors.New("point timestamp falls in an already-closed hour bucket")

// ---------- Persistence interface (Chunk 2 dependency) ----------
// Defined as an interface so Chunk 3 can be built/tested against a mock
// before Chunk 2's real implementation is ready.

type Persistence interface {
	WritePoint(series string, timestamp uint64, value float64) error
	FindChunks(series string, start, end uint64) ([]string, error)
	ReadChunk(chunkID string) ([]Point, error)
}

// RollupStore abstracts reading/writing finalized rollup records to disk.
// Implement this against flat fixed-width binary files per the spec (§6.1),
// or swap in a mock for early testing.
type RollupStore interface {
	AppendHourlyRecord(series string, rec RollupRecord) error
	AppendDailyRecord(series string, rec RollupRecord) error
	ReadHourlyRecordsForDay(series string, dayStart uint64) ([]RollupRecord, error)
	HourlyRollupFullyCovers(series string, start, end uint64) (bool, error)
	DailyRollupFullyCovers(series string, start, end uint64) (bool, error)
	ReadHourlyRecords(series string, start, end uint64) ([]RollupRecord, error)
	ReadDailyRecords(series string, start, end uint64) ([]RollupRecord, error)
}

// ---------- Engine ----------

type Engine struct {
	mu           sync.Mutex
	persistence  Persistence
	rollups      RollupStore
	accumulators map[string]*HourlyAccumulator // per series
}

func NewEngine(p Persistence, r RollupStore) *Engine {
	return &Engine{
		persistence:  p,
		rollups:      r,
		accumulators: make(map[string]*HourlyAccumulator),
	}
}

// ---------- Write path ----------

// WritePoint is called by Chunk 4. It forwards to Chunk 2 and updates
// the hourly accumulator, finalizing bucket(s) as needed.
func (e *Engine) WritePoint(series string, timestamp uint64, value float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	acc, exists := e.accumulators[series]
	bucketStart := (timestamp / 3600) * 3600

	if exists && bucketStart < acc.WindowStart {
		// Late-arriving point for an already-closed hour: reject explicitly.
		// (Documented decision — see STDLIB.md "Rollup late-arrival policy".)
		return ErrLateArrival
	}

	if !exists {
		acc = &HourlyAccumulator{
			WindowStart: bucketStart,
			Sum:         0,
			Min:         math.Inf(1),
			Max:         math.Inf(-1),
			Count:       0,
		}
		e.accumulators[series] = acc
	} else if bucketStart != acc.WindowStart {
		// Crossed into a new hour: finalize the old one first.
		if err := e.finalizeHourly(series, acc); err != nil {
			return err
		}
		acc = &HourlyAccumulator{
			WindowStart: bucketStart,
			Sum:         0,
			Min:         math.Inf(1),
			Max:         math.Inf(-1),
			Count:       0,
		}
		e.accumulators[series] = acc
	}

	acc.Sum += value
	if value < acc.Min {
		acc.Min = value
	}
	if value > acc.Max {
		acc.Max = value
	}
	acc.Count++

	// Forward to Chunk 2 last, after rollup bookkeeping succeeds.
	return e.persistence.WritePoint(series, timestamp, value)
}

func (e *Engine) finalizeHourly(series string, acc *HourlyAccumulator) error {
	if acc.Count == 0 {
		return nil
	}
	rec := RollupRecord{
		WindowStart: acc.WindowStart,
		Avg:         acc.Sum / float64(acc.Count),
		Min:         acc.Min,
		Max:         acc.Max,
		Count:       acc.Count,
	}
	if err := e.rollups.AppendHourlyRecord(series, rec); err != nil {
		return err
	}
	return e.maybeFinalizeDaily(series, acc.WindowStart)
}

// maybeFinalizeDaily derives a daily record from 24 hourly records,
// using a count-weighted average (see theory notes).
func (e *Engine) maybeFinalizeDaily(series string, hourWindowStart uint64) error {
	dayStart := (hourWindowStart / 86400) * 86400
	if hourWindowStart != dayStart+23*3600 {
		return nil // not the 24th hour of the day yet
	}

	hourlyRecords, err := e.rollups.ReadHourlyRecordsForDay(series, dayStart)
	if err != nil {
		return err
	}
	if len(hourlyRecords) != 24 {
		return nil // incomplete day, don't finalize
	}

	var totalCount uint32
	var weightedSum float64
	dayMin := math.Inf(1)
	dayMax := math.Inf(-1)

	for _, r := range hourlyRecords {
		totalCount += r.Count
		weightedSum += r.Avg * float64(r.Count)
		if r.Min < dayMin {
			dayMin = r.Min
		}
		if r.Max > dayMax {
			dayMax = r.Max
		}
	}

	daily := RollupRecord{
		WindowStart: dayStart,
		Avg:         weightedSum / float64(totalCount),
		Min:         dayMin,
		Max:         dayMax,
		Count:       totalCount,
	}
	return e.rollups.AppendDailyRecord(series, daily)
}

// ---------- Read path / query planner ----------

// Query implements the tier-selection cascade: daily -> hourly -> raw,
// falling back whenever a tier doesn't fully cover the requested range.
func (e *Engine) Query(series string, start, end uint64) (QueryResult, error) {
	rangeWidth := end - start

	if rangeWidth >= DailyThresholdSeconds {
		covers, err := e.rollups.DailyRollupFullyCovers(series, start, end)
		if err != nil {
			return QueryResult{}, err
		}
		if covers {
			recs, err := e.rollups.ReadDailyRecords(series, start, end)
			if err != nil {
				return QueryResult{}, err
			}
			return QueryResult{Kind: KindRollup, Rollups: recs}, nil
		}
	}

	if rangeWidth >= HourlyThresholdSeconds {
		covers, err := e.rollups.HourlyRollupFullyCovers(series, start, end)
		if err != nil {
			return QueryResult{}, err
		}
		if covers {
			recs, err := e.rollups.ReadHourlyRecords(series, start, end)
			if err != nil {
				return QueryResult{}, err
			}
			return QueryResult{Kind: KindRollup, Rollups: recs}, nil
		}
	}

	// Fall back to raw.
	chunkIDs, err := e.persistence.FindChunks(series, start, end)
	if err != nil {
		return QueryResult{}, err
	}

	var allPoints []Point
	for _, id := range chunkIDs {
		pts, err := e.persistence.ReadChunk(id)
		if err != nil {
			return QueryResult{}, err
		}
		allPoints = append(allPoints, pts...)
	}

	filtered := make([]Point, 0, len(allPoints))
	for _, p := range allPoints {
		if p.Timestamp >= start && p.Timestamp <= end {
			filtered = append(filtered, p)
		}
	}

	return QueryResult{Kind: KindRaw, Points: filtered}, nil
}
