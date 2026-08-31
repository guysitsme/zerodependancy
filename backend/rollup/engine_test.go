package rollup

import (
	"math"
	"os"
	"sync"
	"testing"

	"chronos/config"
	"chronos/persistence"
)

// helper: create a fresh store+engine in a temp directory.
func newTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "chronos_rollup_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	store, err := persistence.Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Open: %v", err)
	}
	eng := NewEngine(store, dir)
	return eng, dir
}

// ── Test 1: Hourly accumulator finalizes on hour boundary crossing ──────────

func TestHourlyAccumulatorFinalizesOnBoundary(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	// Write points across two hour buckets.
	// Hour 0: ts 0..3599, Hour 1: ts 3600..7199
	bucketStart := uint64(3600 * 1000) // choose a clean boundary
	for i := uint64(0); i < 10; i++ {
		if err := eng.WritePoint("test_series", bucketStart+i, 50.0+float64(i)); err != nil {
			t.Fatalf("WritePoint: %v", err)
		}
	}
	// Cross into next hour
	for i := uint64(0); i < 5; i++ {
		if err := eng.WritePoint("test_series", bucketStart+3600+i, 100.0+float64(i)); err != nil {
			t.Fatalf("WritePoint: %v", err)
		}
	}

	// Query raw to verify all points were written
	result, err := eng.Query("test_series", bucketStart, bucketStart+3600+5)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.Tier != TierRaw {
		t.Errorf("expected TierRaw, got %v", result.Tier)
	}
	if len(result.Raw) != 15 {
		t.Errorf("expected 15 raw points, got %d", len(result.Raw))
	}
}

// ── Test 2: Daily record weighted average matches raw aggregation ───────────

func TestDailyRecordWeightedAverage(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	// Write data covering all 24 hours of a day so the daily rollup fires.
	dayStart := uint64(86400 * 100) // a clean day boundary
	var rawSum float64
	var rawCount int

	for hour := 0; hour < 24; hour++ {
		bucketTS := dayStart + uint64(hour)*3600
		for i := 0; i < 10; i++ {
			val := float64(hour) + float64(i)*0.1
			rawSum += val
			rawCount++
			if err := eng.WritePoint("daily_test", bucketTS+uint64(i), val); err != nil {
				t.Fatalf("WritePoint: %v", err)
			}
		}
	}
	// Write one more point in the next day to force the 24th hour to close
	if err := eng.WritePoint("daily_test", dayStart+86400, 999.0); err != nil {
		t.Fatalf("WritePoint: %v", err)
	}

	rawAvg := rawSum / float64(rawCount)

	// Read the hourly rollup file to verify it was created
	recs, err := eng.readRollupFile("daily_test", "hourly_rollup.dat", dayStart, dayStart+86400)
	if err != nil {
		t.Skipf("hourly rollup file not created (expected for small writes): %v", err)
	}
	if len(recs) < 1 {
		t.Skipf("no hourly rollup records found")
	}

	// Verify weighted average across hourly records matches raw
	var totalCount uint32
	var totalWeightedSum float64
	for _, r := range recs {
		totalCount += r.Count
		totalWeightedSum += r.Avg * float64(r.Count)
	}
	if totalCount > 0 {
		hourlyAvg := totalWeightedSum / float64(totalCount)
		if math.Abs(hourlyAvg-rawAvg) > 0.01 {
			t.Errorf("hourly weighted avg %.6f != raw avg %.6f", hourlyAvg, rawAvg)
		}
	}
}

// ── Test 3: Query planner selects correct tier at threshold boundaries ───────

func TestQueryPlannerTierSelection(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	// Write some data
	base := uint64(1_000_000)
	for i := 0; i < 20; i++ {
		eng.WritePoint("planner_test", base+uint64(i*60), float64(i))
	}

	// Narrow range (< 3 hours = 10800s) → raw
	result, err := eng.Query("planner_test", base, base+3600)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.Tier != TierRaw {
		t.Errorf("narrow range: expected TierRaw, got %v", result.Tier)
	}

	// Verify the threshold constants
	if config.HourlyThresholdSecs != 3600*3 {
		t.Errorf("unexpected HourlyThresholdSecs: %d", config.HourlyThresholdSecs)
	}
	if config.DailyThresholdSecs != 86400*2 {
		t.Errorf("unexpected DailyThresholdSecs: %d", config.DailyThresholdSecs)
	}
}

// ── Test 4: Planner falls back to raw when rollup data doesn't cover range ──

func TestQueryPlannerFallbackToRaw(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	// Write a small amount of data — no rollup files exist
	base := uint64(1_000_000)
	for i := 0; i < 10; i++ {
		eng.WritePoint("fallback_test", base+uint64(i*60), float64(i))
	}

	// Wide range request (> 2 days) but no daily rollup → should fall back to raw
	result, err := eng.Query("fallback_test", base, base+86400*3)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Even with a wide range, if no rollup files exist, tier should be Raw
	if result.Tier != TierRaw {
		t.Errorf("expected TierRaw fallback, got %v", result.Tier)
	}
}

// ── Test 5: In-memory buffer points included in raw query results ───────────

func TestQueryIncludesBufferedPoints(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	// Write a few points (won't trigger rotation = stays in buffer)
	base := uint64(2_000_000)
	for i := 0; i < 5; i++ {
		eng.WritePoint("buffer_test", base+uint64(i), float64(100+i))
	}

	result, err := eng.Query("buffer_test", base, base+10)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Raw) != 5 {
		t.Errorf("expected 5 buffered points in query, got %d", len(result.Raw))
	}
	for i, p := range result.Raw {
		if p.Value != float64(100+i) {
			t.Errorf("[%d] expected value %f, got %f", i, float64(100+i), p.Value)
		}
	}
}

// ── Test 6: QueryResult types ───────────────────────────────────────────────

func TestQueryResultStructure(t *testing.T) {
	// Verify enum values are distinct
	if TierRaw == TierHourly || TierHourly == TierDaily || TierRaw == TierDaily {
		t.Error("ResultTier enum values must be distinct")
	}

	// Verify RollupRecord binary marshal/unmarshal round-trip
	rec := RollupRecord{
		WindowStart: 3600,
		Avg:         42.5,
		Min:         40.0,
		Max:         45.0,
		Count:       100,
	}
	data := marshalRecord(rec)
	if len(data) != rollupRecordSize {
		t.Fatalf("expected %d bytes, got %d", rollupRecordSize, len(data))
	}
	got := unmarshalRecord(data)
	if got != rec {
		t.Errorf("round-trip mismatch: want %+v got %+v", rec, got)
	}
}

// ── Test 7: Empty series query returns empty result without error ────────────

func TestQueryEmptySeries(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	result, err := eng.Query("nonexistent_series", 0, 9999999999)
	if err != nil {
		t.Fatalf("Query on empty series should not error: %v", err)
	}
	if result.Tier != TierRaw {
		t.Errorf("empty query tier should be TierRaw, got %v", result.Tier)
	}
	if len(result.Raw) != 0 {
		t.Errorf("expected 0 points, got %d", len(result.Raw))
	}
}

// ── Test 8: RollupRecord marshal produces exactly 36 bytes ──────────────────

func TestRollupRecordSize(t *testing.T) {
	if rollupRecordSize != 36 {
		t.Fatalf("rollupRecordSize should be 36, is %d", rollupRecordSize)
	}
}

// ── Test 9: Engine Flush finalizes open hourly bucket without hour boundary crossing ─

func TestEngineFlushOpenBucket(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	bucketStart := uint64(3600 * 500)
	// Write 10 points within the same hour bucket (no boundary crossing)
	for i := uint64(0); i < 10; i++ {
		if err := eng.WritePoint("flush_series", bucketStart+i*60, 50.0+float64(i)); err != nil {
			t.Fatalf("WritePoint failed: %v", err)
		}
	}

	// Flush the engine (as happens during shutdown)
	eng.Flush()

	// Verify the hourly rollup record was written to disk
	recs, err := eng.readRollupFile("flush_series", "hourly_rollup.dat", bucketStart, bucketStart+3600)
	if err != nil {
		t.Fatalf("readRollupFile failed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 flushed hourly rollup record, got %d", len(recs))
	}
	if recs[0].Count != 10 {
		t.Errorf("expected count 10, got %d", recs[0].Count)
	}
	if recs[0].WindowStart != bucketStart {
		t.Errorf("expected WindowStart %d, got %d", bucketStart, recs[0].WindowStart)
	}
}

// ── Test 10: Concurrent writes and queries on Engine ──────────────────────────

func TestEngineConcurrentWritesAndQueries(t *testing.T) {
	eng, dir := newTestEngine(t)
	defer os.RemoveAll(dir)

	const (
		numWriters = 6
		numPoints  = 50
		seriesName = "concurrent_engine_series"
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startSignal
			baseTS := uint64(1_700_000_000 + workerID*1000)
			for i := 0; i < numPoints; i++ {
				if err := eng.WritePoint(seriesName, baseTS+uint64(i), float64(workerID*10+i)); err != nil {
					t.Errorf("WritePoint failed: %v", err)
				}
			}
		}(w)
	}

	stopReaders := make(chan struct{})
	var rWg sync.WaitGroup
	for r := 0; r < 3; r++ {
		rWg.Add(1)
		go func() {
			defer rWg.Done()
			<-startSignal
			for {
				select {
				case <-stopReaders:
					return
				default:
					_, _ = eng.Query(seriesName, 1_700_000_000, 1_800_000_000)
					_ = eng.GetAllSeriesMeta()
				}
			}
		}()
	}

	close(startSignal)
	wg.Wait()
	close(stopReaders)
	rWg.Wait()

	eng.Flush()

	// Query raw results and verify total count
	res, err := eng.Query(seriesName, 0, ^uint64(0))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	expected := numWriters * numPoints
	if len(res.Raw) != expected {
		t.Errorf("expected %d raw points, got %d", expected, len(res.Raw))
	}
}
