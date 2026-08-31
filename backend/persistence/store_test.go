package persistence

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"chronos/compression"
)

// TestWALCrashRecovery tests that points written to unrotated memory buffers
// are recovered 100% when a store is opened after a crash/restart.
func TestWALCrashRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "chronos_wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	series := "cpu_load"

	// 1. Open store, write 5 points (without triggering rotation)
	store1, err := Open(tempDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	testPoints := []struct {
		ts  uint64
		val float64
	}{
		{1000, 45.2},
		{1001, 46.8},
		{1002, 50.1},
		{1003, 49.9},
		{1004, 48.0},
	}

	for _, p := range testPoints {
		if err := store1.WritePoint(series, p.ts, p.val); err != nil {
			t.Fatalf("WritePoint failed: %v", err)
		}
	}

	// Verify WAL file exists on disk
	walFile := filepath.Join(tempDir, series, "wal.log")
	if info, err := os.Stat(walFile); err != nil || info.Size() != int64(len(testPoints)*16) {
		t.Fatalf("expected WAL file size %d bytes, got info: %v, size: %d",
			len(testPoints)*16, err, info.Size())
	}

	// 2. Simulate abrupt shutdown/crash: drop store1 reference without flushing
	store1 = nil

	// 3. Open store2 on same directory (simulates server restart)
	store2, err := Open(tempDir)
	if err != nil {
		t.Fatalf("Open store2 failed: %v", err)
	}

	// 4. Verify in-memory buffer was recovered from WAL
	recovered := store2.GetBufferedPoints(series)
	if len(recovered) != len(testPoints) {
		t.Fatalf("expected %d recovered points, got %d", len(testPoints), len(recovered))
	}

	for i, expected := range testPoints {
		actual := recovered[i]
		if actual.TS != expected.ts || math.Abs(actual.Value-expected.val) > 1e-6 {
			t.Errorf("point %d mismatch: expected (%d, %f), got (%d, %f)",
				i, expected.ts, expected.val, actual.TS, actual.Value)
		}
	}

	// 5. Test rotation flushes chunk and truncates WAL
	if err := store2.FlushSeries(series); err != nil {
		t.Fatalf("FlushSeries failed: %v", err)
	}

	if info, err := os.Stat(walFile); err != nil || info.Size() != 0 {
		t.Errorf("expected truncated WAL file size 0, got err: %v, size: %d", err, info.Size())
	}
}

// TestConcurrentWritesAndQueries verifies concurrent writes to the same series,
// concurrent writes to different series, and concurrent queries under the race detector.
func TestConcurrentWritesAndQueries(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "chronos_concurrency_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	s, err := Open(tempDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const (
		numWriters    = 8
		pointsPerW    = 50
		sameSeries    = "concurrent_series"
		diffSeriesPfx = "series_"
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	// 1. Concurrent writers to the SAME series
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startSignal
			for i := 0; i < pointsPerW; i++ {
				ts := uint64(1_700_000_000 + workerID*1000 + i)
				val := float64(workerID)*100.0 + float64(i)
				if err := s.WritePoint(sameSeries, ts, val); err != nil {
					t.Errorf("WritePoint failed: %v", err)
				}
			}
		}(w)
	}

	// 2. Concurrent writers to DIFFERENT series
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startSignal
			series := fmt.Sprintf("%s%d", diffSeriesPfx, workerID)
			for i := 0; i < pointsPerW; i++ {
				ts := uint64(1_700_000_000 + i*10)
				val := float64(i) * 1.5
				if err := s.WritePoint(series, ts, val); err != nil {
					t.Errorf("WritePoint diff series failed: %v", err)
				}
			}
		}(w)
	}

	// 3. Concurrent readers executing FindChunks and GetBufferedPoints
	stopReaders := make(chan struct{})
	var rWg sync.WaitGroup
	for r := 0; r < 4; r++ {
		rWg.Add(1)
		go func() {
			defer rWg.Done()
			<-startSignal
			for {
				select {
				case <-stopReaders:
					return
				default:
					_ = s.FindChunks(sameSeries, 1_700_000_000, 1_800_000_000)
					_ = s.GetBufferedPoints(sameSeries)
					_ = s.GetAllSeriesMeta()
				}
			}
		}()
	}

	// Launch all goroutines simultaneously
	close(startSignal)

	// Wait for all writers to complete
	wg.Wait()

	// Stop readers
	close(stopReaders)
	rWg.Wait()

	// Flush the series to commit in-memory buffers to chunks
	if err := s.FlushSeries(sameSeries); err != nil {
		t.Fatalf("FlushSeries failed: %v", err)
	}

	// Read all chunks for the series and verify point count
	chunks := s.FindChunks(sameSeries, 0, ^uint64(0))
	var recoveredPoints []compression.Point
	for _, ch := range chunks {
		pts, err := s.ReadChunk(sameSeries, ch)
		if err != nil {
			t.Fatalf("ReadChunk %s failed: %v", ch, err)
		}
		recoveredPoints = append(recoveredPoints, pts...)
	}
	// Add any remaining buffered points
	recoveredPoints = append(recoveredPoints, s.GetBufferedPoints(sameSeries)...)

	expectedCount := numWriters * pointsPerW
	if len(recoveredPoints) != expectedCount {
		t.Fatalf("expected %d total points for %s, got %d", expectedCount, sameSeries, len(recoveredPoints))
	}
}
