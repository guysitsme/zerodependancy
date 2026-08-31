package persistence

import (
	"math"
	"os"
	"path/filepath"
	"testing"
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
