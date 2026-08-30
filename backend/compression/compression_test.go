package compression

import (
	"math"
	"math/rand"
	"testing"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustRoundTrip(t *testing.T, pts []Point) {
	t.Helper()
	streams := Encode(pts)
	got, err := Decode(streams, len(pts))
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	if len(got) != len(pts) {
		t.Fatalf("length mismatch: want %d got %d", len(pts), len(got))
	}
	for i := range pts {
		if got[i].TS != pts[i].TS {
			t.Errorf("[%d] TS want %d got %d", i, pts[i].TS, got[i].TS)
		}
		if got[i].Value != pts[i].Value {
			t.Errorf("[%d] Value want %v got %v", i, pts[i].Value, got[i].Value)
		}
	}
}

// ─── Test 1: empty round-trip ─────────────────────────────────────────────────

func TestEncodeDecodeEmpty(t *testing.T) {
	got, err := Decode(Encode(nil), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

// ─── Test 2: single point ────────────────────────────────────────────────────

func TestEncodeDecodeSingle(t *testing.T) {
	pts := []Point{{TS: 1_735_000_000, Value: 87.3}}
	mustRoundTrip(t, pts)
}

// ─── Test 3: regular series (all dod == 0) ───────────────────────────────────

func TestEncodeDecodeRegular(t *testing.T) {
	pts := make([]Point, 100)
	for i := range pts {
		pts[i] = Point{TS: uint64(1_000 + i*60), Value: 72.5 + float64(i)*0.01}
	}
	mustRoundTrip(t, pts)
}

// ─── Test 4: large time gap (forces 32-bit fallback) ─────────────────────────

func TestEncodeDecodeLargeGap(t *testing.T) {
	pts := []Point{
		{TS: 1000, Value: 1.0},
		{TS: 1060, Value: 1.1},
		{TS: 1_000_000, Value: 1.2}, // dod = ~998879, needs 32-bit
	}
	mustRoundTrip(t, pts)
}

// ─── Test 5: constant values (all XORs are zero) ─────────────────────────────

func TestEncodeDecodeConstantValues(t *testing.T) {
	pts := make([]Point, 50)
	for i := range pts {
		pts[i] = Point{TS: uint64(1000 + i*60), Value: 42.0}
	}
	mustRoundTrip(t, pts)
}

// ─── Test 6: wildly varying values (no window reuse) ─────────────────────────

func TestEncodeDecodeWildValues(t *testing.T) {
	pts := make([]Point, 50)
	bases := []float64{0.0001, 1e15, -3.14, 1.0 / 3.0, math.Pi, math.E, -999.9}
	for i := range pts {
		pts[i] = Point{TS: uint64(1000 + i*60), Value: bases[i%len(bases)]}
	}
	mustRoundTrip(t, pts)
}

// ─── Test 7: two points only ─────────────────────────────────────────────────

func TestEncodeDecodeTwoPoints(t *testing.T) {
	pts := []Point{
		{TS: 1_735_000_000, Value: 87.3},
		{TS: 1_735_000_060, Value: 87.4},
	}
	mustRoundTrip(t, pts)
}

// ─── Test 8: fuzz — 10 000 random sequences ──────────────────────────────────

func TestEncodeDecodeFuzz(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 10_000; iter++ {
		n := rng.Intn(30) // 0..29 points
		pts := make([]Point, n)
		ts := uint64(rng.Int63n(1_800_000_000))
		for i := range pts {
			ts += uint64(rng.Int63n(3600) + 1)
			pts[i] = Point{TS: ts, Value: rng.Float64()*2000 - 1000}
		}
		streams := Encode(pts)
		got, err := Decode(streams, len(pts))
		if err != nil {
			t.Fatalf("iter %d: Decode error: %v", iter, err)
		}
		if len(got) != len(pts) {
			t.Fatalf("iter %d: length mismatch want %d got %d", iter, len(pts), len(got))
		}
		for i := range pts {
			if got[i] != pts[i] {
				t.Fatalf("iter %d [%d]: want %+v got %+v", iter, i, pts[i], got[i])
			}
		}
	}
}

// ─── Test 9: truncated input raises ErrTruncatedInput ───────────────────────

func TestBitReaderTruncated(t *testing.T) {
	// Encode a real payload, then truncate the values stream specifically —
	// each stream is independent now, so truncation has to target one on
	// purpose rather than slicing a single combined blob.
	pts := []Point{{TS: 1000, Value: 1.0}, {TS: 1060, Value: 2.0}, {TS: 1120, Value: 3.0}}
	streams := Encode(pts)
	streams.Values = streams.Values[:len(streams.Values)/2]
	_, err := Decode(streams, len(pts))
	if err == nil {
		t.Fatal("expected error on truncated input, got nil")
	}
}

// ─── Benchmark ───────────────────────────────────────────────────────────────

func TestBenchmarkRuns(t *testing.T) {
	result := Benchmark()
	if result.RawBytes == 0 || result.CompressedBytes == 0 {
		t.Fatal("benchmark returned zero sizes")
	}
	// Any ratio > 1.0 means we are actually compressing the data.
	// Gorilla compression achieves 14x+ on near-constant production data;
	// on oscillating or random data the ratio is typically 1.5–3x.
	if result.Ratio < 1.5 {
		t.Errorf("compression ratio %.2f — output is larger than raw (expected > 1.5x)", result.Ratio)
	}
	t.Logf("Compression benchmark: raw=%d compressed=%d ratio=%.2fx",
		result.RawBytes, result.CompressedBytes, result.Ratio)
}
