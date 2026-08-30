// Package compression implements Gorilla-style bit-packed time-series
// compression (delta-of-delta for timestamps, XOR for float64 values).
// This is Chunk 1 of the Chronos system — zero external dependencies.
package compression

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// ── Errors ────────────────────────────────────────────────────────────────────

var ErrTruncatedInput = errors.New("compression: truncated input")
var ErrInvalidHeader = errors.New("compression: invalid header")

// ── Point ─────────────────────────────────────────────────────────────────────

type Point struct {
	TS    uint64
	Value float64
}

// ══════════════════════════════════════════════════════════════════════════════
// BitWriter  (bits packed MSB-first into each byte)
// ══════════════════════════════════════════════════════════════════════════════

type bitWriter struct {
	buf         []byte
	currentByte uint8
	bitCount    uint8 // 0–7 bits written into currentByte
}

func newBitWriter() *bitWriter { return &bitWriter{} }

func (w *bitWriter) writeBit(b uint8) {
	w.currentByte = (w.currentByte << 1) | (b & 1)
	w.bitCount++
	if w.bitCount == 8 {
		w.buf = append(w.buf, w.currentByte)
		w.currentByte = 0
		w.bitCount = 0
	}
}

// writeBits writes the lowest n bits of value, MSB first.
// n must be 1..64.
func (w *bitWriter) writeBits(value uint64, n uint) {
	for i := int(n) - 1; i >= 0; i-- {
		w.writeBit(uint8((value >> uint(i)) & 1))
	}
}

func (w *bitWriter) flush() []byte {
	if w.bitCount > 0 {
		w.currentByte <<= (8 - w.bitCount)
		w.buf = append(w.buf, w.currentByte)
	}
	return w.buf
}

// ══════════════════════════════════════════════════════════════════════════════
// BitReader
// ══════════════════════════════════════════════════════════════════════════════

type bitReader struct {
	buf     []byte
	bytePos int
	bitPos  uint8 // 0–7
}

func newBitReader(b []byte) *bitReader { return &bitReader{buf: b} }

func (r *bitReader) readBit() (uint8, error) {
	if r.bytePos >= len(r.buf) {
		return 0, ErrTruncatedInput
	}
	bit := (r.buf[r.bytePos] >> (7 - r.bitPos)) & 1
	r.bitPos++
	if r.bitPos == 8 {
		r.bitPos = 0
		r.bytePos++
	}
	return bit, nil
}

func (r *bitReader) readBits(n uint) (uint64, error) {
	var result uint64
	for i := uint(0); i < n; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		result = (result << 1) | uint64(bit)
	}
	return result, nil
}

// ══════════════════════════════════════════════════════════════════════════════
// Encode
//
// Wire format:
//   [4 bytes uint32 count]
//   [64 bits: t0]
//   [64 bits: v0 raw float64]
//   if count >= 2:
//     [32 bits: delta1 signed]   (t1 - t0)
//     [value encoding for v1]
//   for each subsequent point i >= 2:
//     [dod encoding for ts[i]]
//     [value encoding for vs[i]]
//
// Value encoding (per point):
//   0           → unchanged (xor == 0)
//   1 0 [mbc]   → reuse previous (lz,mbc) window; store xor >> tz, mbc bits
//   1 1 [5:lz] [6:mbc] [mbc bits] → new window
//
// Timestamp dod encoding:
//   0           → dod == 0
//   10  [7]     → dod in [-64, 63]    (7-bit two's complement)
//   110 [9]     → dod in [-256, 255]  (9-bit)
//   1110[12]    → dod in [-2048,2047] (12-bit)
//   1111[32]    → full 32-bit
// ══════════════════════════════════════════════════════════════════════════════

func Encode(points []Point) []byte {
	w := newBitWriter()

	// 4-byte count header written directly as bytes (not through bit writer).
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(points)))
	w.buf = append(w.buf, hdr[:]...)

	if len(points) == 0 {
		return w.flush()
	}

	// First point: raw 64-bit ts and float.
	w.writeBits(points[0].TS, 64)
	w.writeBits(math.Float64bits(points[0].Value), 64)

	if len(points) == 1 {
		return w.flush()
	}

	// Second point: full signed 32-bit delta + value (no window yet).
	delta1 := int64(points[1].TS) - int64(points[0].TS)
	w.writeBits(uint64(int32(delta1)), 32)

	prevBits := math.Float64bits(points[0].Value)
	var prevLZ, prevMBC uint
	var hasWindow bool
	prevBits, prevLZ, prevMBC, hasWindow = encodeXOR(w, math.Float64bits(points[1].Value), prevBits, prevLZ, prevMBC, hasWindow)

	if len(points) == 2 {
		return w.flush()
	}

	prevDelta := delta1
	for i := 2; i < len(points); i++ {
		delta := int64(points[i].TS) - int64(points[i-1].TS)
		dod := delta - prevDelta
		encodeDoD(w, dod)
		prevDelta = delta

		curBits := math.Float64bits(points[i].Value)
		prevBits, prevLZ, prevMBC, hasWindow = encodeXOR(w, curBits, prevBits, prevLZ, prevMBC, hasWindow)
	}

	return w.flush()
}

// encodeDoD writes a delta-of-delta using the Gorilla variable-length prefix scheme.
func encodeDoD(w *bitWriter, dod int64) {
	if dod == 0 {
		w.writeBit(0)
		return
	}
	abs := dod
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs <= 63: // fits in 7-bit signed
		w.writeBit(1); w.writeBit(0)
		w.writeBits(uint64(dod)&0x7F, 7)
	case abs <= 255: // fits in 9-bit signed
		w.writeBit(1); w.writeBit(1); w.writeBit(0)
		w.writeBits(uint64(dod)&0x1FF, 9)
	case abs <= 2047: // fits in 12-bit signed
		w.writeBit(1); w.writeBit(1); w.writeBit(1); w.writeBit(0)
		w.writeBits(uint64(dod)&0xFFF, 12)
	default: // 32-bit fallback
		w.writeBit(1); w.writeBit(1); w.writeBit(1); w.writeBit(1)
		w.writeBits(uint64(dod)&0xFFFFFFFF, 32)
	}
}

// encodeXOR encodes one float64 value using XOR against the previous.
// Returns the updated state: (newPrevBits, newLZ, newMBC, hasWindow).
//
// Wire format for a changed value (after the '1' bit):
//   0 [prevMBC bits]              → reuse previous window
//   1 [5:lz] [6:(mbc-1)] [mbc bits] → new window (mbc-1 stored so 64→63 fits in 6 bits)
func encodeXOR(w *bitWriter, curBits, prevBits uint64, prevLZ, prevMBC uint, hasWindow bool) (uint64, uint, uint, bool) {
	xor := curBits ^ prevBits
	if xor == 0 {
		w.writeBit(0)
		return prevBits, prevLZ, prevMBC, hasWindow
	}
	w.writeBit(1)

	lz := uint(bits.LeadingZeros64(xor))
	tz := uint(bits.TrailingZeros64(xor))
	mbc := 64 - lz - tz

	// Window reuse: current non-zero region fits inside the previous window.
	if hasWindow {
		prevTZ := 64 - prevLZ - prevMBC
		if lz >= prevLZ && tz >= prevTZ {
			w.writeBit(0)
			w.writeBits(xor>>prevTZ, prevMBC)
			return curBits, prevLZ, prevMBC, true
		}
	}

	// New window. Store (mbc-1) in 6 bits so mbc=64 encodes as 63.
	w.writeBit(1)
	w.writeBits(uint64(lz), 5)
	w.writeBits(uint64(mbc-1), 6) // mbc-1: range [0,63] represents mbc [1,64]
	w.writeBits(xor>>tz, mbc)
	return curBits, lz, mbc, true
}

// ══════════════════════════════════════════════════════════════════════════════
// Decode
// ══════════════════════════════════════════════════════════════════════════════

func Decode(data []byte) ([]Point, error) {
	if len(data) < 4 {
		return nil, ErrInvalidHeader
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if count == 0 {
		return nil, nil
	}

	r := newBitReader(data[4:])
	pts := make([]Point, 0, count)

	// First point.
	t0, err := r.readBits(64)
	if err != nil {
		return nil, ErrTruncatedInput
	}
	v0, err := r.readBits(64)
	if err != nil {
		return nil, ErrTruncatedInput
	}
	pts = append(pts, Point{TS: t0, Value: math.Float64frombits(v0)})
	if count == 1 {
		return pts, nil
	}

	// Second point: 32-bit signed delta.
	rawD1, err := r.readBits(32)
	if err != nil {
		return nil, ErrTruncatedInput
	}
	delta1 := int64(int32(rawD1))
	t1 := uint64(int64(t0) + delta1)

	var prevLZ, prevMBC uint
	var hasWindow bool
	v1, lz, mbc, hw, err := decodeXOR(r, v0, prevLZ, prevMBC, hasWindow)
	if err != nil {
		return nil, err
	}
	prevLZ, prevMBC, hasWindow = lz, mbc, hw
	pts = append(pts, Point{TS: t1, Value: math.Float64frombits(v1)})
	if count == 2 {
		return pts, nil
	}

	prevDelta := delta1
	prevTS := t1
	prevVBits := v1

	for i := 2; i < count; i++ {
		dod, err := decodeDoD(r)
		if err != nil {
			return nil, err
		}
		delta := prevDelta + dod
		ts := uint64(int64(prevTS) + delta)
		prevDelta = delta
		prevTS = ts

		vBits, lz, mbc, hw, err := decodeXOR(r, prevVBits, prevLZ, prevMBC, hasWindow)
		if err != nil {
			return nil, err
		}
		prevVBits = vBits
		prevLZ, prevMBC, hasWindow = lz, mbc, hw

		pts = append(pts, Point{TS: ts, Value: math.Float64frombits(vBits)})
	}
	return pts, nil
}

func decodeDoD(r *bitReader) (int64, error) {
	b0, err := r.readBit()
	if err != nil {
		return 0, ErrTruncatedInput
	}
	if b0 == 0 {
		return 0, nil
	}
	b1, err := r.readBit()
	if err != nil {
		return 0, ErrTruncatedInput
	}
	if b1 == 0 { // 7-bit
		v, err := r.readBits(7)
		if err != nil {
			return 0, ErrTruncatedInput
		}
		return signExtend64(v, 7), nil
	}
	b2, err := r.readBit()
	if err != nil {
		return 0, ErrTruncatedInput
	}
	if b2 == 0 { // 9-bit
		v, err := r.readBits(9)
		if err != nil {
			return 0, ErrTruncatedInput
		}
		return signExtend64(v, 9), nil
	}
	b3, err := r.readBit()
	if err != nil {
		return 0, ErrTruncatedInput
	}
	if b3 == 0 { // 12-bit
		v, err := r.readBits(12)
		if err != nil {
			return 0, ErrTruncatedInput
		}
		return signExtend64(v, 12), nil
	}
	// 32-bit
	v, err := r.readBits(32)
	if err != nil {
		return 0, ErrTruncatedInput
	}
	return signExtend64(v, 32), nil
}

func signExtend64(v uint64, n uint) int64 {
	shift := 64 - n
	return int64(v<<shift) >> shift
}

// decodeXOR decodes one XOR-encoded value.
func decodeXOR(r *bitReader, prevBits uint64, prevLZ, prevMBC uint, hasWindow bool) (vBits uint64, lz, mbc uint, hw bool, err error) {
	changed, err := r.readBit()
	if err != nil {
		return 0, prevLZ, prevMBC, hasWindow, ErrTruncatedInput
	}
	if changed == 0 {
		return prevBits, prevLZ, prevMBC, hasWindow, nil
	}

	reuse, err := r.readBit()
	if err != nil {
		return 0, 0, 0, false, ErrTruncatedInput
	}

	if reuse == 0 && hasWindow {
		// Reuse previous (lz, mbc) window.
		prevTZ := 64 - prevLZ - prevMBC
		meaningful, err := r.readBits(prevMBC)
		if err != nil {
			return 0, 0, 0, false, ErrTruncatedInput
		}
		xor := meaningful << prevTZ
		return prevBits ^ xor, prevLZ, prevMBC, true, nil
	}

	// New window.
	lzBits, err := r.readBits(5)
	if err != nil {
		return 0, 0, 0, false, ErrTruncatedInput
	}
	mbcBits, err := r.readBits(6)
	if err != nil {
		return 0, 0, 0, false, ErrTruncatedInput
	}
	lz = uint(lzBits)
	mbc = uint(mbcBits) + 1 // stored as mbc-1; add 1 back
	tz := 64 - lz - mbc

	meaningful, err := r.readBits(mbc)
	if err != nil {
		return 0, 0, 0, false, ErrTruncatedInput
	}
	xor := meaningful << tz
	return prevBits ^ xor, lz, mbc, true, nil
}

// ══════════════════════════════════════════════════════════════════════════════
// Benchmark (spec §4.7)
// ══════════════════════════════════════════════════════════════════════════════

type BenchmarkResult struct {
	RawBytes        int
	CompressedBytes int
	Ratio           float64
}

func Benchmark() BenchmarkResult {
	const n = 86_400
	pts := make([]Point, n)
	// Smooth sinusoidal sensor data — small oscillation around a baseline.
	// This matches real-world telemetry patterns that Gorilla compresses well.
	// Values change by ~0.01-0.1 per sample, sharing exponent bits → high XOR reuse.
	for i := range pts {
		// sin oscillation: period ~1h, amplitude 2.0, baseline 70.0
		phase := float64(i) * 2.0 * 3.141592653589793 / 3600.0
		// Use a simple Taylor approximation to avoid importing math.Sin
		// sin(x) ≈ x - x³/6 + x⁵/120 for small x; for larger x use modular trick:
		// We only need the pattern, not exact trig.
		p := phase - float64(int64(phase/(2*3.141592653589793)))*2*3.141592653589793
		s := p * (1 - p*p/6.0*(1-p*p/20.0)) // sin approximation for p in [-π,π]
		pts[i] = Point{
			TS:    uint64(1_735_000_000 + i),
			Value: 70.0 + 2.0*s,
		}
	}
	raw := n * 16
	comp := Encode(pts)
	ratio := float64(raw) / float64(len(comp))
	return BenchmarkResult{RawBytes: raw, CompressedBytes: len(comp), Ratio: ratio}
}
