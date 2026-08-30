// Package config centralises every tuning constant so nothing is scattered
// as magic numbers throughout the codebase.
package config

const (
	// ── Chunk 2: Persistence ─────────────────────────────────────
	MaxPointsPerChunk      = 10_000
	MaxChunkDurationSecs   = 7_200 // 2 hours
	DataDir                = "data"
	ChunkMagic             = "CHRN"
	ChunkFormatVersion     = byte(1)
	HeaderSize             = 29 // 4 magic + 1 version + 4 count + 8 start + 8 end + 4 payloadLen

	// ── Chunk 3: Query Planner ────────────────────────────────────
	// Ranges shorter than HourlyThresholdSecs use raw points — precision matters more.
	HourlyThresholdSecs = 3_600 * 3   // 3 hours
	// Ranges longer than DailyThresholdSecs use daily rollups — speed matters more.
	DailyThresholdSecs  = 86_400 * 2  // 2 days

	// ── Chunk 4: Server ───────────────────────────────────────────
	TCPPort       = ":9000"
	WSPort        = ":9001"
)
