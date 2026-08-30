package server

import "chronos/compression"

// compressionBenchmark is a thin shim so server.go doesn't import the
// compression package directly (keeps the dependency graph clean).
func compressionBenchmark() compression.BenchmarkResult {
	return compression.Benchmark()
}
