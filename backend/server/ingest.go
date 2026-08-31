// Package server — ingest.go
// Background goroutines that poll free public APIs (no key required) and
// write real-world time-series data directly into the Chronos store.
//
// Sources:
//   - Open-Meteo  (weather)   — https://open-meteo.com  — unlimited, no key
//   - CoinGecko   (crypto)    — https://coingecko.com   — 50 req/min, no key
//   - Frankfurter (forex)     — https://frankfurter.app — unlimited, no key
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// IngestConfig controls which free-API feeds are enabled.
type IngestConfig struct {
	Weather bool
	Crypto  bool
	Forex   bool
}

// PointWriter abstracts any target that can receive points.
type PointWriter interface {
	WritePoint(series string, ts uint64, value float64) error
}

// StartIngest launches background goroutines that poll public APIs and write
// data into the provided PointWriter (TCPServer or Engine). Call once after the server is ready.
func StartIngest(writer PointWriter, cfg IngestConfig) {
	if cfg.Weather {
		go ingestWeather(writer)
	}
	if cfg.Crypto {
		go ingestCrypto(writer)
	}
	if cfg.Forex {
		go ingestForex(writer)
	}
}

// ── Open-Meteo: temperature in New Delhi (lat=28.6, lon=77.2) ─────────────────

type openMeteoResp struct {
	Current struct {
		Temperature float64 `json:"temperature_2m"`
		WindSpeed   float64 `json:"wind_speed_10m"`
		Humidity    int     `json:"relative_humidity_2m"`
	} `json:"current"`
}

func ingestWeather(writer PointWriter) {
	const url = "https://api.open-meteo.com/v1/forecast?latitude=28.6&longitude=77.2" +
		"&current=temperature_2m,wind_speed_10m,relative_humidity_2m"

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	// Fire immediately, then tick every 60s.
	doWeather(writer, url)
	for range ticker.C {
		doWeather(writer, url)
	}
}

func doWeather(writer PointWriter, url string) {
	body, err := fetchJSON(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest/weather: %v\n", err)
		return
	}
	var r openMeteoResp
	if err := json.Unmarshal(body, &r); err != nil {
		fmt.Fprintf(os.Stderr, "ingest/weather: parse error: %v\n", err)
		return
	}
	ts := uint64(time.Now().Unix())
	write(writer, "weather_temp_c", ts, r.Current.Temperature)
	write(writer, "weather_wind_kmh", ts, r.Current.WindSpeed)
	write(writer, "weather_humidity_pct", ts, float64(r.Current.Humidity))
	fmt.Fprintf(os.Stdout, "ingest/weather: temp=%.1f°C wind=%.1f wind=%.1f%% RH\n",
		r.Current.Temperature, r.Current.WindSpeed, float64(r.Current.Humidity))
}

// ── CoinGecko: BTC & ETH price in USD ─────────────────────────────────────────

type coinGeckoResp map[string]map[string]float64

func ingestCrypto(writer PointWriter) {
	const url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	doCrypto(writer, url)
	for range ticker.C {
		doCrypto(writer, url)
	}
}

func doCrypto(writer PointWriter, url string) {
	body, err := fetchJSON(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest/crypto: %v\n", err)
		return
	}
	var r coinGeckoResp
	if err := json.Unmarshal(body, &r); err != nil {
		fmt.Fprintf(os.Stderr, "ingest/crypto: parse error: %v\n", err)
		return
	}
	ts := uint64(time.Now().Unix())
	if btc, ok := r["bitcoin"]["usd"]; ok {
		write(writer, "btc_usd", ts, btc)
		fmt.Fprintf(os.Stdout, "ingest/crypto: BTC=$%.2f\n", btc)
	}
	if eth, ok := r["ethereum"]["usd"]; ok {
		write(writer, "eth_usd", ts, eth)
		fmt.Fprintf(os.Stdout, "ingest/crypto: ETH=$%.2f\n", eth)
	}
}

// ── Frankfurter: EUR/USD & GBP/USD forex rate ─────────────────────────────────

type frankfurterResp struct {
	Rates map[string]float64 `json:"rates"`
}

func ingestForex(writer PointWriter) {
	const url = "https://api.frankfurter.app/latest?from=EUR&to=USD,GBP,JPY"

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	doForex(writer, url)
	for range ticker.C {
		doForex(writer, url)
	}
}

func doForex(writer PointWriter, url string) {
	body, err := fetchJSON(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest/forex: %v\n", err)
		return
	}
	var r frankfurterResp
	if err := json.Unmarshal(body, &r); err != nil {
		fmt.Fprintf(os.Stderr, "ingest/forex: parse error: %v\n", err)
		return
	}
	ts := uint64(time.Now().Unix())
	for cur, rate := range r.Rates {
		series := "forex_eur_" + cur
		write(writer, series, ts, rate)
		fmt.Fprintf(os.Stdout, "ingest/forex: EUR/%s=%.4f\n", cur, rate)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

var httpClient = &http.Client{Timeout: 10 * time.Second}

func fetchJSON(url string) ([]byte, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func write(writer PointWriter, series string, ts uint64, value float64) {
	if err := writer.WritePoint(series, ts, value); err != nil {
		fmt.Fprintf(os.Stderr, "ingest: write %s: %v\n", series, err)
	}
}

