// Package server — rest.go
// REST HTTP API (v1) endpoints that sit alongside the WebSocket gateway.
// All responses are JSON. Endpoints:
//   GET  /api/v1/series            — list all series names with point count
//   GET  /api/v1/stats             — server-wide storage stats
//   POST /api/v1/write             — write a single point (JSON body)
//   GET  /api/v1/query             — query a series over a time range
//   GET  /api/v1/export            — download results as CSV or JSON
package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"chronos/config"
	"chronos/rollup"
)

var restSeriesRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// registerRESTRoutes adds all /api/v1/* handlers to the given mux.
func registerRESTRoutes(mux *http.ServeMux, srv *TCPServer, dataDir string) {
	mux.HandleFunc("/api/v1/series", withCORS(handleListSeries(srv.engine)))
	mux.HandleFunc("/api/v1/stats", withCORS(handleStats(srv.engine)))
	mux.HandleFunc("/api/v1/write", withCORS(handleRESTWrite(srv)))
	mux.HandleFunc("/api/v1/query", withCORS(handleRESTQuery(srv.engine)))
	mux.HandleFunc("/api/v1/export", withCORS(handleExport(srv.engine)))
	mux.HandleFunc("/api/v1/alerts", withCORS(handleAlerts(srv)))
	mux.HandleFunc("/api/v1/alerts/test", withCORS(handleAlertTest(srv)))
}

// ── CORS middleware ───────────────────────────────────────────────────────────

func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ── GET /api/v1/series ───────────────────────────────────────────────────────

type seriesInfo struct {
	Name        string `json:"name"`
	PointCount  int64  `json:"point_count"`
	DiskBytes   int64  `json:"disk_bytes"`
	LastUpdated int64  `json:"last_updated_unix"`
}

func handleListSeries(engine *rollup.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		metas := engine.GetAllSeriesMeta()
		list := make([]seriesInfo, len(metas))
		for i, m := range metas {
			list[i] = seriesInfo{
				Name:        m.Name,
				PointCount:  m.PointCount,
				DiskBytes:   m.DiskBytes,
				LastUpdated: m.LastUpdated,
			}
		}
		writeJSON(w, map[string]any{"series": list, "count": len(list)})
	}
}

// ── GET /api/v1/stats ─────────────────────────────────────────────────────────

type serverStats struct {
	SeriesCount int    `json:"series_count"`
	TotalBytes  int64  `json:"total_disk_bytes"`
	Uptime      string `json:"uptime"`
	TCPPort     string `json:"tcp_port"`
	WSPort      string `json:"ws_port"`
}

var serverStart = time.Now()

func handleStats(engine *rollup.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		metas := engine.GetAllSeriesMeta()
		var totalBytes int64
		for _, m := range metas {
			totalBytes += m.DiskBytes
		}
		elapsed := time.Since(serverStart)
		uptime := fmt.Sprintf("%dh %dm %ds",
			int(elapsed.Hours()), int(elapsed.Minutes())%60, int(elapsed.Seconds())%60)
		writeJSON(w, serverStats{
			SeriesCount: len(metas),
			TotalBytes:  totalBytes,
			Uptime:      uptime,
			TCPPort:     config.TCPPort,
			WSPort:      config.WSPort,
		})
	}
}

// ── POST /api/v1/write ────────────────────────────────────────────────────────

type writeRequest struct {
	Series    string  `json:"series"`
	Timestamp uint64  `json:"ts"`
	Value     float64 `json:"value"`
}

func handleRESTWrite(srv *TCPServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req writeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if !restSeriesRe.MatchString(req.Series) {
			writeJSONError(w, "invalid series name: use [a-zA-Z0-9_]", http.StatusBadRequest)
			return
		}
		if req.Timestamp == 0 {
			req.Timestamp = uint64(time.Now().Unix())
		}
		if err := srv.WritePoint(req.Series, req.Timestamp, req.Value); err != nil {
			writeJSONError(w, "write error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "series": req.Series, "ts": req.Timestamp, "value": req.Value})
	}
}

// ── GET /api/v1/query?series=X&start=Y&end=Z ─────────────────────────────────

func handleRESTQuery(engine *rollup.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		series := q.Get("series")
		if !restSeriesRe.MatchString(series) {
			writeJSONError(w, "invalid series name", http.StatusBadRequest)
			return
		}
		start, err1 := strconv.ParseUint(q.Get("start"), 10, 64)
		end, err2 := strconv.ParseUint(q.Get("end"), 10, 64)
		if err1 != nil || err2 != nil {
			writeJSONError(w, "start and end must be unix epoch integers", http.StatusBadRequest)
			return
		}
		if start >= end {
			writeJSONError(w, "start must be less than end", http.StatusBadRequest)
			return
		}
		result, err := engine.Query(series, start, end)
		if err != nil {
			writeJSONError(w, "query error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		tier := "raw"
		switch result.Tier {
		case rollup.TierHourly:
			tier = "hourly"
		case rollup.TierDaily:
			tier = "daily"
		}

		type rawPoint struct {
			TS    uint64  `json:"ts"`
			Value float64 `json:"value"`
		}
		type rollupPoint struct {
			WindowStart uint64  `json:"window_start"`
			Avg         float64 `json:"avg"`
			Min         float64 `json:"min"`
			Max         float64 `json:"max"`
			Count       uint32  `json:"count"`
		}

		resp := map[string]any{"series": series, "tier": tier}
		if result.Tier == rollup.TierRaw {
			pts := make([]rawPoint, len(result.Raw))
			for i, p := range result.Raw {
				pts[i] = rawPoint{TS: p.TS, Value: p.Value}
			}
			resp["points"] = pts
			resp["count"] = len(pts)
		} else {
			pts := make([]rollupPoint, len(result.Rollups))
			for i, r := range result.Rollups {
				pts[i] = rollupPoint{WindowStart: r.WindowStart, Avg: r.Avg, Min: r.Min, Max: r.Max, Count: r.Count}
			}
			resp["points"] = pts
			resp["count"] = len(pts)
		}
		writeJSON(w, resp)
	}
}

// ── GET /api/v1/export?series=X&start=Y&end=Z&format=csv|json ────────────────

func handleExport(engine *rollup.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		series := q.Get("series")
		if !restSeriesRe.MatchString(series) {
			writeJSONError(w, "invalid series name", http.StatusBadRequest)
			return
		}
		start, err1 := strconv.ParseUint(q.Get("start"), 10, 64)
		end, err2 := strconv.ParseUint(q.Get("end"), 10, 64)
		if err1 != nil || err2 != nil {
			writeJSONError(w, "start and end must be unix epoch integers", http.StatusBadRequest)
			return
		}
		if start >= end {
			writeJSONError(w, "start must be less than end", http.StatusBadRequest)
			return
		}
		format := q.Get("format")
		if format == "" {
			format = "csv"
		}

		result, err := engine.Query(series, start, end)
		if err != nil {
			writeJSONError(w, "query error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		filename := fmt.Sprintf("chronos_%s_%d_%d.%s", series, start, end, format)

		if format == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
			handleRESTQuery(engine)(w, r)
			return
		}

		// CSV export
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)

		if result.Tier == rollup.TierRaw {
			fmt.Fprintln(w, "timestamp,value")
			for _, p := range result.Raw {
				fmt.Fprintf(w, "%d,%.6g\n", p.TS, p.Value)
			}
		} else {
			fmt.Fprintln(w, "window_start,avg,min,max,count")
			for _, rec := range result.Rollups {
				fmt.Fprintf(w, "%d,%.6g,%.6g,%.6g,%d\n",
					rec.WindowStart, rec.Avg, rec.Min, rec.Max, rec.Count)
			}
		}
	}
}

// ── GET/POST/DELETE /api/v1/alerts ───────────────────────────────────────────

type createRuleRequest struct {
	Series    string  `json:"series"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
}

func handleAlerts(srv *TCPServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if srv.alerts == nil {
			writeJSONError(w, "alert manager not configured", http.StatusInternalServerError)
			return
		}

		switch r.Method {
		case http.MethodGet:
			rules := srv.alerts.ListRules()
			triggers := srv.alerts.ListTriggers()
			writeJSON(w, map[string]any{
				"rules":    rules,
				"triggers": triggers,
			})

		case http.MethodPost:
			var req createRuleRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSONError(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
			rule, err := srv.alerts.AddRule(req.Series, req.Operator, req.Threshold)
			if err != nil {
				writeJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]any{"ok": true, "rule": rule})

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				writeJSONError(w, "missing rule id parameter", http.StatusBadRequest)
				return
			}
			deleted := srv.alerts.DeleteRule(id)
			writeJSON(w, map[string]any{"ok": deleted, "id": id})

		default:
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// ── POST /api/v1/alerts/test ──────────────────────────────────────────────────

func handleAlertTest(srv *TCPServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		series := r.URL.Query().Get("series")
		if series == "" {
			series = "test_metric"
		}
		trig := AlertTrigger{
			ID:        fmt.Sprintf("trig_test_%d", time.Now().Unix()),
			RuleID:    "rule_test",
			Series:    series,
			Value:     99.9,
			Threshold: 90.0,
			Operator:  ">",
			Timestamp: time.Now().Unix(),
		}
		if srv.broker != nil {
			srv.broker.BroadcastAlert(trig)
		}
		writeJSON(w, map[string]any{"ok": true, "test_trigger": trig})
	}
}

