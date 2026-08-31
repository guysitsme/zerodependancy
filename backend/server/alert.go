// Package server — alert.go
// Real-Time Threshold Alerting Engine for Chronos.
// Zero third-party dependencies — pure Go standard library.
//
// Evaluates rules on incoming points, logs trigger history, persists rules,
// and broadcasts trigger notifications to WebSocket clients.
package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AlertRule defines a threshold condition on a metric series.
type AlertRule struct {
	ID        string  `json:"id"`
	Series    string  `json:"series"`
	Operator  string  `json:"operator"` // ">", "<", ">=", "<=", "=="
	Threshold float64 `json:"threshold"`
	Enabled   bool    `json:"enabled"`
	CreatedAt int64   `json:"created_at"`
}

// AlertTrigger represents a breach event recorded when a rule condition is met.
type AlertTrigger struct {
	ID        string  `json:"id"`
	RuleID    string  `json:"rule_id"`
	Series    string  `json:"series"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Operator  string  `json:"operator"`
	Timestamp int64   `json:"timestamp"`
}

// AlertManager tracks active rules and evaluates every written data point.
type AlertManager struct {
	dataDir   string
	mu        sync.RWMutex
	rules     map[string]*AlertRule
	triggers  []AlertTrigger
	cooldown  map[string]int64 // ruleID -> lastTriggerTime to avoid alert spam
	onTrigger func(AlertTrigger)
	nextID    uint64
}

// NewAlertManager initializes the alert manager and loads persisted rules.
func NewAlertManager(dataDir string, onTrigger func(AlertTrigger)) *AlertManager {
	am := &AlertManager{
		dataDir:   dataDir,
		rules:     make(map[string]*AlertRule),
		triggers:  make([]AlertTrigger, 0, 100),
		cooldown:  make(map[string]int64),
		onTrigger: onTrigger,
		nextID:    uint64(time.Now().UnixNano()),
	}

	am.loadRules()

	// Seed sensible default rules if none exist
	if len(am.rules) == 0 {
		am.AddRule("btc_usd", ">", 80000)
		am.AddRule("weather_temp_c", ">", 35)
		am.AddRule("cpu_usage", ">", 90)
	}

	return am
}

// AddRule creates a new alert rule and persists it.
func (am *AlertManager) AddRule(series, op string, threshold float64) (*AlertRule, error) {
	if series == "" {
		return nil, fmt.Errorf("series name cannot be empty")
	}
	switch op {
	case ">", "<", ">=", "<=", "==":
	default:
		return nil, fmt.Errorf("invalid operator %q (use >, <, >=, <=, ==)", op)
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	am.nextID++
	id := fmt.Sprintf("rule_%d", am.nextID%100000)
	rule := &AlertRule{
		ID:        id,
		Series:    series,
		Operator:  op,
		Threshold: threshold,
		Enabled:   true,
		CreatedAt: time.Now().Unix(),
	}

	am.rules[id] = rule
	_ = am.saveRulesLocked()
	return rule, nil
}

// DeleteRule removes a rule by ID.
func (am *AlertManager) DeleteRule(id string) bool {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.rules[id]; !exists {
		return false
	}
	delete(am.rules, id)
	_ = am.saveRulesLocked()
	return true
}

// ListRules returns all configured alert rules.
func (am *AlertManager) ListRules() []*AlertRule {
	am.mu.RLock()
	defer am.mu.RUnlock()

	list := make([]*AlertRule, 0, len(am.rules))
	for _, r := range am.rules {
		list = append(list, r)
	}
	return list
}

// ListTriggers returns the recent trigger events (up to 100).
func (am *AlertManager) ListTriggers() []AlertTrigger {
	am.mu.RLock()
	defer am.mu.RUnlock()

	res := make([]AlertTrigger, len(am.triggers))
	copy(res, am.triggers)
	return res
}

// Check evaluates all enabled rules for the series against the incoming point.
func (am *AlertManager) Check(series string, ts uint64, val float64) {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now().Unix()

	for _, rule := range am.rules {
		if !rule.Enabled || rule.Series != series {
			continue
		}

		// Cooldown of 5s per rule to prevent alert spamming
		if last, ok := am.cooldown[rule.ID]; ok && now-last < 5 {
			continue
		}

		triggered := false
		switch rule.Operator {
		case ">":
			triggered = val > rule.Threshold
		case "<":
			triggered = val < rule.Threshold
		case ">=":
			triggered = val >= rule.Threshold
		case "<=":
			triggered = val <= rule.Threshold
		case "==":
			triggered = val == rule.Threshold
		}

		if triggered {
			am.cooldown[rule.ID] = now
			am.nextID++
			trig := AlertTrigger{
				ID:        fmt.Sprintf("trig_%d", am.nextID%100000),
				RuleID:    rule.ID,
				Series:    series,
				Value:     val,
				Threshold: rule.Threshold,
				Operator:  rule.Operator,
				Timestamp: int64(ts),
			}

			// Prepend to trigger history (keep max 100)
			am.triggers = append([]AlertTrigger{trig}, am.triggers...)
			if len(am.triggers) > 100 {
				am.triggers = am.triggers[:100]
			}

			// Dispatch callback asynchronously if configured
			if am.onTrigger != nil {
				go am.onTrigger(trig)
			}
		}
	}
}

// ── Persistence helpers ───────────────────────────────────────────────────────

func (am *AlertManager) saveRulesLocked() error {
	path := filepath.Join(am.dataDir, "alerts.json")
	data, err := json.MarshalIndent(am.rules, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (am *AlertManager) loadRules() {
	path := filepath.Join(am.dataDir, "alerts.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &am.rules)
}
