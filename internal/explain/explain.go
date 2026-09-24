// Package explain is the one place a model is involved: a single, bounded
// Bedrock call that runs only AFTER the deterministic detector has flagged a
// window. It never decides whether something is anomalous. It reads the
// flagged window's statistics, the other sensors on the same tool over the
// same window, and the tool's recent maintenance/incident history, and
// returns a structured diagnostic hypothesis:
//
//	{ likely_causes, explanation, recommended_checks, severity, confidence }
//
// What happens next (log / ticket / page) is decided by plain code in
// internal/dispatch, not by the model.
package explain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Equipment identifies the tool being diagnosed.
type Equipment struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ToolType string `json:"tool_type"`
	Line     string `json:"line,omitempty"`
}

// Trigger is one detector flag that is part of this alert.
type Trigger struct {
	SensorID   string  `json:"sensor_id"`
	SensorType string  `json:"sensor_type"`
	Rule       string  `json:"rule"`
	Tick       int     `json:"tick"`
	Z          float64 `json:"z"`
}

// SensorSummary describes one sensor on the tool over the alert's window.
// Values are deterministic statistics computed in Go; the model only reads
// them.
type SensorSummary struct {
	SensorID             string    `json:"sensor_id"`
	SensorType           string    `json:"sensor_type"`
	Unit                 string    `json:"unit"`
	Flagged              bool      `json:"flagged"`
	Rule                 string    `json:"rule,omitempty"`
	WarmupMean           float64   `json:"warmup_mean"`
	Sigma                float64   `json:"sigma"`
	Latest               *float64  `json:"latest"`
	LatestDevSigma       float64   `json:"latest_dev_sigma"`         // (latest - warm-up mean) / sigma
	MaxDevSigma          float64   `json:"max_abs_dev_sigma"`        // largest |value - warm-up mean| / sigma in the window
	ShiftSigma           float64   `json:"shift_from_warmup_sigma"`  // (mean of the last 10 readings - warm-up mean) / sigma
	TrendSigmaPer10Ticks float64   `json:"trend_sigma_per_10_ticks"` // least-squares slope over the window
	Missing              int       `json:"missing_readings"`
	Recent               []float64 `json:"recent_values"` // downsampled window values, oldest first
}

// HistoryItem is a maintenance/incident record from the state store.
type HistoryItem struct {
	Date    string `json:"date"`
	Kind    string `json:"kind"`
	Summary string `json:"summary"`
}

// Input is everything the model sees. It is stored on the alert so anyone
// can audit exactly what the explanation was based on.
type Input struct {
	Site            string          `json:"site"`
	Equipment       Equipment       `json:"equipment"`
	WindowFromTick  int             `json:"window_from_tick"`
	WindowToTick    int             `json:"window_to_tick"`
	TickSeconds     float64         `json:"tick_seconds"`
	Triggers        []Trigger       `json:"triggers"`
	Sensors         []SensorSummary `json:"sensors"`
	History         []HistoryItem   `json:"recent_history"`
	CauseCategories []string        `json:"cause_categories"`
}

// FlaggedCount is the number of distinct sensors that flagged.
func (in Input) FlaggedCount() int {
	n := 0
	for _, s := range in.Sensors {
		if s.Flagged {
			n++
		}
	}
	return n
}

// Explanation is the model's structured output (plus audit metadata).
type Explanation struct {
	LikelyCauses      []string  `json:"likely_causes"`
	Explanation       string    `json:"explanation"`
	RecommendedChecks []string  `json:"recommended_checks"`
	Severity          string    `json:"severity"`
	Confidence        float64   `json:"confidence"`
	Model             string    `json:"model,omitempty"`
	At                time.Time `json:"at"`
	Raw               string    `json:"raw,omitempty"` // exact model text, for the audit trail
}

// Model is one explain backend (mock or Bedrock).
type Model interface {
	Name() string
	Explain(ctx context.Context, in Input) (Explanation, error)
}

// ErrBadOutput marks a reply that didn't match the schema. The pipeline
// treats it like any other failure: the alert goes to a human.
var ErrBadOutput = errors.New("explain: model output did not match the schema")

var severities = map[string]bool{"low": true, "medium": true, "high": true}

// Parse strictly parses and validates the model's JSON reply. Causes must
// come from the configured categories.
func Parse(text string, categories []string) (Explanation, error) {
	body := strings.TrimSpace(text)
	body = strings.TrimPrefix(body, "```json")
	body = strings.TrimPrefix(body, "```")
	body = strings.TrimSuffix(body, "```")
	if i, j := strings.Index(body, "{"), strings.LastIndex(body, "}"); i >= 0 && j > i {
		body = body[i : j+1]
	}
	var raw struct {
		LikelyCauses      []string `json:"likely_causes"`
		Explanation       string   `json:"explanation"`
		RecommendedChecks []string `json:"recommended_checks"`
		Severity          string   `json:"severity"`
		Confidence        *float64 `json:"confidence"`
	}
	dec := json.NewDecoder(strings.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return Explanation{}, fmt.Errorf("%w: %v", ErrBadOutput, err)
	}
	e := Explanation{
		Explanation: strings.TrimSpace(raw.Explanation),
		Severity:    strings.ToLower(strings.TrimSpace(raw.Severity)),
		Raw:         text,
	}
	allowed := map[string]bool{}
	for _, c := range categories {
		allowed[c] = true
	}
	for _, c := range raw.LikelyCauses {
		c = strings.ToLower(strings.TrimSpace(c))
		if !allowed[c] {
			return Explanation{}, fmt.Errorf("%w: cause %q is not one of the configured categories", ErrBadOutput, c)
		}
		e.LikelyCauses = appendUnique(e.LikelyCauses, c)
	}
	for _, c := range raw.RecommendedChecks {
		if c = strings.TrimSpace(c); c != "" {
			e.RecommendedChecks = append(e.RecommendedChecks, c)
		}
	}
	switch {
	case len(e.LikelyCauses) == 0 || len(e.LikelyCauses) > 5:
		return Explanation{}, fmt.Errorf("%w: likely_causes must have 1-5 entries", ErrBadOutput)
	case e.Explanation == "" || len(e.Explanation) > 800:
		return Explanation{}, fmt.Errorf("%w: explanation must be 1-800 characters", ErrBadOutput)
	case len(e.RecommendedChecks) == 0 || len(e.RecommendedChecks) > 6:
		return Explanation{}, fmt.Errorf("%w: recommended_checks must have 1-6 entries", ErrBadOutput)
	case !severities[e.Severity]:
		return Explanation{}, fmt.Errorf("%w: severity must be low, medium or high", ErrBadOutput)
	case raw.Confidence == nil || *raw.Confidence < 0 || *raw.Confidence > 1:
		return Explanation{}, fmt.Errorf("%w: confidence must be a number from 0 to 1", ErrBadOutput)
	}
	e.Confidence = *raw.Confidence
	return e, nil
}

func appendUnique(xs []string, x string) []string {
	for _, v := range xs {
		if v == x {
			return xs
		}
	}
	return append(xs, x)
}
