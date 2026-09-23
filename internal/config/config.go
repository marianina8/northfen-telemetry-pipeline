// Package config loads config/northfen.yaml: detector thresholds, alert
// grouping, explain settings and the dispatch table. Behaviour lives in this
// file, not in code.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-yaml"

	northfen "github.com/marianina8/northfen-telemetry-pipeline"
)

// Detector holds the windowed-statistics parameters. Every field can be
// overridden per sensor type (zero = inherit).
type Detector struct {
	Warmup              int     `yaml:"warmup" json:"warmup"`
	EWMAAlpha           float64 `yaml:"ewma_alpha" json:"ewma_alpha"`
	SustainedZ          float64 `yaml:"sustained_z" json:"sustained_z"`
	SustainedCount      int     `yaml:"sustained_count" json:"sustained_count"`
	SpikeZ              float64 `yaml:"spike_z" json:"spike_z"`
	DriftSigma          float64 `yaml:"drift_sigma" json:"drift_sigma"`
	ClearAfter          int     `yaml:"clear_after" json:"clear_after"`
	StuckCount          int     `yaml:"stuck_count" json:"stuck_count"`
	StuckToleranceSigma float64 `yaml:"stuck_tolerance_sigma" json:"stuck_tolerance_sigma"`
	DropoutCount        int     `yaml:"dropout_count" json:"dropout_count"`
	WindowSize          int     `yaml:"window_size" json:"window_size"`
	MinSigma            float64 `yaml:"min_sigma" json:"min_sigma"`

	SensorTypes map[string]Detector `yaml:"sensor_types" json:"-"`
}

// For returns the effective parameters for one sensor type.
func (d Detector) For(sensorType string) Detector {
	out := d
	out.SensorTypes = nil
	o, ok := d.SensorTypes[sensorType]
	if !ok {
		return out
	}
	setI := func(dst *int, v int) {
		if v != 0 {
			*dst = v
		}
	}
	setF := func(dst *float64, v float64) {
		if v != 0 {
			*dst = v
		}
	}
	setI(&out.Warmup, o.Warmup)
	setF(&out.EWMAAlpha, o.EWMAAlpha)
	setF(&out.SustainedZ, o.SustainedZ)
	setI(&out.SustainedCount, o.SustainedCount)
	setF(&out.SpikeZ, o.SpikeZ)
	setF(&out.DriftSigma, o.DriftSigma)
	setI(&out.ClearAfter, o.ClearAfter)
	setI(&out.StuckCount, o.StuckCount)
	setF(&out.StuckToleranceSigma, o.StuckToleranceSigma)
	setI(&out.DropoutCount, o.DropoutCount)
	setI(&out.WindowSize, o.WindowSize)
	setF(&out.MinSigma, o.MinSigma)
	return out
}

// Alerts controls how flags become alerts and when they are explained.
type Alerts struct {
	GroupTicks            int `yaml:"group_ticks" json:"group_ticks"`
	SettleTicks           int `yaml:"settle_ticks" json:"settle_ticks"`
	MaxExplainsPerAlert   int `yaml:"max_explains_per_alert" json:"max_explains_per_alert"`
	MaxExplainsPerSession int `yaml:"max_explains_per_session" json:"max_explains_per_session"`
}

// Bedrock settings for the explain call.
type Bedrock struct {
	Region      string  `yaml:"region"`
	ModelID     string  `yaml:"model_id"`
	MaxTokens   int32   `yaml:"max_tokens"`
	Temperature float32 `yaml:"temperature"`
}

// Explain configures the one bounded model call.
type Explain struct {
	Provider        string   `yaml:"provider"`
	ContextTicks    int      `yaml:"context_ticks"`
	HistoryItems    int      `yaml:"history_items"`
	Bedrock         Bedrock  `yaml:"bedrock"`
	CauseCategories []string `yaml:"cause_categories"`
}

// Condition is one dispatch rule's match. Unset fields match anything.
type Condition struct {
	Kind            string   `yaml:"kind,omitempty" json:"kind,omitempty"`
	Severity        string   `yaml:"severity,omitempty" json:"severity,omitempty"`
	ConfidenceBelow *float64 `yaml:"confidence_below,omitempty" json:"confidence_below,omitempty"`
	ExplainFailed   *bool    `yaml:"explain_failed,omitempty" json:"explain_failed,omitempty"`
	MinAbsZ         *float64 `yaml:"min_abs_z,omitempty" json:"min_abs_z,omitempty"`
}

// Rule maps a condition to an action.
type Rule struct {
	Name   string    `yaml:"name" json:"name"`
	When   Condition `yaml:"when" json:"when"`
	Action string    `yaml:"action" json:"action"`
}

// Dispatch is the deterministic (severity, confidence) -> action table.
type Dispatch struct {
	LowConfidenceBelow  float64 `yaml:"low_confidence_below"`
	Rules               []Rule  `yaml:"rules"`
	Floors              []Rule  `yaml:"floors"`
	PageSandboxSessions bool    `yaml:"page_sandbox_sessions"`
}

// Simulate controls live pacing and sandbox limits.
type Simulate struct {
	TickSeconds       float64 `yaml:"tick_seconds"`
	SandboxTTL        string  `yaml:"sandbox_ttl"`
	MaxRunsPerSession int     `yaml:"max_runs_per_session"`
}

// TickInterval is TickSeconds as a duration.
func (s Simulate) TickInterval() time.Duration {
	return time.Duration(s.TickSeconds * float64(time.Second))
}

// TTL parses SandboxTTL (default 24h).
func (s Simulate) TTL() time.Duration {
	d, err := time.ParseDuration(s.SandboxTTL)
	if err != nil || d <= 0 {
		return 24 * time.Hour
	}
	return d
}

// Config is the whole file.
type Config struct {
	Company  string   `yaml:"company"`
	Fab      string   `yaml:"fab"`
	Detector Detector `yaml:"detector"`
	Alerts   Alerts   `yaml:"alerts"`
	Explain  Explain  `yaml:"explain"`
	Dispatch Dispatch `yaml:"dispatch"`
	Simulate Simulate `yaml:"simulate"`
}

// Actions the dispatch table may choose, in escalation order.
const (
	ActionLogOnly    = "log_only"
	ActionOpenTicket = "open_ticket"
	ActionPageOnCall = "page_oncall"
)

// ActionRank orders actions so floors and escalations only ever go up.
func ActionRank(a string) int {
	switch a {
	case ActionLogOnly:
		return 1
	case ActionOpenTicket:
		return 2
	case ActionPageOnCall:
		return 3
	}
	return 0
}

// Severities the explain step may return.
var Severities = []string{"low", "medium", "high"}

// Parse reads YAML bytes and validates them.
func Parse(b []byte) (*Config, error) {
	var c Config
	if err := yaml.UnmarshalWithOptions(b, &c, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Load reads a config file; an empty path loads the embedded default.
func Load(path string) (*Config, error) {
	if path == "" {
		return Parse(northfen.DefaultConfig)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Default returns the embedded config/northfen.yaml (panics if it's invalid,
// which the tests prevent).
func Default() *Config {
	c, err := Parse(northfen.DefaultConfig)
	if err != nil {
		panic(err)
	}
	return c
}

// Validate checks the values the detector and dispatcher rely on.
func (c *Config) Validate() error {
	d := c.Detector
	switch {
	case d.Warmup < 2:
		return fmt.Errorf("detector.warmup must be >= 2")
	case d.EWMAAlpha <= 0 || d.EWMAAlpha > 1:
		return fmt.Errorf("detector.ewma_alpha must be in (0, 1]")
	case d.SustainedZ <= 0 || d.SustainedCount < 1:
		return fmt.Errorf("detector.sustained_z and sustained_count must be positive")
	case d.SpikeZ < d.SustainedZ:
		return fmt.Errorf("detector.spike_z must be >= sustained_z")
	case d.DriftSigma <= 0:
		return fmt.Errorf("detector.drift_sigma must be positive")
	case d.ClearAfter < 1 || d.StuckCount < 2 || d.DropoutCount < 1 || d.WindowSize < 1:
		return fmt.Errorf("detector.clear_after, stuck_count, dropout_count and window_size must be positive")
	}
	for t, o := range d.SensorTypes {
		e := d.For(t)
		if e.SpikeZ < e.SustainedZ {
			return fmt.Errorf("detector.sensor_types.%s: spike_z must be >= sustained_z", t)
		}
		if o.MinSigma < 0 {
			return fmt.Errorf("detector.sensor_types.%s: min_sigma must be >= 0", t)
		}
	}
	if c.Alerts.SettleTicks < 0 || c.Alerts.GroupTicks < 0 {
		return fmt.Errorf("alerts.settle_ticks and group_ticks must be >= 0")
	}
	if c.Alerts.MaxExplainsPerAlert < 1 {
		c.Alerts.MaxExplainsPerAlert = 1
	}
	switch c.Explain.Provider {
	case "mock", "bedrock":
	default:
		return fmt.Errorf("explain.provider must be mock or bedrock")
	}
	if len(c.Explain.CauseCategories) == 0 {
		return fmt.Errorf("explain.cause_categories must not be empty")
	}
	if c.Explain.ContextTicks <= 0 {
		c.Explain.ContextTicks = 40
	}
	if len(c.Dispatch.Rules) == 0 {
		return fmt.Errorf("dispatch.rules must not be empty")
	}
	for _, r := range append(append([]Rule{}, c.Dispatch.Rules...), c.Dispatch.Floors...) {
		if ActionRank(r.Action) == 0 {
			return fmt.Errorf("dispatch rule %q: unknown action %q", r.Name, r.Action)
		}
		if r.When.Severity != "" && !validSeverity(r.When.Severity) {
			return fmt.Errorf("dispatch rule %q: unknown severity %q", r.Name, r.When.Severity)
		}
	}
	if c.Simulate.TickSeconds <= 0 {
		c.Simulate.TickSeconds = 0.4
	}
	return nil
}

func validSeverity(s string) bool {
	for _, v := range Severities {
		if v == s {
			return true
		}
	}
	return false
}
