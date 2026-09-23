// Package store is the pipeline's state: raw readings (time series), the
// detector's per-series state, every scored window, alerts with their
// explanation, dispatch decision and human acknowledgments (the audit
// trail), visitor runs and equipment history.
//
// Implementations: Mem (tests), File (local dev, survives restarts and is
// shared by the CLI, local consumer and local dashboard) and Dynamo (AWS).
package store

import (
	"context"
	"errors"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/detect"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// Errors.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict: record changed since it was read")
	ErrLimit    = errors.New("limit reached")
)

// Alert lifecycle status (the human side).
type Status string

const (
	StatusOpen         Status = "open"
	StatusAcknowledged Status = "acknowledged"
	StatusDismissed    Status = "dismissed"
	StatusEscalated    Status = "escalated"
)

// ExplainState tracks the model step.
type ExplainState string

const (
	ExplainPending ExplainState = "pending"
	ExplainDone    ExplainState = "done"
	ExplainFailed  ExplainState = "failed"
	ExplainSkipped ExplainState = "skipped" // sensor faults: no model call by design
)

// Event is one audit-trail entry.
type Event struct {
	At     time.Time `json:"at"`
	Actor  string    `json:"actor"`
	Type   string    `json:"type"`
	Detail string    `json:"detail,omitempty"`
}

// SeriesFlag is one sensor's flag within an alert.
type SeriesFlag struct {
	SensorID   string    `json:"sensor_id"`
	SensorType string    `json:"sensor_type"`
	Unit       string    `json:"unit"`
	Rule       string    `json:"rule"`
	Tick       int       `json:"tick"`
	TS         time.Time `json:"ts"`
	Z          float64   `json:"z"`
	WindowID   string    `json:"window_id"`
}

// Decision is the dispatch table's verdict.
type Decision struct {
	Action string    `json:"action"`
	Rule   string    `json:"rule"`
	Reason string    `json:"reason"`
	Floor  string    `json:"floor,omitempty"`
	At     time.Time `json:"at"`
}

// ActionRecord is one action actually taken.
type ActionRecord struct {
	Action    string    `json:"action"`
	At        time.Time `json:"at"`
	Detail    string    `json:"detail"`
	Simulated bool      `json:"simulated,omitempty"`
	Ref       string    `json:"ref,omitempty"` // ticket ID, SNS message ID, ...
}

// Alert is one tool incident: one or more sensor flags on the same tool,
// grouped, explained once, dispatched, and acknowledged by a human.
type Alert struct {
	ID             string               `json:"id"`
	SessionID      string               `json:"session_id"`
	RunID          string               `json:"run_id"`
	Scenario       string               `json:"scenario,omitempty"`
	EquipmentID    string               `json:"equipment_id"`
	ToolType       string               `json:"tool_type"`
	Kind           string               `json:"kind"`
	WindowID       string               `json:"window_id"` // window of the first flag
	FirstTick      int                  `json:"first_tick"`
	FirstTS        time.Time            `json:"first_ts"`
	Flags          []SeriesFlag         `json:"flags"`
	MaxAbsZ        float64              `json:"max_abs_z"`
	Status         Status               `json:"status"`
	Explain        ExplainState         `json:"explain_state"`
	DueTick        int                  `json:"explain_due_tick"`
	Explains       int                  `json:"explains"`
	ExplainedFlags int                  `json:"explained_flags"` // flags included in the latest explanation
	Input          *explain.Input       `json:"explain_input,omitempty"`
	Explanation    *explain.Explanation `json:"explanation,omitempty"`
	ExplainErr     string               `json:"explain_error,omitempty"`
	Decision       *Decision            `json:"decision,omitempty"`
	Actions        []ActionRecord       `json:"actions,omitempty"`
	Events         []Event              `json:"events"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
	ExpiresAt      int64                `json:"expires_at,omitempty"`
	Version        int                  `json:"version"`
}

// AddEvent appends to the audit trail.
func (a *Alert) AddEvent(at time.Time, actor, typ, detail string) {
	a.Events = append(a.Events, Event{At: at.UTC(), Actor: actor, Type: typ, Detail: detail})
	a.UpdatedAt = at.UTC()
}

// HasFlag reports whether a sensor already flagged on this alert.
func (a *Alert) HasFlag(sensorID string) bool {
	for _, f := range a.Flags {
		if f.SensorID == sensorID {
			return true
		}
	}
	return false
}

// Took reports whether an action was already taken for this alert.
func (a *Alert) Took(action string) bool {
	for _, r := range a.Actions {
		if r.Action == action {
			return true
		}
	}
	return false
}

// Window is one scored window for one series (every window is stored).
type Window struct {
	ID          string    `json:"id"`
	SessionID   string    `json:"session_id"`
	RunID       string    `json:"run_id"`
	EquipmentID string    `json:"equipment_id"`
	SensorID    string    `json:"sensor_id"`
	SensorType  string    `json:"sensor_type"`
	StartTick   int       `json:"start_tick"`
	EndTick     int       `json:"end_tick"`
	StartTS     time.Time `json:"start_ts"`
	EndTS       time.Time `json:"end_ts"`
	N           int       `json:"n"`
	Missing     int       `json:"missing"`
	Mean        float64   `json:"mean"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	MaxAbsZ     float64   `json:"max_abs_z"`
	LastZ       *float64  `json:"last_z,omitempty"`
	Baseline    float64   `json:"baseline"`
	Sigma       float64   `json:"sigma"`
	Status      string    `json:"status"` // warmup | normal | flagged | fault
	Rules       []string  `json:"rules,omitempty"`
	AlertID     string    `json:"alert_id,omitempty"`
	ExpiresAt   int64     `json:"expires_at,omitempty"`
}

// SeriesState is the detector state for one sensor in one session.
type SeriesState struct {
	SessionID   string       `json:"session_id"`
	EquipmentID string       `json:"equipment_id"`
	SensorID    string       `json:"sensor_id"`
	SensorType  string       `json:"sensor_type"`
	Unit        string       `json:"unit"`
	State       detect.State `json:"state"`
	LastTS      time.Time    `json:"last_ts"`
	WinStartTS  time.Time    `json:"win_start_ts"`           // timestamp of the open window's first reading
	WinAlertID  string       `json:"win_alert_id,omitempty"` // alert raised inside the open window
	ExpiresAt   int64        `json:"expires_at,omitempty"`
}

// Key is "<equipment>#<sensor>".
func (s SeriesState) Key() string { return telemetry.SeriesKey(s.EquipmentID, s.SensorID) }

// Run is one scenario run in a session.
type Run struct {
	ID          string    `json:"id"`
	SessionID   string    `json:"session_id"`
	Scenario    string    `json:"scenario"`
	Title       string    `json:"title"`
	EquipmentID string    `json:"equipment_id"`
	Ticks       int       `json:"ticks"`
	TickSeconds float64   `json:"tick_seconds"`
	StartedAt   time.Time `json:"started_at"`
	Source      string    `json:"source"` // dashboard | webhook | cli | mcp
	ExpiresAt   int64     `json:"expires_at,omitempty"`
}

// EndsAt is when the last reading is due.
func (r Run) EndsAt() time.Time {
	return r.StartedAt.Add(time.Duration(float64(r.Ticks) * r.TickSeconds * float64(time.Second)))
}

// Session holds per-visitor counters (public-demo guard rails).
type Session struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Runs      int       `json:"runs"`
	Explains  int       `json:"explains"`
	ExpiresAt int64     `json:"expires_at,omitempty"`
}

// Store is the persistence interface.
type Store interface {
	// Time series.
	PutReadings(ctx context.Context, rs []telemetry.Reading, expiresAt int64) error
	Readings(ctx context.Context, sessionID, equipmentID, sensorID string, from, to time.Time) ([]telemetry.Reading, error)

	// Detector state (keyed by session + equipment#sensor).
	GetStates(ctx context.Context, sessionID string, keys []string) (map[string]SeriesState, error)
	PutStates(ctx context.Context, states []SeriesState) error
	ListStates(ctx context.Context, sessionID string) ([]SeriesState, error)

	// Scored windows.
	PutWindows(ctx context.Context, ws []Window) error
	GetWindow(ctx context.Context, id string) (Window, error)
	ListWindows(ctx context.Context, sessionID, runID string) ([]Window, error)

	// Alerts (audit trail lives on the alert).
	CreateAlert(ctx context.Context, a Alert) (bool, error) // false if it already existed
	GetAlert(ctx context.Context, id string) (Alert, error)
	UpdateAlert(ctx context.Context, a *Alert) error                   // optimistic: fails with ErrConflict on a stale Version
	ListAlerts(ctx context.Context, sessionID string) ([]Alert, error) // "" = every session

	// Runs and session counters.
	PutRun(ctx context.Context, r Run) error
	GetRun(ctx context.Context, sessionID, runID string) (Run, error)
	ListRuns(ctx context.Context, sessionID string) ([]Run, error)
	GetSession(ctx context.Context, id string) (Session, error)
	// Incr adds 1 to a session counter ("runs" or "explains") unless it is
	// already >= max (max <= 0 = unlimited). Creates the session if needed.
	Incr(ctx context.Context, id, counter string, max int, expiresAt int64) (int, error)

	// Equipment history (synthetic maintenance/incident records).
	PutHistory(ctx context.Context, hs []sim.HistoryRecord) error
	History(ctx context.Context, equipmentID string, limit int) ([]sim.HistoryRecord, error)
}
