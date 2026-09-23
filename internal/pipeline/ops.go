package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// ErrRunLimit is returned when a sandbox session has used its runs.
var ErrRunLimit = errors.New("this demo session has used all its scenario runs")

// NewRunID returns a random run ID ("R" + 10 hex characters).
func NewRunID() string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "R" + hex.EncodeToString(b)
}

// RunRequest starts one scenario run.
type RunRequest struct {
	SessionID string
	Scenario  string // name or numeric prefix ("04")
	Source    string // dashboard | webhook | cli | mcp
	RunID     string // optional (tests); random otherwise
	Start     time.Time
	// TickSeconds overrides the configured live pacing (0 = config). The
	// timestamps of the readings are spaced by this interval.
	TickSeconds float64
}

// StartRun records a run and returns it with every reading it will produce
// (generated deterministically from the scenario's seed). The caller decides
// how the readings reach the consumer: all at once (CLI), paced by a local
// streamer (local dashboard) or via Kinesis (AWS).
func (s *Service) StartRun(ctx context.Context, req RunRequest) (store.Run, []telemetry.Reading, error) {
	if req.SessionID == "" {
		req.SessionID = telemetry.LocalSession
	}
	if !telemetry.ValidID(req.SessionID) {
		return store.Run{}, nil, fmt.Errorf("invalid session id %q", req.SessionID)
	}
	sc, ok := s.Cat.Scenario(req.Scenario)
	if !ok {
		return store.Run{}, nil, fmt.Errorf("unknown scenario %q (see `northfen scenarios`)", req.Scenario)
	}
	if req.RunID == "" {
		req.RunID = NewRunID()
	}
	if req.Start.IsZero() {
		req.Start = s.now()
	}
	tick := req.TickSeconds
	if tick <= 0 {
		tick = s.Cfg.Simulate.TickSeconds
	}
	var exp int64
	if IsSandbox(req.SessionID) || s.ExpireAll {
		exp = s.now().Add(s.Cfg.Simulate.TTL()).Unix()
	}
	if IsSandbox(req.SessionID) {
		if _, err := s.Store.Incr(ctx, req.SessionID, "runs", s.Cfg.Simulate.MaxRunsPerSession, exp); err != nil {
			if errors.Is(err, store.ErrLimit) {
				return store.Run{}, nil, ErrRunLimit
			}
			return store.Run{}, nil, err
		}
	}
	run := store.Run{
		ID: req.RunID, SessionID: req.SessionID, Scenario: sc.Name, Title: sc.Title, EquipmentID: sc.EquipmentID,
		Ticks: sc.Ticks, TickSeconds: tick, StartedAt: req.Start.UTC(), Source: req.Source, ExpiresAt: exp,
	}
	if err := s.Store.PutRun(ctx, run); err != nil {
		return run, nil, err
	}
	rs, err := s.Readings(run)
	return run, rs, err
}

// Readings regenerates a run's readings (deterministic, so the simulator
// Lambda can rebuild them from the run record alone).
func (s *Service) Readings(run store.Run) ([]telemetry.Reading, error) {
	sc, ok := s.Cat.Scenario(run.Scenario)
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q", run.Scenario)
	}
	return sim.Generate(s.Cat, sc, sim.Options{
		SessionID: run.SessionID, RunID: run.ID, Start: run.StartedAt,
		Interval: time.Duration(run.TickSeconds * float64(time.Second)),
	})
}

// SeedHistory writes the synthetic maintenance/incident history for every
// tool (idempotent). The explain step reads it from the store.
func (s *Service) SeedHistory(ctx context.Context) error {
	var all []sim.HistoryRecord
	for _, eq := range s.Cat.Equipment {
		all = append(all, eq.HistoryAsOf(s.now())...)
	}
	return s.Store.PutHistory(ctx, all)
}

// Due reports whether an alert's settle time has passed: the tool has
// streamed past the alert's due tick, or its run has finished.
func (s *Service) Due(ctx context.Context, a store.Alert) (bool, error) {
	if a.Explain == store.ExplainSkipped {
		return true, nil
	}
	if run, err := s.Store.GetRun(ctx, a.SessionID, a.RunID); err == nil && !s.now().Before(run.EndsAt()) {
		return true, nil
	}
	states, err := s.Store.ListStates(ctx, a.SessionID)
	if err != nil {
		return false, err
	}
	for _, st := range states {
		if st.EquipmentID == a.EquipmentID && st.State.RunID == a.RunID && st.State.LastTick >= a.DueTick {
			return true, nil
		}
	}
	return false, nil
}

// Sweep resolves every alert in a session that needs it (local mode: there
// is no SQS, so the local runner calls this after each tick). With
// dueOnly=false it resolves regardless of settle time (batch runs, where the
// whole stream has already been scored).
func (s *Service) Sweep(ctx context.Context, session string, dueOnly bool) ([]store.Alert, error) {
	as, err := s.Store.ListAlerts(ctx, session)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(as, func(i, j int) bool { return as[i].FirstTick < as[j].FirstTick })
	var out []store.Alert
	for _, a := range as {
		if !s.NeedsResolve(a) {
			continue
		}
		if dueOnly {
			ok, err := s.Due(ctx, a)
			if err != nil {
				return out, err
			}
			if !ok {
				continue
			}
		}
		r, err := s.Resolve(ctx, a.ID, "")
		if err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, nil
}

// RunBatch is the non-streaming path used by the CLI and tests: start a run,
// score all of its readings in Kinesis-sized batches, then resolve every
// alert.
func (s *Service) RunBatch(ctx context.Context, req RunRequest, batchSize int) (store.Run, []store.Alert, error) {
	run, rs, err := s.StartRun(ctx, req)
	if err != nil {
		return run, nil, err
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	for i := 0; i < len(rs); i += batchSize {
		j := min(i+batchSize, len(rs))
		if _, err := s.Process(ctx, rs[i:j]); err != nil {
			return run, nil, err
		}
	}
	if _, err := s.Sweep(ctx, run.SessionID, false); err != nil {
		return run, nil, err
	}
	as, err := s.RunAlerts(ctx, run.SessionID, run.ID)
	return run, as, err
}

// RunAlerts lists one run's alerts, oldest first.
func (s *Service) RunAlerts(ctx context.Context, session, runID string) ([]store.Alert, error) {
	as, err := s.Store.ListAlerts(ctx, session)
	if err != nil {
		return nil, err
	}
	var out []store.Alert
	for _, a := range as {
		if a.RunID == runID {
			out = append(out, a)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].FirstTick < out[j].FirstTick })
	return out, nil
}

// ---- human side: acknowledge / dismiss / escalate ---------------------------

// Ack verbs.
const (
	VerbAcknowledge = "acknowledge"
	VerbDismiss     = "dismiss"
	VerbEscalate    = "escalate"
)

// AckRequest is one human (or permitted agent) decision on an alert.
type AckRequest struct {
	AlertID string
	Verb    string // acknowledge | dismiss | escalate
	Actor   string // "engineer:alex", "mcp:agent", ...
	Note    string
	// SessionID, when set, must match the alert's session (sandbox
	// isolation for the dashboard).
	SessionID string
}

// ParseVerb normalizes ack verbs ("ack" -> acknowledge).
func ParseVerb(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "ack", "acknowledge", "acknowledged", "":
		return VerbAcknowledge, nil
	case "dismiss", "dismissed":
		return VerbDismiss, nil
	case "escalate", "escalated":
		return VerbEscalate, nil
	}
	return "", fmt.Errorf("unknown action %q (acknowledge, dismiss or escalate)", v)
}

// Ack records a human decision in the audit trail. Dismiss requires a note
// (the audit trail must say why an alert was waved off). Escalate pages
// on-call if that hasn't happened already. A dismissed alert is closed.
func (s *Service) Ack(ctx context.Context, req AckRequest) (store.Alert, error) {
	verb, err := ParseVerb(req.Verb)
	if err != nil {
		return store.Alert{}, err
	}
	note := strings.TrimSpace(req.Note)
	if verb == VerbDismiss && note == "" {
		return store.Alert{}, fmt.Errorf("dismiss needs a note so the audit trail says why")
	}
	if len(note) > 500 {
		note = note[:500]
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = "engineer"
	}
	for attempt := 0; ; attempt++ {
		a, err := s.Store.GetAlert(ctx, req.AlertID)
		if err != nil {
			return a, err
		}
		if req.SessionID != "" && a.SessionID != req.SessionID {
			return store.Alert{}, fmt.Errorf("alert %s: %w", req.AlertID, store.ErrNotFound)
		}
		if a.Status == store.StatusDismissed {
			return a, fmt.Errorf("alert %s was already dismissed", a.ID)
		}
		now := s.now()
		detail := note
		switch verb {
		case VerbAcknowledge:
			if a.Status == store.StatusOpen {
				a.Status = store.StatusAcknowledged
			}
			a.AddEvent(now, actor, "acknowledged", detail)
		case VerbDismiss:
			a.Status = store.StatusDismissed
			a.AddEvent(now, actor, "dismissed", detail)
		case VerbEscalate:
			a.Status = store.StatusEscalated
			a.AddEvent(now, actor, "escalated", detail)
			if !a.Took(config.ActionPageOnCall) {
				rec, err := s.Sink.Do(ctx, &a, config.ActionPageOnCall, a.ExpiresAt > 0)
				if err != nil {
					a.AddEvent(now, "dispatch", "action-failed", fmt.Sprintf("%s: %v", config.ActionPageOnCall, err))
				} else {
					rec.Detail += " (human escalation)"
					a.Actions = append(a.Actions, rec)
					a.AddEvent(now, "dispatch", "action", rec.Detail)
				}
			}
		}
		err = s.Store.UpdateAlert(ctx, &a)
		if err == nil || attempt >= 3 || !isConflict(err) {
			return a, err
		}
	}
}

// ---- read side --------------------------------------------------------------

// AlertSummary is one row of the anomaly feed.
type AlertSummary struct {
	ID          string    `json:"id"`
	SessionID   string    `json:"session_id"`
	RunID       string    `json:"run_id"`
	Scenario    string    `json:"scenario,omitempty"`
	EquipmentID string    `json:"equipment_id"`
	Kind        string    `json:"kind"`
	Sensors     []string  `json:"sensors"`
	Rules       []string  `json:"rules"`
	FirstTick   int       `json:"first_tick"`
	MaxAbsZ     float64   `json:"max_abs_z"`
	Explain     string    `json:"explain_state"`
	Severity    string    `json:"severity,omitempty"`
	Confidence  float64   `json:"confidence,omitempty"`
	TopCause    string    `json:"top_cause,omitempty"`
	Action      string    `json:"action,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// Summarize builds a feed row.
func Summarize(a store.Alert) AlertSummary {
	out := AlertSummary{ID: a.ID, SessionID: a.SessionID, RunID: a.RunID, Scenario: a.Scenario, EquipmentID: a.EquipmentID,
		Kind: a.Kind, FirstTick: a.FirstTick, MaxAbsZ: round(a.MaxAbsZ, 2), Explain: string(a.Explain),
		Status: string(a.Status), CreatedAt: a.CreatedAt}
	for _, f := range a.Flags {
		out.Sensors = append(out.Sensors, f.SensorID)
		out.Rules = append(out.Rules, f.Rule)
	}
	if e := a.Explanation; e != nil && a.Explain == store.ExplainDone {
		out.Severity, out.Confidence = e.Severity, e.Confidence
		if len(e.LikelyCauses) > 0 {
			out.TopCause = e.LikelyCauses[0]
		}
	}
	if d := a.Decision; d != nil {
		out.Action = d.Action
	}
	return out
}

// Feed lists a session's alerts, newest first ("" = every session).
func (s *Service) Feed(ctx context.Context, session string) ([]AlertSummary, error) {
	as, err := s.Store.ListAlerts(ctx, session)
	if err != nil {
		return nil, err
	}
	out := make([]AlertSummary, 0, len(as))
	for _, a := range as {
		out = append(out, Summarize(a))
	}
	return out, nil
}

// AlertForWindow finds the alert a scored window belongs to. Windows the
// detector didn't flag have none: explain never runs on them.
func (s *Service) AlertForWindow(ctx context.Context, windowID string) (store.Alert, store.Window, error) {
	w, err := s.Store.GetWindow(ctx, windowID)
	if err != nil {
		return store.Alert{}, w, err
	}
	if w.AlertID == "" {
		return store.Alert{}, w, fmt.Errorf("window %s was not flagged by the detector (status %s) - explain only runs on flagged windows", w.ID, w.Status)
	}
	a, err := s.Store.GetAlert(ctx, w.AlertID)
	return a, w, err
}

// SensorHistory returns one sensor's stored readings and scored windows for
// a run (the MCP get-sensor-history tool and the dashboard chart use this).
type SensorHistory struct {
	EquipmentID string              `json:"equipment_id"`
	SensorID    string              `json:"sensor_id"`
	RunID       string              `json:"run_id"`
	Readings    []telemetry.Reading `json:"readings"`
	Windows     []store.Window      `json:"windows"`
}

// SensorHistory loads a sensor's readings and windows for one run.
func (s *Service) SensorHistory(ctx context.Context, session, runID, equipmentID, sensorID string) (SensorHistory, error) {
	run, err := s.Store.GetRun(ctx, session, runID)
	if err != nil {
		return SensorHistory{}, err
	}
	rs, err := s.Store.Readings(ctx, session, equipmentID, sensorID, run.StartedAt, run.EndsAt().Add(time.Minute))
	if err != nil {
		return SensorHistory{}, err
	}
	out := SensorHistory{EquipmentID: equipmentID, SensorID: sensorID, RunID: runID}
	for _, r := range rs {
		if r.RunID == runID {
			out.Readings = append(out.Readings, r)
		}
	}
	ws, err := s.Store.ListWindows(ctx, session, runID)
	if err != nil {
		return out, err
	}
	for _, w := range ws {
		if w.EquipmentID == equipmentID && w.SensorID == sensorID {
			out.Windows = append(out.Windows, w)
		}
	}
	return out, nil
}
