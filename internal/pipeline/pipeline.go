// Package pipeline wires the stages together. The same code runs locally
// (CLI, local consumer, local dashboard) and in the Lambdas:
//
//	Process   stream batch -> deterministic scoring -> windows, flags -> alerts
//	          (the Kinesis consumer; no model call, ever)
//	Resolve   flagged alert -> Bedrock explain (anomalies only) -> dispatch
//	          table -> actions (the explain worker)
//	Ack       human acknowledge / dismiss / escalate -> audit trail
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/detect"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// ExplainJob asks the explain worker to resolve one alert.
type ExplainJob struct {
	AlertID   string        `json:"alert_id"`
	SessionID string        `json:"session_id"`
	DueTick   int           `json:"due_tick"`
	Attempt   int           `json:"attempt,omitempty"` // times re-queued because it wasn't due yet
	Delay     time.Duration `json:"-"`                 // how long to wait before running (settle time)
}

// JobQueue carries explain jobs from the consumer to the explain worker
// (SQS in AWS). Locally the consumer uses NoQueue and Sweep instead.
type JobQueue interface {
	EnqueueExplain(ctx context.Context, j ExplainJob) error
}

// NoQueue drops jobs; pending alerts are picked up by Sweep (local mode).
type NoQueue struct{}

// EnqueueExplain implements JobQueue.
func (NoQueue) EnqueueExplain(context.Context, ExplainJob) error { return nil }

// Service is the pipeline.
type Service struct {
	Cfg   *config.Config
	Cat   *sim.Catalog
	Store store.Store
	Model explain.Model
	Sink  dispatch.Sink
	Queue JobQueue
	Log   *slog.Logger
	Now   func() time.Time
	// ExplainTimeout bounds one model call.
	ExplainTimeout time.Duration
	// ExpireAll gives every session (not only visitor sandboxes) the sandbox
	// TTL. The deployed demo stack sets it so nothing lingers.
	ExpireAll bool
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func (s *Service) queue() JobQueue {
	if s.Queue == nil {
		return NoQueue{}
	}
	return s.Queue
}

func shortHash(n int, parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return strings.ToUpper(hex.EncodeToString(h[:]))[:n]
}

// AlertID is deterministic, so a replayed Kinesis batch recreates the same
// alert instead of a duplicate.
func AlertID(session, run, equipment, sensor string, tick int) string {
	return "NF-" + shortHash(8, session, run, equipment, sensor, fmt.Sprint(tick))
}

// WindowID is deterministic for the same reason.
func WindowID(session, run, equipment, sensor string, startTick int) string {
	return "W-" + shortHash(10, session, run, equipment, sensor, fmt.Sprint(startTick))
}

// ProcessResult summarizes one batch.
type ProcessResult struct {
	Readings  int      `json:"readings"`
	Skipped   int      `json:"skipped"` // replays and invalid readings
	Windows   int      `json:"windows"`
	Flags     int      `json:"flags"`
	NewAlerts []string `json:"new_alerts,omitempty"`
	Joined    []string `json:"joined_alerts,omitempty"`
}

type pendingFlag struct {
	r      telemetry.Reading
	flag   detect.Flag
	window string
	st     *store.SeriesState
}

// Process scores a batch of readings (the Kinesis consumer). It is
// idempotent: readings at or before a series' last processed tick (same
// run) are skipped, and alert/window IDs are deterministic. The detector
// state is written last, so it acts as the checkpoint.
func (s *Service) Process(ctx context.Context, batch []telemetry.Reading) (ProcessResult, error) {
	var res ProcessResult
	bySession := map[string][]telemetry.Reading{}
	var sessions []string
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			s.log().Warn("dropping invalid reading", "err", err)
			res.Skipped++
			continue
		}
		if _, ok := bySession[r.SessionID]; !ok {
			sessions = append(sessions, r.SessionID)
		}
		bySession[r.SessionID] = append(bySession[r.SessionID], r)
	}
	for _, sess := range sessions {
		if err := s.processSession(ctx, sess, bySession[sess], &res); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (s *Service) processSession(ctx context.Context, session string, rs []telemetry.Reading, res *ProcessResult) error {
	sort.SliceStable(rs, func(i, j int) bool { return rs[i].Tick < rs[j].Tick })
	var keys []string
	seen := map[string]bool{}
	for _, r := range rs {
		if k := r.SeriesKey(); !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	loaded, err := s.Store.GetStates(ctx, session, keys)
	if err != nil {
		return fmt.Errorf("load detector state: %w", err)
	}
	states := map[string]*store.SeriesState{}
	exp := s.expiry(session, rs)

	var fresh []telemetry.Reading
	var windows []store.Window
	var flags []pendingFlag
	for _, r := range rs {
		k := r.SeriesKey()
		st := states[k]
		if st == nil {
			if ls, ok := loaded[k]; ok {
				st = &ls
			} else {
				st = &store.SeriesState{SessionID: session, EquipmentID: r.EquipmentID, SensorID: r.SensorID}
			}
			if st.State.RunID != r.RunID {
				// New run on this tool: start the detector from scratch.
				*st = store.SeriesState{SessionID: session, EquipmentID: r.EquipmentID, SensorID: r.SensorID}
				st.State.RunID = r.RunID
			}
			st.SensorType, st.Unit, st.ExpiresAt = r.SensorType, r.Unit, exp
			states[k] = st
		}
		if st.State.Started && r.Tick <= st.State.LastTick {
			res.Skipped++
			continue
		}
		fresh = append(fresh, r)
		if st.State.Win.Count == 0 {
			st.WinStartTS, st.WinAlertID = r.TS, ""
		}
		startTick := st.State.Win.StartTick
		if st.State.Win.Count == 0 {
			startTick = r.Tick
		}
		out := detect.Step(s.Cfg.Detector.For(r.SensorType), &st.State, r.Tick, r.Value)
		st.LastTS = r.TS
		wid := WindowID(session, r.RunID, r.EquipmentID, r.SensorID, startTick)
		if out.Flag != nil {
			res.Flags++
			flags = append(flags, pendingFlag{r: r, flag: *out.Flag, window: wid, st: st})
		}
		if out.Window != nil {
			windows = append(windows, s.window(st, r, *out.Window, wid, exp))
		}
	}
	res.Readings += len(fresh)

	// Group flags into alerts (in tick order across the tool's sensors).
	sort.SliceStable(flags, func(i, j int) bool { return flags[i].flag.Tick < flags[j].flag.Tick })
	var jobs []ExplainJob
	if len(flags) > 0 {
		alerts, err := s.Store.ListAlerts(ctx, session)
		if err != nil {
			return err
		}
		for _, pf := range flags {
			job, created, joined, err := s.alertFor(ctx, &alerts, pf, exp)
			if err != nil {
				return err
			}
			pf.st.State.EpisodeID = job.AlertID
			if w := pf.st.State.Win; w.Count > 0 && WindowID(session, pf.r.RunID, pf.r.EquipmentID, pf.r.SensorID, w.StartTick) == pf.window {
				pf.st.WinAlertID = job.AlertID // the flag's window is still open
			}
			for i := range windows {
				if windows[i].ID == pf.window {
					windows[i].AlertID = job.AlertID
				}
			}
			if created {
				res.NewAlerts = append(res.NewAlerts, job.AlertID)
			}
			if joined {
				res.Joined = append(res.Joined, job.AlertID)
			}
			if created || joined {
				jobs = append(jobs, job)
			}
		}
	}

	if err := s.Store.PutReadings(ctx, fresh, exp); err != nil {
		return fmt.Errorf("write readings: %w", err)
	}
	if err := s.Store.PutWindows(ctx, windows); err != nil {
		return fmt.Errorf("write windows: %w", err)
	}
	res.Windows += len(windows)
	for _, j := range jobs {
		if err := s.queue().EnqueueExplain(ctx, j); err != nil {
			return fmt.Errorf("enqueue explain %s: %w", j.AlertID, err)
		}
	}
	var out []store.SeriesState
	for _, k := range keys {
		if st := states[k]; st != nil {
			out = append(out, *st)
		}
	}
	if err := s.Store.PutStates(ctx, out); err != nil {
		return fmt.Errorf("write detector state: %w", err)
	}
	return nil
}

func (s *Service) window(st *store.SeriesState, r telemetry.Reading, w detect.Window, id string, exp int64) store.Window {
	status := "normal"
	switch {
	case !w.Warm:
		status = "warmup"
	case len(w.Rules) > 0 && detect.KindOf(w.Rules[0]) == detect.KindSensorFault:
		status = "fault"
	case len(w.Rules) > 0:
		status = "flagged"
	}
	// A window inside an episode that opened earlier belongs to that alert.
	alertID := st.WinAlertID
	if alertID == "" && len(w.Rules) > 0 {
		alertID = st.State.EpisodeID
	}
	return store.Window{
		ID: id, SessionID: r.SessionID, RunID: r.RunID, EquipmentID: r.EquipmentID, SensorID: r.SensorID,
		SensorType: r.SensorType, StartTick: w.StartTick, EndTick: w.EndTick, StartTS: st.WinStartTS, EndTS: r.TS,
		N: w.N, Missing: w.Missing, Mean: w.Mean(), Min: w.Min, Max: w.Max, MaxAbsZ: w.MaxAbsZ, LastZ: w.LastZ,
		Baseline: st.State.Baseline, Sigma: st.State.Sigma, Status: status, Rules: w.Rules, AlertID: alertID,
		ExpiresAt: exp,
	}
}

// alertFor creates a new alert for a flag, or joins an open alert of the
// same kind on the same tool (same run) that started within group_ticks.
func (s *Service) alertFor(ctx context.Context, alerts *[]store.Alert, pf pendingFlag, exp int64) (ExplainJob, bool, bool, error) {
	now := s.now()
	r, f := pf.r, pf.flag
	sf := store.SeriesFlag{SensorID: r.SensorID, SensorType: r.SensorType, Unit: r.Unit, Rule: f.Rule, Tick: f.Tick, TS: r.TS, Z: f.Z, WindowID: pf.window}
	settle := s.Cfg.Alerts.SettleTicks
	if f.Kind == detect.KindSensorFault {
		settle = 0
	}

	for i := range *alerts {
		a := &(*alerts)[i]
		if a.RunID != r.RunID || a.EquipmentID != r.EquipmentID || a.Kind != f.Kind ||
			a.Status == store.StatusDismissed || f.Tick-a.FirstTick > s.Cfg.Alerts.GroupTicks || f.Tick < a.FirstTick {
			continue
		}
		job := ExplainJob{AlertID: a.ID, SessionID: a.SessionID, DueTick: f.Tick + settle, Delay: s.tickDelay(settle)}
		if a.HasFlag(r.SensorID) {
			return job, false, false, nil
		}
		for attempt := 0; ; attempt++ {
			a.Flags = append(a.Flags, sf)
			a.MaxAbsZ = math.Max(a.MaxAbsZ, math.Abs(f.Z))
			a.AddEvent(now, "detector", "correlated",
				fmt.Sprintf("%s joined: %s rule at tick %d (z=%.2f)", r.SensorID, f.Rule, f.Tick, f.Z))
			if a.Explain != store.ExplainSkipped && a.Explains < s.Cfg.Alerts.MaxExplainsPerAlert {
				a.DueTick = job.DueTick
				if a.Explain != store.ExplainPending {
					a.Explain = store.ExplainPending
					a.AddEvent(now, "detector", "re-explain-scheduled", "a correlated sensor joined after the explanation")
				}
			}
			err := s.Store.UpdateAlert(ctx, a)
			if err == nil {
				return job, false, true, nil
			}
			if attempt >= 3 || !isConflict(err) {
				return job, false, false, err
			}
			fresh, gerr := s.Store.GetAlert(ctx, a.ID)
			if gerr != nil {
				return job, false, false, gerr
			}
			*a = fresh
			if a.HasFlag(r.SensorID) {
				return job, false, false, nil
			}
		}
	}

	id := AlertID(r.SessionID, r.RunID, r.EquipmentID, r.SensorID, f.Tick)
	a := store.Alert{
		ID: id, SessionID: r.SessionID, RunID: r.RunID, EquipmentID: r.EquipmentID, ToolType: r.ToolType,
		Kind: f.Kind, WindowID: pf.window, FirstTick: f.Tick, FirstTS: r.TS, Flags: []store.SeriesFlag{sf},
		MaxAbsZ: math.Abs(f.Z), Status: store.StatusOpen, Explain: store.ExplainPending, DueTick: f.Tick + settle,
		CreatedAt: now, UpdatedAt: now, ExpiresAt: exp,
	}
	if run, err := s.Store.GetRun(ctx, r.SessionID, r.RunID); err == nil {
		a.Scenario = run.Scenario
	}
	if f.Kind == detect.KindSensorFault {
		a.Explain = store.ExplainSkipped
	}
	a.AddEvent(now, "detector", "flagged",
		fmt.Sprintf("%s on %s: %s rule at tick %d (z=%.2f) - %s", r.SensorID, r.EquipmentID, f.Rule, f.Tick, f.Z, ruleText(f.Rule)))
	created, err := s.Store.CreateAlert(ctx, a)
	if err != nil {
		return ExplainJob{}, false, false, err
	}
	if created {
		a.Version = 1
		*alerts = append(*alerts, a)
	}
	return ExplainJob{AlertID: id, SessionID: r.SessionID, DueTick: a.DueTick, Delay: s.tickDelay(settle)}, created, false, nil
}

func (s *Service) tickDelay(ticks int) time.Duration {
	return time.Duration(float64(ticks) * s.Cfg.Simulate.TickSeconds * float64(time.Second))
}

// expiry: sandbox sessions (visitors) expire; local/CLI/webhook data doesn't.
func (s *Service) expiry(session string, rs []telemetry.Reading) int64 {
	if !IsSandbox(session) && !s.ExpireAll {
		return 0
	}
	return s.now().Add(s.Cfg.Simulate.TTL()).Unix()
}

// IsSandbox reports whether a session is a public-demo visitor sandbox
// (dashboard sign-ins get "v" + 16 hex characters).
func IsSandbox(session string) bool {
	if len(session) != 17 || session[0] != 'v' {
		return false
	}
	for _, c := range session[1:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func ruleText(rule string) string {
	switch rule {
	case detect.RuleSpike:
		return "single reading far outside this tool's normal"
	case detect.RuleSustained:
		return "consecutive readings beyond the threshold"
	case detect.RuleDrift:
		return "baseline has drifted away from the warm-up reference"
	case detect.RuleStuck:
		return "sensor reports the same value repeatedly"
	case detect.RuleDropout:
		return "sensor stopped reporting"
	}
	return rule
}

func isConflict(err error) bool { return errors.Is(err, store.ErrConflict) }
