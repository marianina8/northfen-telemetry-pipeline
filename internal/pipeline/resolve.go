package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

// NeedsResolve reports whether the explain worker has something to do for
// an alert: a sensor fault not yet dispatched, or an anomaly whose
// explanation is pending / missing a later-joined sensor (within budget).
func (s *Service) NeedsResolve(a store.Alert) bool {
	if a.Status == store.StatusDismissed {
		return false
	}
	if a.Explain == store.ExplainSkipped {
		return a.Decision == nil
	}
	if a.Explains >= s.Cfg.Alerts.MaxExplainsPerAlert {
		return a.Decision == nil
	}
	return a.Explain == store.ExplainPending || len(a.Flags) > a.ExplainedFlags
}

// Resolve runs the explain step (anomalies only) and the dispatch table for
// one alert, then records everything on the alert. Safe to call repeatedly:
// if there's nothing new to do it returns the alert unchanged.
func (s *Service) Resolve(ctx context.Context, alertID, actor string) (store.Alert, error) {
	a, err := s.Store.GetAlert(ctx, alertID)
	if err != nil {
		return a, err
	}
	if !s.NeedsResolve(a) {
		return a, nil
	}
	if actor == "" {
		actor = "explain-worker"
	}

	// ---- explain (the only model call in the system) ----
	var (
		in      *explain.Input
		exp     *explain.Explanation
		expErr  error
		ran     bool
		nFlags  = len(a.Flags)
		skipped = a.Explain == store.ExplainSkipped
	)
	if !skipped && a.Explains < s.Cfg.Alerts.MaxExplainsPerAlert {
		input, err := s.BuildInput(ctx, a)
		if err != nil {
			return a, fmt.Errorf("build explain input: %w", err)
		}
		in = &input
		if _, err := s.Store.Incr(ctx, a.SessionID, "explains", s.sessionExplainCap(a.SessionID), a.ExpiresAt); err != nil {
			expErr = err
			if errors.Is(err, store.ErrLimit) {
				expErr = fmt.Errorf("this demo session used its %d model calls", s.Cfg.Alerts.MaxExplainsPerSession)
			}
		} else {
			ran = true
			cctx, cancel := context.WithTimeout(ctx, s.explainTimeout())
			e, err := s.Model.Explain(cctx, input)
			cancel()
			if err != nil {
				expErr = err
				if e.Raw != "" {
					exp = &e // keep the raw text for the audit trail
				}
			} else {
				exp = &e
			}
		}
	}

	// ---- record + dispatch (retry on concurrent updates) ----
	for attempt := 0; ; attempt++ {
		now := s.now()
		if in != nil {
			a.Input = in
			if ran {
				a.Explains++
			}
			a.ExplainedFlags = nFlags
			if expErr == nil {
				a.Explanation, a.Explain, a.ExplainErr = exp, store.ExplainDone, ""
				a.AddEvent(now, actor, "explained", fmt.Sprintf("%s: severity=%s confidence=%.2f causes=%v",
					exp.Model, exp.Severity, exp.Confidence, exp.LikelyCauses))
			} else {
				a.Explain, a.ExplainErr = store.ExplainFailed, expErr.Error()
				if exp != nil {
					a.Explanation = &explain.Explanation{Raw: exp.Raw, Model: s.Model.Name(), At: now}
				}
				a.AddEvent(now, actor, "explain-failed", expErr.Error())
			}
		}
		prev := ""
		if a.Decision != nil {
			prev = a.Decision.Action
		}
		d := dispatch.Decide(s.Cfg.Dispatch, s.facts(a))
		a.Decision = &store.Decision{Action: d.Action, Rule: d.Rule, Reason: d.Reason, Floor: d.Floor, At: now}
		a.AddEvent(now, "dispatch", "dispatched", fmt.Sprintf("%s (rule %s: %s)", d.Action, d.Rule, d.Reason))

		// Actions only ever escalate: a re-explanation can raise the action
		// but never repeats or downgrades one already taken.
		var took []store.ActionRecord
		if config.ActionRank(d.Action) > config.ActionRank(prev) && !a.Took(d.Action) {
			rec, err := s.Sink.Do(ctx, &a, d.Action, a.ExpiresAt > 0)
			if err != nil {
				a.AddEvent(now, "dispatch", "action-failed", fmt.Sprintf("%s: %v", d.Action, err))
			} else {
				took = append(took, rec)
				a.Actions = append(a.Actions, rec)
				a.AddEvent(now, "dispatch", "action", rec.Detail)
			}
		}
		err := s.Store.UpdateAlert(ctx, &a)
		if err == nil {
			return a, nil
		}
		if attempt >= 3 || !isConflict(err) {
			return a, err
		}
		// Someone (usually the consumer adding a correlated sensor) changed
		// the alert meanwhile: re-read it and re-apply this result.
		fresh, gerr := s.Store.GetAlert(ctx, alertID)
		if gerr != nil {
			return a, gerr
		}
		fresh.Actions = append(fresh.Actions, took...)
		a = fresh
	}
}

func (s *Service) facts(a store.Alert) dispatch.Facts {
	f := dispatch.Facts{Kind: a.Kind, MaxAbsZ: a.MaxAbsZ, ExplainFailed: a.Explain == store.ExplainFailed}
	if e := a.Explanation; e != nil && a.Explain == store.ExplainDone {
		f.Severity, f.Confidence = e.Severity, e.Confidence
	}
	return f
}

func (s *Service) sessionExplainCap(session string) int {
	if IsSandbox(session) {
		return s.Cfg.Alerts.MaxExplainsPerSession
	}
	return 0
}

func (s *Service) explainTimeout() time.Duration {
	if s.ExplainTimeout > 0 {
		return s.ExplainTimeout
	}
	return 25 * time.Second
}

// BuildInput assembles what the model sees: the flagged window's stats, the
// other sensors on the same tool over the same window, and the tool's recent
// history from the state store. Pure statistics computed here in Go.
func (s *Service) BuildInput(ctx context.Context, a store.Alert) (explain.Input, error) {
	eq, ok := s.Cat.Tool(a.EquipmentID)
	if !ok {
		return explain.Input{}, fmt.Errorf("unknown equipment %s", a.EquipmentID)
	}
	tickSec := s.Cfg.Simulate.TickSeconds
	if run, err := s.Store.GetRun(ctx, a.SessionID, a.RunID); err == nil && run.TickSeconds > 0 {
		tickSec = run.TickSeconds
	}
	ctxTicks := s.Cfg.Explain.ContextTicks
	from := a.FirstTS.Add(-time.Duration(float64(ctxTicks) * tickSec * float64(time.Second)))

	keys := make([]string, 0, len(eq.Sensors))
	for _, sn := range eq.Sensors {
		keys = append(keys, a.EquipmentID+"#"+sn.ID)
	}
	states, err := s.Store.GetStates(ctx, a.SessionID, keys)
	if err != nil {
		return explain.Input{}, err
	}
	to := a.FirstTS
	for _, st := range states {
		if st.State.RunID == a.RunID && st.LastTS.After(to) {
			to = st.LastTS
		}
	}

	flagged := map[string]store.SeriesFlag{}
	in := explain.Input{
		Fab:             s.Cfg.Fab,
		Equipment:       explain.Equipment{ID: eq.ID, Name: eq.Name, ToolType: eq.ToolType, Line: eq.Line},
		TickSeconds:     tickSec,
		CauseCategories: s.Cfg.Explain.CauseCategories,
		WindowFromTick:  a.FirstTick,
		WindowToTick:    a.FirstTick,
	}
	for _, f := range a.Flags {
		flagged[f.SensorID] = f
		in.Triggers = append(in.Triggers, explain.Trigger{SensorID: f.SensorID, SensorType: f.SensorType, Rule: f.Rule, Tick: f.Tick, Z: round(f.Z, 2)})
	}
	first := true
	for _, sn := range eq.Sensors {
		rs, err := s.Store.Readings(ctx, a.SessionID, a.EquipmentID, sn.ID, from, to)
		if err != nil {
			return explain.Input{}, err
		}
		st := states[a.EquipmentID+"#"+sn.ID]
		sum := explain.SensorSummary{SensorID: sn.ID, SensorType: sn.Type, Unit: sn.Unit,
			WarmupMean: round(st.State.Reference, sn.Decimals+2), Sigma: round(st.State.Sigma, sn.Decimals+3)}
		if f, ok := flagged[sn.ID]; ok {
			sum.Flagged, sum.Rule = true, f.Rule
		}
		var xs, ts []float64
		for _, r := range rs {
			if r.RunID != a.RunID {
				continue
			}
			if first || r.Tick < in.WindowFromTick {
				in.WindowFromTick = r.Tick
			}
			if r.Tick > in.WindowToTick {
				in.WindowToTick = r.Tick
			}
			first = false
			if r.Value == nil {
				sum.Missing++
				continue
			}
			xs, ts = append(xs, *r.Value), append(ts, float64(r.Tick))
		}
		sigma := st.State.Sigma
		if len(xs) > 0 && sigma > 0 {
			latest := xs[len(xs)-1]
			sum.Latest = &latest
			sum.LatestDevSigma = round((latest-st.State.Reference)/sigma, 2)
			for _, x := range xs {
				sum.MaxDevSigma = math.Max(sum.MaxDevSigma, math.Abs(x-st.State.Reference)/sigma)
			}
			sum.MaxDevSigma = round(sum.MaxDevSigma, 2)
			tail := xs
			if len(tail) > 10 {
				tail = tail[len(tail)-10:]
			}
			sum.ShiftSigma = round((mean(tail)-st.State.Reference)/sigma, 2)
			sum.TrendSigmaPer10Ticks = round(slope(ts, xs)*10/sigma, 2)
		}
		sum.Recent = downsample(xs, 20, sn.Decimals+1)
		in.Sensors = append(in.Sensors, sum)
	}

	hs, err := s.Store.History(ctx, a.EquipmentID, s.Cfg.Explain.HistoryItems)
	if err != nil {
		return explain.Input{}, err
	}
	for _, h := range hs {
		in.History = append(in.History, explain.HistoryItem{Date: h.Date.Format("2006-01-02"), Kind: h.Kind, Summary: h.Summary})
	}
	return in, nil
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

// slope is the least-squares slope of y over x.
func slope(x, y []float64) float64 {
	if len(x) < 2 {
		return 0
	}
	mx, my := mean(x), mean(y)
	var num, den float64
	for i := range x {
		num += (x[i] - mx) * (y[i] - my)
		den += (x[i] - mx) * (x[i] - mx)
	}
	if den == 0 {
		return 0
	}
	return num / den
}

func downsample(xs []float64, n, decimals int) []float64 {
	if len(xs) == 0 {
		return []float64{}
	}
	out := make([]float64, 0, n)
	if len(xs) <= n {
		for _, x := range xs {
			out = append(out, round(x, decimals))
		}
		return out
	}
	step := float64(len(xs)-1) / float64(n-1)
	for i := 0; i < n; i++ {
		out = append(out, round(xs[int(math.Round(float64(i)*step))], decimals))
	}
	return out
}

func round(v float64, decimals int) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	p := math.Pow(10, float64(decimals))
	return math.Round(v*p) / p
}
