// Package detect is the deterministic anomaly detector. It decides whether a
// reading is anomalous using plain windowed statistics — no model, no
// network, no randomness — so every outcome can be unit-tested and replayed.
//
// Per series (one sensor on one tool) it keeps:
//
//   - a warm-up phase: the first `warmup` readings set a reference mean and a
//     sigma (population std, floored at min_sigma for the sensor type);
//   - an EWMA baseline mean that follows in-control readings only
//     (|z| < sustained_z), so an excursion can't drag its own baseline along;
//   - z = (value - baseline) / sigma for every reading after warm-up.
//
// Rules (any one opens an episode; an open episode doesn't re-flag):
//
//	spike      |z| >= spike_z on a single reading                    anomaly
//	sustained  |z| >= sustained_z for sustained_count readings in a
//	           row, same direction                                   anomaly
//	drift      |baseline - reference| >= drift_sigma * sigma         anomaly
//	           (catches drift too slow for the z rules: the EWMA
//	           baseline follows it, so the baseline itself moves)
//	stuck      range of the last stuck_count readings <=
//	           stuck_tolerance_sigma * sigma                          sensor_fault
//	dropout    dropout_count missing readings in a row               sensor_fault
//
// An episode closes after clear_after readings with no rule active.
// Sigma is learned once in warm-up and then held, so a slow drift can't
// inflate its own noise estimate and hide itself.
package detect

import (
	"math"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
)

// Rule names.
const (
	RuleSpike     = "spike"
	RuleSustained = "sustained"
	RuleDrift     = "drift"
	RuleStuck     = "stuck"
	RuleDropout   = "dropout"
)

// Alert kinds.
const (
	KindAnomaly     = "anomaly"      // process/equipment behaviour -> explain with Bedrock
	KindSensorFault = "sensor_fault" // the sensor itself looks broken -> no model call
)

// rulePriority orders rules when several fire at once (the first becomes the
// episode's rule). Sensor faults come first: a stuck gauge's z-score is
// meaningless.
var rulePriority = []string{RuleDropout, RuleStuck, RuleSpike, RuleSustained, RuleDrift}

// KindOf returns the alert kind a rule opens.
func KindOf(rule string) string {
	if rule == RuleStuck || rule == RuleDropout {
		return KindSensorFault
	}
	return KindAnomaly
}

// Window accumulates one scored window (window_size readings).
type Window struct {
	StartTick int      `json:"start_tick"`
	EndTick   int      `json:"end_tick"`
	Count     int      `json:"count"`
	N         int      `json:"n"`
	Missing   int      `json:"missing"`
	Sum       float64  `json:"sum"`
	Min       float64  `json:"min"`
	Max       float64  `json:"max"`
	MaxAbsZ   float64  `json:"max_abs_z"`
	LastZ     *float64 `json:"last_z,omitempty"`
	Rules     []string `json:"rules,omitempty"`
	Warm      bool     `json:"warm"`
}

// State is everything the detector remembers about one series. It is small
// and JSON-serializable because the Lambda consumer is stateless: it loads
// the state from DynamoDB, steps it, and writes it back.
type State struct {
	RunID      string    `json:"run_id"`
	N          int       `json:"n"` // valid readings seen
	Warm       []float64 `json:"warm,omitempty"`
	Ready      bool      `json:"ready"`
	Reference  float64   `json:"reference"`
	Sigma      float64   `json:"sigma"`
	Baseline   float64   `json:"baseline"`
	Streak     int       `json:"streak"`
	StreakSign int       `json:"streak_sign"`
	Missing    int       `json:"missing"`
	Recent     []float64 `json:"recent,omitempty"`
	InEpisode  bool      `json:"in_episode"`
	Episode    string    `json:"episode,omitempty"` // kind of the open episode
	EpisodeID  string    `json:"episode_id,omitempty"`
	Quiet      int       `json:"quiet"`
	LastTick   int       `json:"last_tick"`
	Started    bool      `json:"started"`
	Win        Window    `json:"win"`
}

// Flag is emitted when a reading opens a new episode.
type Flag struct {
	Kind string  `json:"kind"`
	Rule string  `json:"rule"`
	Tick int     `json:"tick"`
	Z    float64 `json:"z"`
}

// Result describes one step.
type Result struct {
	Z        *float64 // nil during warm-up or for a missing reading
	Rules    []string // rules active on this reading
	Flag     *Flag    // non-nil when this reading opened an episode
	Cleared  bool     // the open episode closed on this reading
	Window   *Window  // non-nil when this reading completed a window
	Baseline float64
	Sigma    float64
}

// Step scores one reading. value == nil is a missing reading. Readings must
// arrive in tick order; callers skip ticks <= LastTick (replays).
func Step(p config.Detector, s *State, tick int, value *float64) Result {
	var res Result
	var active []string
	s.LastTick, s.Started = tick, true
	if s.Win.Count == 0 {
		s.Win = Window{StartTick: tick}
	}

	if value == nil {
		s.Missing++
		s.Streak, s.StreakSign = 0, 0
		s.Win.Missing++
		if s.Missing >= p.DropoutCount {
			active = append(active, RuleDropout)
		}
	} else {
		v := *value
		s.Missing = 0
		s.Recent = append(s.Recent, v)
		if len(s.Recent) > p.StuckCount {
			s.Recent = s.Recent[len(s.Recent)-p.StuckCount:]
		}
		s.N++
		// Min/Max start from the first valid reading (never +/-Inf: the state
		// is stored as JSON between Lambda invocations).
		if s.Win.N == 0 {
			s.Win.Min, s.Win.Max = v, v
		}
		s.Win.N++
		s.Win.Sum += v
		s.Win.Min = math.Min(s.Win.Min, v)
		s.Win.Max = math.Max(s.Win.Max, v)

		if !s.Ready {
			s.Warm = append(s.Warm, v)
			if len(s.Warm) >= p.Warmup {
				s.Reference, s.Sigma = meanStd(s.Warm)
				s.Sigma = math.Max(s.Sigma, p.MinSigma)
				if s.Sigma <= 0 {
					s.Sigma = 1e-9
				}
				s.Baseline = s.Reference
				s.Ready = true
				s.Warm = nil
			}
		} else {
			z := (v - s.Baseline) / s.Sigma
			res.Z = &z
			az := math.Abs(z)
			if az >= p.SustainedZ {
				sign := 1
				if z < 0 {
					sign = -1
				}
				if sign == s.StreakSign {
					s.Streak++
				} else {
					s.Streak, s.StreakSign = 1, sign
				}
			} else {
				s.Streak, s.StreakSign = 0, 0
				// Only in-control readings move the baseline.
				s.Baseline += p.EWMAAlpha * (v - s.Baseline)
			}
			if az >= p.SpikeZ {
				active = append(active, RuleSpike)
			}
			if s.Streak >= p.SustainedCount {
				active = append(active, RuleSustained)
			}
			if math.Abs(s.Baseline-s.Reference) >= p.DriftSigma*s.Sigma {
				active = append(active, RuleDrift)
			}
			if az > s.Win.MaxAbsZ {
				s.Win.MaxAbsZ = az
			}
			zc := z
			s.Win.LastZ = &zc
		}

		if len(s.Recent) == p.StuckCount {
			sigma := s.Sigma
			if !s.Ready {
				sigma = p.MinSigma
			}
			lo, hi := minMax(s.Recent)
			if hi-lo <= p.StuckToleranceSigma*sigma {
				active = append(active, RuleStuck)
			}
		}
	}

	active = ordered(active)
	res.Rules = active
	for _, r := range active {
		s.Win.Rules = addUnique(s.Win.Rules, r)
	}

	switch {
	case len(active) > 0:
		s.Quiet = 0
		if !s.InEpisode {
			s.InEpisode = true
			s.Episode = KindOf(active[0])
			z := 0.0
			if res.Z != nil {
				z = *res.Z
			}
			res.Flag = &Flag{Kind: s.Episode, Rule: active[0], Tick: tick, Z: z}
		}
	case s.InEpisode:
		s.Quiet++
		if s.Quiet >= p.ClearAfter {
			s.InEpisode, s.Episode, s.EpisodeID, s.Quiet = false, "", "", 0
			res.Cleared = true
		}
	}

	s.Win.Count++
	s.Win.EndTick = tick
	s.Win.Warm = s.Ready
	if s.Win.Count >= p.WindowSize {
		w := s.Win
		res.Window = &w
		s.Win = Window{}
	}
	res.Baseline, res.Sigma = s.Baseline, s.Sigma
	return res
}

// Mean of a completed window's valid readings.
func (w Window) Mean() float64 {
	if w.N == 0 {
		return 0
	}
	return w.Sum / float64(w.N)
}

// meanStd returns the mean and population standard deviation.
func meanStd(xs []float64) (float64, float64) {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	m := sum / float64(len(xs))
	var ss float64
	for _, x := range xs {
		d := x - m
		ss += d * d
	}
	return m, math.Sqrt(ss / float64(len(xs)))
}

func minMax(xs []float64) (float64, float64) {
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, x := range xs {
		lo, hi = math.Min(lo, x), math.Max(hi, x)
	}
	return lo, hi
}

func ordered(active []string) []string {
	if len(active) < 2 {
		return active
	}
	out := make([]string, 0, len(active))
	for _, r := range rulePriority {
		for _, a := range active {
			if a == r {
				out = append(out, r)
			}
		}
	}
	return out
}

func addUnique(xs []string, x string) []string {
	for _, v := range xs {
		if v == x {
			return xs
		}
	}
	return append(xs, x)
}
