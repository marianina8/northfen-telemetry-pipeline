package detect_test

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/detect"
)

// params: warm-up of 4 readings alternating base±1 gives mean=base, sigma=1
// exactly, so z-scores in these tests are exact.
func params() config.Detector {
	return config.Detector{
		Warmup: 4, EWMAAlpha: 0.1, SustainedZ: 3, SustainedCount: 3, SpikeZ: 5, DriftSigma: 4,
		ClearAfter: 5, StuckCount: 6, StuckToleranceSigma: 0.01, DropoutCount: 3, WindowSize: 5, MinSigma: 0.001,
	}
}

func f(v float64) *float64 { return &v }

type step struct {
	v    *float64
	flag string // expected rule of a new flag on this reading ("" = none)
}

func run(t *testing.T, p config.Detector, s *detect.State, start int, steps []step) {
	t.Helper()
	for i, st := range steps {
		res := detect.Step(p, s, start+i, st.v)
		got := ""
		if res.Flag != nil {
			got = res.Flag.Rule
		}
		if got != st.flag {
			t.Fatalf("tick %d (value %v): flag %q, want %q (rules %v)", start+i, deref(st.v), got, st.flag, res.Rules)
		}
	}
}

func deref(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func warm(t *testing.T, p config.Detector, s *detect.State) {
	run(t, p, s, 0, []step{{v: f(101)}, {v: f(99)}, {v: f(101)}, {v: f(99)}})
	if !s.Ready || s.Reference != 100 || s.Sigma != 1 {
		t.Fatalf("warm-up: ready=%v ref=%v sigma=%v, want true 100 1", s.Ready, s.Reference, s.Sigma)
	}
}

func TestWarmupNeverScores(t *testing.T) {
	p := params()
	var s detect.State
	for i := 0; i < p.Warmup; i++ {
		res := detect.Step(p, &s, i, f(1e6*float64(i))) // wild values during warm-up
		if res.Z != nil || res.Flag != nil {
			t.Fatalf("tick %d: warm-up reading was scored", i)
		}
	}
}

// The sustained rule's cutoff is inclusive: exactly sustained_z counts.
func TestSustainedBoundaryInclusive(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	run(t, p, &s, 4, []step{{v: f(103)}, {v: f(103)}, {v: f(103), flag: detect.RuleSustained}})
}

func TestSustainedNeedsConsecutiveReadings(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	// two at the threshold, one just under (breaks the streak), then 2.999 x3
	run(t, p, &s, 4, []step{{v: f(103)}, {v: f(103)}, {v: f(100)}, {v: f(102.999)}, {v: f(102.999)}, {v: f(102.999)}})
	if s.InEpisode {
		t.Fatal("no rule should have fired")
	}
}

func TestSustainedNeedsSameDirection(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	run(t, p, &s, 4, []step{{v: f(103.5)}, {v: f(96.5)}, {v: f(103.5)}, {v: f(96.5)}})
}

func TestSpikeFlagsOnOneReading(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	run(t, p, &s, 4, []step{{v: f(100)}, {v: f(105), flag: detect.RuleSpike}})
	// the episode is open: the next excursions don't re-flag
	run(t, p, &s, 6, []step{{v: f(106)}, {v: f(107)}})
}

func TestSpikeJustUnderDoesNotFlagAlone(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	run(t, p, &s, 4, []step{{v: f(104.99)}, {v: f(100)}})
}

// Excursions must not drag the baseline along (only in-control readings
// update the EWMA).
func TestExcursionDoesNotMoveBaseline(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	detect.Step(p, &s, 4, f(110))
	if s.Baseline != 100 {
		t.Fatalf("baseline moved to %v on an excursion", s.Baseline)
	}
	detect.Step(p, &s, 5, f(101))
	if want := 100 + 0.1*(101-100); math.Abs(s.Baseline-want) > 1e-12 {
		t.Fatalf("baseline %v, want %v", s.Baseline, want)
	}
}

// A creep too slow for the z rules is caught by the baseline itself moving.
func TestSlowDriftCaughtByDriftRule(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	var flag *detect.Flag
	for i := 0; i < 400 && flag == nil; i++ {
		v := 100 + 0.05*float64(i) + []float64{0.5, -0.5}[i%2]
		res := detect.Step(p, &s, 4+i, &v)
		if res.Z != nil && math.Abs(*res.Z) >= p.SustainedZ {
			t.Fatalf("tick %d: z=%.2f - the creep should stay under the z threshold", 4+i, *res.Z)
		}
		flag = res.Flag
	}
	if flag == nil || flag.Rule != detect.RuleDrift || flag.Kind != detect.KindAnomaly {
		t.Fatalf("got %+v, want a drift anomaly", flag)
	}
}

func TestStuckSensorIsAFault(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	steps := []step{}
	for i := 0; i < p.StuckCount-1; i++ {
		steps = append(steps, step{v: f(100.2)})
	}
	steps = append(steps, step{v: f(100.2), flag: detect.RuleStuck})
	run(t, p, &s, 4, steps)
	if s.Episode != detect.KindSensorFault {
		t.Fatalf("episode kind %q, want sensor_fault", s.Episode)
	}
}

func TestDropoutIsAFaultAndClears(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	run(t, p, &s, 4, []step{{v: nil}, {v: nil}, {v: nil, flag: detect.RuleDropout}, {v: nil}})
	// back online: after clear_after quiet readings the episode closes
	for i := 0; i < p.ClearAfter; i++ {
		res := detect.Step(p, &s, 8+i, f(100+[]float64{0.5, -0.5}[i%2]))
		if res.Flag != nil {
			t.Fatalf("unexpected flag %+v", res.Flag)
		}
		if i == p.ClearAfter-1 && !res.Cleared {
			t.Fatal("episode should have cleared")
		}
	}
	if s.InEpisode {
		t.Fatal("episode still open")
	}
	// ... and a new problem flags again
	run(t, p, &s, 8+p.ClearAfter, []step{{v: f(110), flag: detect.RuleSpike}})
}

// Missing readings are counted, never treated as zero.
func TestMissingIsNotZero(t *testing.T) {
	p := params()
	var s detect.State
	warm(t, p, &s)
	res := detect.Step(p, &s, 4, nil)
	if res.Z != nil || res.Flag != nil {
		t.Fatal("a single missing reading must not be scored")
	}
}

func TestSensorFaultRulesOutrankAnomalies(t *testing.T) {
	if detect.KindOf(detect.RuleStuck) != detect.KindSensorFault || detect.KindOf(detect.RuleDropout) != detect.KindSensorFault {
		t.Fatal("stuck/dropout must be sensor faults")
	}
	for _, r := range []string{detect.RuleSpike, detect.RuleSustained, detect.RuleDrift} {
		if detect.KindOf(r) != detect.KindAnomaly {
			t.Fatalf("%s must be an anomaly", r)
		}
	}
}

func TestWindowsCoverEveryReading(t *testing.T) {
	p := params()
	var s detect.State
	var ws []detect.Window
	for i := 0; i < 23; i++ {
		var v *float64
		if i != 7 {
			v = f(100 + []float64{1, -1}[i%2])
		}
		if res := detect.Step(p, &s, i, v); res.Window != nil {
			ws = append(ws, *res.Window)
		}
	}
	if len(ws) != 4 {
		t.Fatalf("%d windows, want 4 (23 readings / window 5)", len(ws))
	}
	if ws[0].StartTick != 0 || ws[0].EndTick != 4 || ws[0].Min != 99 || ws[0].Max != 101 {
		t.Fatalf("first window %+v", ws[0])
	}
	if ws[1].Missing != 1 || ws[1].N != 4 || ws[1].Count != 5 {
		t.Fatalf("second window counts %+v", ws[1])
	}
	if !ws[3].Warm || ws[3].Mean() == 0 {
		t.Fatalf("last window %+v", ws[3])
	}
}

// The Lambda consumer is stateless: it loads State from DynamoDB (JSON),
// steps a batch, and saves it. Scoring a stream in arbitrary batches with a
// JSON round trip between every batch must give exactly the same result as
// scoring it in one go.
func TestStateSurvivesJSONRoundTrips(t *testing.T) {
	p := params()
	values := make([]*float64, 120)
	for i := range values {
		switch {
		case i >= 50 && i < 54:
			values[i] = nil
		default:
			v := 100 + math.Sin(float64(i)*1.7)*0.9
			if i > 70 {
				v += 0.08 * float64(i-70)
			}
			values[i] = &v
		}
	}
	var one detect.State
	var want []detect.Result
	for i, v := range values {
		want = append(want, detect.Step(p, &one, i, v))
	}
	for _, size := range []int{1, 3, 7, 50} {
		var saved []byte
		var got []detect.Result
		for i := 0; i < len(values); i += size {
			var s detect.State
			if saved != nil {
				if err := json.Unmarshal(saved, &s); err != nil {
					t.Fatal(err)
				}
			}
			for j := i; j < min(i+size, len(values)); j++ {
				got = append(got, detect.Step(p, &s, j, values[j]))
			}
			b, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			saved = b
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("batch size %d: results differ from one continuous run", size)
		}
	}
}

func TestPerSensorTypeOverrides(t *testing.T) {
	c := config.Default()
	pc := c.Detector.For("particle_count")
	if pc.SpikeZ != 6 || pc.MinSigma != 1 {
		t.Fatalf("particle_count override not applied: %+v", pc)
	}
	tmp := c.Detector.For("temperature")
	if tmp.SpikeZ != c.Detector.SpikeZ || tmp.SustainedZ != 3 {
		t.Fatalf("temperature should inherit spike_z/sustained_z: %+v", tmp)
	}
	if c.Detector.For("unknown_type").SpikeZ != c.Detector.SpikeZ {
		t.Fatal("unknown types inherit the defaults")
	}
}
