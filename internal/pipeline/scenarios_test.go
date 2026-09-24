package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	northfen "github.com/marianina8/northfen-telemetry-pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// ---- expected.json -----------------------------------------------------------

type expectedAlert struct {
	Kind        string   `json:"kind"`
	EquipmentID string   `json:"equipment_id"`
	Sensors     []string `json:"sensors"`
	Rules       []string `json:"rules"`
}

type expectedScenario struct {
	ShouldFlag       bool            `json:"should_flag"`
	Alerts           []expectedAlert `json:"alerts"`
	IntendedSeverity []string        `json:"intended_severity"`
	MockAction       string          `json:"mock_action"`
	MustMention      bool            `json:"must_mention_correlation"`
	Why              string          `json:"why"`
}

func loadExpected(t *testing.T) map[string]expectedScenario {
	t.Helper()
	b, err := fs.ReadFile(northfen.Demo, "demo/expected.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Scenarios map[string]expectedScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Scenarios
}

// ---- fixtures -----------------------------------------------------------------

type recSink struct {
	mu      sync.Mutex
	actions []string
}

func (r *recSink) Do(_ context.Context, a *store.Alert, action string, sandbox bool) (store.ActionRecord, error) {
	r.mu.Lock()
	r.actions = append(r.actions, a.ID+":"+action)
	r.mu.Unlock()
	return store.ActionRecord{Action: action, At: time.Now().UTC(), Detail: action, Simulated: sandbox}, nil
}

var t0 = time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC)

func newSvc(t *testing.T, model explain.Model) (*pipeline.Service, *store.Mem, *recSink) {
	t.Helper()
	st := store.NewMem()
	st.Now = func() time.Time { return t0 }
	sink := &recSink{}
	if model == nil {
		model = &explain.Mock{Now: func() time.Time { return t0 }}
	}
	svc := &pipeline.Service{Cfg: config.Default(), Cat: sim.MustCatalog(), Store: st, Model: model, Sink: sink,
		Now: func() time.Time { return t0 }}
	if err := svc.SeedHistory(context.Background()); err != nil {
		t.Fatal(err)
	}
	return svc, st, sink
}

func sorted(xs []string) []string {
	out := append([]string{}, xs...)
	sort.Strings(out)
	return out
}

func uniqSorted(xs []string) []string {
	m := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !m[x] {
			m[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

// ---- every scenario's detection outcome is exactly what expected.json says ----

func TestScenariosMatchExpected(t *testing.T) {
	exp := loadExpected(t)
	cat := sim.MustCatalog()
	if len(exp) != len(cat.Scenarios) {
		t.Fatalf("expected.json has %d scenarios, demo/scenarios has %d", len(exp), len(cat.Scenarios))
	}
	for _, sc := range cat.Scenarios {
		t.Run(sc.File, func(t *testing.T) {
			want, ok := exp[sc.File]
			if !ok {
				t.Fatalf("%s missing from expected.json", sc.File)
			}
			svc, _, _ := newSvc(t, nil)
			_, as, err := svc.RunBatch(context.Background(), pipeline.RunRequest{Scenario: sc.Name, RunID: "R1", Source: "test"}, 100)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(as) > 0; got != want.ShouldFlag {
				t.Fatalf("flagged=%v, want %v (%s)", got, want.ShouldFlag, want.Why)
			}
			if len(as) != len(want.Alerts) {
				t.Fatalf("%d alerts, want %d", len(as), len(want.Alerts))
			}
			for i, a := range as {
				w := want.Alerts[i]
				s := pipeline.Summarize(a)
				if a.Kind != w.Kind || a.EquipmentID != w.EquipmentID ||
					!reflect.DeepEqual(sorted(s.Sensors), sorted(w.Sensors)) ||
					!reflect.DeepEqual(uniqSorted(s.Rules), sorted(w.Rules)) {
					t.Fatalf("alert %d = %s %s %v %v, want %s %s %v %v", i, a.Kind, a.EquipmentID, s.Sensors, s.Rules,
						w.Kind, w.EquipmentID, w.Sensors, w.Rules)
				}
				if a.Decision == nil || a.Decision.Action != want.MockAction {
					t.Fatalf("action %+v, want %s", a.Decision, want.MockAction)
				}
				if a.Kind == "sensor_fault" {
					if a.Explain != store.ExplainSkipped || a.Explanation != nil || a.Input != nil {
						t.Fatal("sensor faults must never reach the model")
					}
				} else {
					if a.Explain != store.ExplainDone || a.Explanation == nil {
						t.Fatalf("anomaly not explained: %s %s", a.Explain, a.ExplainErr)
					}
					if a.Explains != 1 {
						t.Fatalf("explained %d times, want exactly 1", a.Explains)
					}
				}
				if want.MustMention && !strings.Contains(a.Explanation.Explanation, "together") {
					t.Fatalf("explanation should reference the correlation: %q", a.Explanation.Explanation)
				}
			}
		})
	}
}

// Every scored window is stored (the audit trail covers quiet windows too).
func TestEveryWindowIsStored(t *testing.T) {
	svc, st, _ := newSvc(t, nil)
	run, _, err := svc.RunBatch(context.Background(), pipeline.RunRequest{Scenario: "01", RunID: "R1"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := st.ListWindows(context.Background(), telemetry.LocalSession, run.ID)
	// FARM-LGT: 4 metrics x (120 ticks / 10 per window)
	if len(ws) != 48 {
		t.Fatalf("%d windows stored, want 48", len(ws))
	}
	for _, w := range ws {
		if w.Status != "normal" && w.Status != "warmup" {
			t.Fatalf("baseline window %s has status %s", w.ID, w.Status)
		}
	}
}

// Kinesis delivers in batches of any size, and redelivers after failures.
// Neither may change the outcome.
func TestBatchSizeAndReplayDoNotChangeOutcome(t *testing.T) {
	ctx := context.Background()
	type outcome struct {
		Alerts  []string
		Windows int
	}
	get := func(svc *pipeline.Service, st *store.Mem, runID string) outcome {
		as, _ := svc.RunAlerts(ctx, telemetry.LocalSession, runID)
		ws, _ := st.ListWindows(ctx, telemetry.LocalSession, runID)
		var ids []string
		for _, a := range as {
			ids = append(ids, fmt.Sprintf("%s/%s/%v", a.ID, a.Decision.Action, len(a.Flags)))
		}
		return outcome{ids, len(ws)}
	}
	ref, refSt, _ := newSvc(t, nil)
	if _, _, err := ref.RunBatch(ctx, pipeline.RunRequest{Scenario: "10", RunID: "R1"}, 100000); err != nil {
		t.Fatal(err)
	}
	want := get(ref, refSt, "R1")

	for _, size := range []int{1, 7, 100} {
		svc, st, sink := newSvc(t, nil)
		run, rs, err := svc.StartRun(ctx, pipeline.RunRequest{Scenario: "10", RunID: "R1"})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < len(rs); i += size {
			b := rs[i:min(i+size, len(rs))]
			if _, err := svc.Process(ctx, b); err != nil {
				t.Fatal(err)
			}
			// redeliver every batch (at-least-once delivery)
			res, err := svc.Process(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			if res.Readings != 0 || res.Flags != 0 {
				t.Fatalf("replayed batch was processed again: %+v", res)
			}
		}
		if _, err := svc.Sweep(ctx, run.SessionID, false); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Sweep(ctx, run.SessionID, false); err != nil { // idempotent
			t.Fatal(err)
		}
		if got := get(svc, st, "R1"); !reflect.DeepEqual(got, want) {
			t.Fatalf("batch size %d: %+v, want %+v", size, got, want)
		}
		if len(sink.actions) != 1 {
			t.Fatalf("batch size %d: actions %v, want exactly one page", size, sink.actions)
		}
	}
}

// The consumer never calls the model.
type countingModel struct {
	explain.Mock
	mu sync.Mutex
	n  int
}

func (c *countingModel) Explain(ctx context.Context, in explain.Input) (explain.Explanation, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.Mock.Explain(ctx, in)
}

func TestConsumerNeverCallsTheModel(t *testing.T) {
	m := &countingModel{}
	svc, _, _ := newSvc(t, m)
	_, rs, _ := svc.StartRun(context.Background(), pipeline.RunRequest{Scenario: "05", RunID: "R1"})
	res, err := svc.Process(context.Background(), rs)
	if err != nil {
		t.Fatal(err)
	}
	if res.Flags == 0 || len(res.NewAlerts) != 1 {
		t.Fatalf("expected one new alert: %+v", res)
	}
	if m.n != 0 {
		t.Fatalf("the stream consumer called the model %d times", m.n)
	}
	if _, err := svc.Sweep(context.Background(), telemetry.LocalSession, false); err != nil {
		t.Fatal(err)
	}
	if m.n != 1 {
		t.Fatalf("model called %d times, want exactly 1 (one bounded call per flagged alert)", m.n)
	}
}

// Normal scenarios make zero model calls.
func TestQuietScenariosCostNothing(t *testing.T) {
	for _, name := range []string{"01", "02", "03", "06"} {
		m := &countingModel{}
		svc, _, _ := newSvc(t, m)
		if _, _, err := svc.RunBatch(context.Background(), pipeline.RunRequest{Scenario: name, RunID: "R1"}, 50); err != nil {
			t.Fatal(err)
		}
		if m.n != 0 {
			t.Fatalf("scenario %s made %d model calls", name, m.n)
		}
	}
}

// When the model fails or returns junk, the alert goes to a human.
type failModel struct{ raw string }

func (failModel) Name() string { return "fail" }
func (f failModel) Explain(context.Context, explain.Input) (explain.Explanation, error) {
	return explain.Explanation{Raw: f.raw}, fmt.Errorf("%w: boom", explain.ErrBadOutput)
}

func TestExplainFailureGoesToAHuman(t *testing.T) {
	svc, _, _ := newSvc(t, failModel{raw: "I think it's fine"})
	_, as, err := svc.RunBatch(context.Background(), pipeline.RunRequest{Scenario: "04", RunID: "R1"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	a := as[0]
	if a.Explain != store.ExplainFailed || a.Decision.Action != config.ActionPageOnCall || a.Decision.Rule != "explain-failed-goes-to-human" {
		t.Fatalf("got explain=%s decision=%+v", a.Explain, a.Decision)
	}
	if a.Explanation == nil || a.Explanation.Raw != "I think it's fine" {
		t.Fatal("the raw model text should be kept for the audit trail")
	}
}

// A sensor that crosses its threshold after the alert was explained joins
// the alert and triggers one re-explanation (bounded by config).
func TestLateCorrelatedSensorReExplains(t *testing.T) {
	ctx := context.Background()
	svc, _, sink := newSvc(t, nil)
	_, rs, _ := svc.StartRun(ctx, pipeline.RunRequest{Scenario: "10", RunID: "R1"})
	// Stream tick by tick and resolve due alerts as we go (like the live runner).
	byTick := map[int][]telemetry.Reading{}
	maxTick := 0
	for _, r := range rs {
		byTick[r.Tick] = append(byTick[r.Tick], r)
		maxTick = max(maxTick, r.Tick)
	}
	for tk := 0; tk <= maxTick; tk++ {
		if _, err := svc.Process(ctx, byTick[tk]); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Sweep(ctx, telemetry.LocalSession, true); err != nil {
			t.Fatal(err)
		}
	}
	as, _ := svc.RunAlerts(ctx, telemetry.LocalSession, "R1")
	if len(as) != 1 {
		t.Fatalf("%d alerts, want 1", len(as))
	}
	a := as[0]
	if len(a.Flags) != 3 || a.Explains < 1 || a.Explains > svc.Cfg.Alerts.MaxExplainsPerAlert {
		t.Fatalf("flags=%d explains=%d", len(a.Flags), a.Explains)
	}
	if a.ExplainedFlags != 3 && a.Explains < svc.Cfg.Alerts.MaxExplainsPerAlert {
		t.Fatalf("latest explanation covers %d of 3 sensors", a.ExplainedFlags)
	}
	pages := 0
	for _, x := range sink.actions {
		if strings.HasSuffix(x, ":page_oncall") {
			pages++
		}
	}
	if pages != 1 {
		t.Fatalf("paged %d times, want 1 (actions only escalate, never repeat): %v", pages, sink.actions)
	}
}

// Settle time: while streaming, an anomaly isn't explained until the tool
// has streamed settle_ticks past the flag (so correlated sensors are in).
func TestExplainWaitsForSettleTicks(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newSvc(t, nil)
	svc.Now = func() time.Time { return t0 } // run never "ends" during this test
	run, rs, _ := svc.StartRun(ctx, pipeline.RunRequest{Scenario: "05", RunID: "R1", Start: t0})
	_ = run
	var upTo []telemetry.Reading
	for _, r := range rs {
		if r.Tick <= 70 {
			upTo = append(upTo, r)
		}
	}
	if _, err := svc.Process(ctx, upTo); err != nil {
		t.Fatal(err)
	}
	done, _ := svc.Sweep(ctx, telemetry.LocalSession, true)
	if len(done) != 0 {
		t.Fatal("explained before the settle time")
	}
	var more []telemetry.Reading
	for _, r := range rs {
		if r.Tick > 70 && r.Tick <= 70+svc.Cfg.Alerts.SettleTicks {
			more = append(more, r)
		}
	}
	if _, err := svc.Process(ctx, more); err != nil {
		t.Fatal(err)
	}
	done, _ = svc.Sweep(ctx, telemetry.LocalSession, true)
	if len(done) != 1 {
		t.Fatalf("resolved %d alerts after settle time, want 1", len(done))
	}
}
