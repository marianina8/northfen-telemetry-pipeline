package pipeline_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

func TestAckFlow(t *testing.T) {
	ctx := context.Background()
	svc, _, sink := newSvc(t, nil)
	_, as, _ := svc.RunBatch(ctx, pipeline.RunRequest{Scenario: "04", RunID: "R1"}, 100) // log_only
	id := as[0].ID

	if _, err := svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: "dismiss", Actor: "engineer:sam"}); err == nil {
		t.Fatal("dismiss without a note must be refused")
	}
	a, err := svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: "ack", Actor: "engineer:sam", Note: "looking"})
	if err != nil || a.Status != store.StatusAcknowledged {
		t.Fatalf("%v %s", err, a.Status)
	}
	// escalate: a log_only alert gets a page (once)
	a, err = svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: "escalate", Actor: "engineer:sam", Note: "foreline again?"})
	if err != nil || a.Status != store.StatusEscalated || !a.Took("page_oncall") {
		t.Fatalf("%v %+v", err, a.Actions)
	}
	n := len(sink.actions)
	if _, err := svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: "escalate", Note: "again"}); err != nil {
		t.Fatal(err)
	}
	if len(sink.actions) != n {
		t.Fatal("escalating twice must not page twice")
	}
	a, err = svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: "dismiss", Actor: "engineer:sam", Note: "PM scheduled"})
	if err != nil || a.Status != store.StatusDismissed {
		t.Fatalf("%v %s", err, a.Status)
	}
	if _, err := svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: "ack"}); err == nil {
		t.Fatal("a dismissed alert is closed")
	}
	// the audit trail has every step, attributed
	var types []string
	for _, e := range a.Events {
		types = append(types, e.Actor+"/"+e.Type)
	}
	joined := strings.Join(types, " ")
	for _, want := range []string{"detector/flagged", "dispatch/dispatched", "engineer:sam/acknowledged", "engineer:sam/escalated", "engineer:sam/dismissed"} {
		if !strings.Contains(joined, want) {
			t.Errorf("audit trail missing %s: %s", want, joined)
		}
	}
}

func TestAckRespectsSandbox(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newSvc(t, nil)
	_, as, _ := svc.RunBatch(ctx, pipeline.RunRequest{SessionID: "v0123456789abcdef", Scenario: "05", RunID: "R1"}, 100)
	_, err := svc.Ack(ctx, pipeline.AckRequest{AlertID: as[0].ID, Verb: "ack", SessionID: "vfedcba9876543210"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("another visitor's alert must look like it doesn't exist: %v", err)
	}
}

func TestSandboxLimitsAndTTL(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newSvc(t, nil)
	svc.Cfg.Simulate.MaxRunsPerSession = 2
	sess := "v0123456789abcdef"
	for i := 0; i < 2; i++ {
		run, _, err := svc.StartRun(ctx, pipeline.RunRequest{SessionID: sess, Scenario: "01"})
		if err != nil || run.ExpiresAt == 0 {
			t.Fatalf("run %d: %v exp=%d", i, err, run.ExpiresAt)
		}
	}
	if _, _, err := svc.StartRun(ctx, pipeline.RunRequest{SessionID: sess, Scenario: "01"}); !errors.Is(err, pipeline.ErrRunLimit) {
		t.Fatalf("third run: %v", err)
	}
	// local/CLI sessions are unlimited and never expire
	run, _, err := svc.StartRun(ctx, pipeline.RunRequest{Scenario: "01"})
	if err != nil || run.ExpiresAt != 0 || run.SessionID != telemetry.LocalSession {
		t.Fatalf("%v %+v", err, run)
	}
}

func TestSandboxExplainBudget(t *testing.T) {
	ctx := context.Background()
	m := &countingModel{}
	svc, _, _ := newSvc(t, m)
	svc.Cfg.Alerts.MaxExplainsPerSession = 1
	sess := "v0123456789abcdef"
	_, as1, _ := svc.RunBatch(ctx, pipeline.RunRequest{SessionID: sess, Scenario: "05", RunID: "R1"}, 100)
	_, as2, _ := svc.RunBatch(ctx, pipeline.RunRequest{SessionID: sess, Scenario: "05", RunID: "R2"}, 100)
	if m.n != 1 || as1[0].Explain != store.ExplainDone || as2[0].Explain != store.ExplainFailed {
		t.Fatalf("calls=%d %s %s", m.n, as1[0].Explain, as2[0].Explain)
	}
	if as2[0].Decision.Action != "page_oncall" {
		t.Fatal("over budget -> no explanation -> a human looks at it")
	}
}

func TestExplainOnlyForFlaggedWindows(t *testing.T) {
	ctx := context.Background()
	svc, st, _ := newSvc(t, nil)
	run, as, _ := svc.RunBatch(ctx, pipeline.RunRequest{Scenario: "05", RunID: "R1"}, 100)
	ws, _ := st.ListWindows(ctx, run.SessionID, run.ID)
	var quiet, flagged string
	for _, w := range ws {
		if w.AlertID == "" && quiet == "" {
			quiet = w.ID
		}
		if w.AlertID != "" {
			flagged = w.ID
		}
	}
	if _, _, err := svc.AlertForWindow(ctx, quiet); err == nil || !strings.Contains(err.Error(), "not flagged") {
		t.Fatalf("unflagged window: %v", err)
	}
	a, w, err := svc.AlertForWindow(ctx, flagged)
	if err != nil || a.ID != as[0].ID || w.Status != "flagged" {
		t.Fatalf("%v %s %s", err, a.ID, w.Status)
	}
	if as[0].WindowID != flagged {
		t.Fatalf("alert window %s, flagged window %s", as[0].WindowID, flagged)
	}
}

// The explain input carries the other sensors on the tool and its history.
func TestExplainInputHasContext(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newSvc(t, nil)
	_, as, _ := svc.RunBatch(ctx, pipeline.RunRequest{Scenario: "05", RunID: "R1"}, 100)
	in := as[0].Input
	if in == nil || len(in.Sensors) != 4 || in.FlaggedCount() != 1 || len(in.History) != 5 {
		t.Fatalf("%+v", in)
	}
	if !strings.Contains(in.History[0].Summary, "asset version") {
		t.Fatalf("history not newest-first: %+v", in.History[0])
	}
	for _, s := range in.Sensors {
		if s.Sigma <= 0 || len(s.Recent) == 0 {
			t.Fatalf("sensor summary missing stats: %+v", s)
		}
	}
}

func TestParseVerb(t *testing.T) {
	for in, want := range map[string]string{"ack": "acknowledge", "Dismiss": "dismiss", "escalated": "escalate"} {
		if got, err := pipeline.ParseVerb(in); err != nil || got != want {
			t.Errorf("%s -> %s %v", in, got, err)
		}
	}
	if _, err := pipeline.ParseVerb("delete"); err == nil {
		t.Error("unknown verb accepted")
	}
}
