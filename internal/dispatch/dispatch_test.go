package dispatch_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

// The full (severity, confidence) -> action table, including the exact
// low-confidence boundary.
func TestDecideTable(t *testing.T) {
	c := config.Default().Dispatch
	cases := []struct {
		name string
		f    dispatch.Facts
		want string
		rule string
	}{
		{"low sev confident", dispatch.Facts{Kind: "anomaly", Severity: "low", Confidence: 0.9, MaxAbsZ: 3}, "log_only", "low-severity-log"},
		{"medium", dispatch.Facts{Kind: "anomaly", Severity: "medium", Confidence: 0.8, MaxAbsZ: 4}, "open_ticket", "medium-severity-ticket"},
		{"high", dispatch.Facts{Kind: "anomaly", Severity: "high", Confidence: 0.95, MaxAbsZ: 4}, "page_oncall", "high-severity-pages"},
		{"conf exactly 0.6 is not low", dispatch.Facts{Kind: "anomaly", Severity: "low", Confidence: 0.6, MaxAbsZ: 3}, "log_only", "low-severity-log"},
		{"conf 0.599 low sev -> human", dispatch.Facts{Kind: "anomaly", Severity: "low", Confidence: 0.599, MaxAbsZ: 3}, "page_oncall", "low-confidence-goes-to-human"},
		{"low conf beats high sev", dispatch.Facts{Kind: "anomaly", Severity: "high", Confidence: 0.2, MaxAbsZ: 3}, "page_oncall", "low-confidence-goes-to-human"},
		{"explain failed", dispatch.Facts{Kind: "anomaly", ExplainFailed: true, MaxAbsZ: 3}, "page_oncall", "explain-failed-goes-to-human"},
		{"sensor fault", dispatch.Facts{Kind: "sensor_fault", MaxAbsZ: 0}, "open_ticket", "sensor-fault-ticket"},
		{"sensor fault is not 'low confidence'", dispatch.Facts{Kind: "sensor_fault", Confidence: 0}, "open_ticket", "sensor-fault-ticket"},
		{"floor: big excursion can't be just logged", dispatch.Facts{Kind: "anomaly", Severity: "low", Confidence: 0.9, MaxAbsZ: 9}, "open_ticket", "low-severity-log"},
		{"floor never lowers", dispatch.Facts{Kind: "anomaly", Severity: "high", Confidence: 0.9, MaxAbsZ: 9}, "page_oncall", "high-severity-pages"},
		{"unknown severity -> human", dispatch.Facts{Kind: "anomaly", Severity: "catastrophic", Confidence: 0.9}, "page_oncall", "no-rule-matched"},
	}
	for _, tc := range cases {
		d := dispatch.Decide(c, tc.f)
		if d.Action != tc.want || d.Rule != tc.rule {
			t.Errorf("%s: got %s via %s, want %s via %s (%s)", tc.name, d.Action, d.Rule, tc.want, tc.rule, d.Reason)
		}
	}
}

type fakeSNS struct{ in *sns.PublishInput }

func (f *fakeSNS) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.in = in
	return &sns.PublishOutput{MessageId: aws.String("msg-1")}, nil
}

func alert() *store.Alert {
	return &store.Alert{ID: "NF-1", SessionID: "local", EquipmentID: "FARM-LGT", Kind: "anomaly",
		Flags:       []store.SeriesFlag{{SensorID: "nas_read_latency", Rule: "drift"}},
		Explanation: &explain.Explanation{Severity: "high", Confidence: 0.8, Explanation: "Shared storage is slowing the pool.", RecommendedChecks: []string{"Check NAS latency"}},
		Decision:    &store.Decision{Action: "page_oncall", Rule: "high-severity-pages", Reason: "severity=high"}}
}

func TestPagerSink(t *testing.T) {
	ctx := context.Background()
	f := &fakeSNS{}
	p := &dispatch.PagerSink{SNS: f, TopicARN: "arn:topic", Next: &dispatch.LogSink{}}

	rec, err := p.Do(ctx, alert(), config.ActionPageOnCall, true)
	if err != nil || !rec.Simulated || f.in != nil {
		t.Fatalf("sandbox pages must be simulated: %+v %v sent=%v", rec, err, f.in != nil)
	}
	rec, err = p.Do(ctx, alert(), config.ActionPageOnCall, false)
	if err != nil || rec.Simulated || rec.Ref != "msg-1" || f.in == nil {
		t.Fatalf("real page not sent: %+v %v", rec, err)
	}
	if got := aws.ToString(f.in.Subject); got != "[Northfen] HIGH anomaly on FARM-LGT" {
		t.Fatalf("subject %q", got)
	}
	f.in = nil
	rec, _ = p.Do(ctx, alert(), config.ActionOpenTicket, false)
	if f.in != nil || rec.Ref != "TKT-1" {
		t.Fatalf("tickets don't page: %+v", rec)
	}
	noTopic := &dispatch.PagerSink{Next: &dispatch.LogSink{}}
	rec, _ = noTopic.Do(ctx, alert(), config.ActionPageOnCall, false)
	if !rec.Simulated {
		t.Fatal("no topic configured -> simulated")
	}
}

func TestMessageCarriesReasoning(t *testing.T) {
	m := dispatch.NewMessage(alert(), config.ActionPageOnCall)
	if m.Summary != "Shared storage is slowing the pool." || m.Rule != "high-severity-pages" || len(m.Checks) != 1 || m.Sensors[0] != "nas_read_latency (drift)" {
		t.Fatalf("%+v", m)
	}
}
