package awsapp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/awsapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store/storetest"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// svc wires the pipeline over the DynamoDB fake, like the deployed stack.
func svc(t *testing.T) *pipeline.Service {
	t.Helper()
	fake := storetest.NewFakeDynamo("r", "s", "a")
	s := &pipeline.Service{Cfg: config.Default(), Cat: sim.MustCatalog(), Model: &explain.Mock{},
		Sink: &dispatch.PagerSink{Next: &dispatch.LogSink{}}, ExpireAll: true,
		Store: &store.Dynamo{Client: fake, ReadingsTable: "r", StateTable: "s", AppTable: "a"}}
	if err := s.SeedHistory(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// Phase 2: every scenario through the real handlers via the Kinesis/SQS
// stand-ins and the DynamoDB fake gives the same outcome as the direct path.
func TestRunLocalMatchesDirectPath(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"04", "05", "07", "08", "09", "10", "06"} {
		direct := svc(t)
		_, want, err := direct.RunBatch(ctx, pipeline.RunRequest{Scenario: name, RunID: "R1"}, 100)
		if err != nil {
			t.Fatal(err)
		}
		s := svc(t)
		_, rs, err := s.StartRun(ctx, pipeline.RunRequest{Scenario: name, RunID: "R1"})
		if err != nil {
			t.Fatal(err)
		}
		st, err := awsapp.RunLocal(ctx, s, rs, awsapp.LocalOptions{BatchSize: 37})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := s.RunAlerts(ctx, telemetry.LocalSession, "R1")
		if len(got) != len(want) {
			t.Fatalf("%s: %d alerts via Kinesis path, %d direct", name, len(got), len(want))
		}
		for i := range got {
			if got[i].ID != want[i].ID || got[i].Decision.Action != want[i].Decision.Action || len(got[i].Flags) != len(want[i].Flags) {
				t.Fatalf("%s: alert %s/%s vs %s/%s", name, got[i].ID, got[i].Decision.Action, want[i].ID, want[i].Decision.Action)
			}
			if got[i].ExpiresAt == 0 {
				t.Fatal("deployed stack: every record expires")
			}
		}
		if st.Records != len(rs) || st.ConsumerInvocations != (len(rs)+36)/37 {
			t.Fatalf("%s: %+v", name, st)
		}
	}
}

func kev(t *testing.T, rs []telemetry.Reading, junk bool) events.KinesisEvent {
	var ev events.KinesisEvent
	for i, r := range rs {
		b, _ := json.Marshal(r)
		ev.Records = append(ev.Records, events.KinesisEventRecord{Kinesis: events.KinesisRecord{Data: b, SequenceNumber: string(rune('a' + i%26))}})
	}
	if junk {
		ev.Records = append(ev.Records, events.KinesisEventRecord{Kinesis: events.KinesisRecord{Data: []byte("not json"), SequenceNumber: "zz"}})
	}
	return ev
}

func TestConsumerSkipsPoisonRecords(t *testing.T) {
	s := svc(t)
	_, rs, _ := s.StartRun(context.Background(), pipeline.RunRequest{Scenario: "01", RunID: "R1"})
	c := &awsapp.Consumer{Svc: s}
	resp, err := c.Handle(context.Background(), kev(t, rs[:40], true))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("%v %+v", err, resp)
	}
	states, _ := s.Store.ListStates(context.Background(), telemetry.LocalSession)
	if len(states) != 4 || states[0].State.LastTick != 9 {
		t.Fatalf("detector state not checkpointed: %+v", states)
	}
}

type failingStore struct{ store.Store }

func (failingStore) PutStates(context.Context, []store.SeriesState) error {
	return errors.New("throttled")
}

func TestConsumerReportsBatchFailureForRetry(t *testing.T) {
	s := svc(t)
	_, rs, _ := s.StartRun(context.Background(), pipeline.RunRequest{Scenario: "01", RunID: "R1"})
	s.Store = failingStore{s.Store}
	resp, err := (&awsapp.Consumer{Svc: s}).Handle(context.Background(), kev(t, rs[:8], false))
	if err != nil || len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "a" {
		t.Fatalf("want the batch retried from its first record: %v %+v", err, resp)
	}
}

type fakeSQS struct {
	mu   sync.Mutex
	msgs []*sqs.SendMessageInput
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	f.msgs = append(f.msgs, in)
	f.mu.Unlock()
	return &sqs.SendMessageOutput{}, nil
}

func TestConsumerEnqueuesWithSettleDelay(t *testing.T) {
	s := svc(t)
	q := &fakeSQS{}
	s.Queue = &awsapp.SQSQueue{Client: q, QueueURL: "u"}
	_, rs, _ := s.StartRun(context.Background(), pipeline.RunRequest{Scenario: "05", RunID: "R1"})
	if _, err := (&awsapp.Consumer{Svc: s}).Handle(context.Background(), kev(t, rs, false)); err != nil {
		t.Fatal(err)
	}
	if len(q.msgs) != 1 {
		t.Fatalf("%d jobs, want 1", len(q.msgs))
	}
	// settle 8 ticks x 0.4 s = 3.2 s -> 4 s
	if q.msgs[0].DelaySeconds != 4 {
		t.Fatalf("delay %d", q.msgs[0].DelaySeconds)
	}
	var job pipeline.ExplainJob
	json.Unmarshal([]byte(aws.ToString(q.msgs[0].MessageBody)), &job)
	// the explainer resolves it (run has ended by "now" in this test? no: requeue path)
	x := &awsapp.Explainer{Svc: s, Queue: s.Queue}
	resp, err := x.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{{MessageId: "m1", Body: aws.ToString(q.msgs[0].MessageBody)}}})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("%v %+v", err, resp)
	}
	a, _ := s.Store.GetAlert(context.Background(), job.AlertID)
	if a.Explain != store.ExplainDone || a.Decision.Action != "page_oncall" {
		t.Fatalf("explain=%s decision=%+v", a.Explain, a.Decision)
	}
	// duplicate delivery is a no-op
	x.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{{MessageId: "m2", Body: aws.ToString(q.msgs[0].MessageBody)}}})
	b, _ := s.Store.GetAlert(context.Background(), job.AlertID)
	if b.Explains != 1 || len(b.Actions) != 1 {
		t.Fatalf("duplicate delivery re-ran: explains=%d actions=%d", b.Explains, len(b.Actions))
	}
}

func TestExplainerRequeuesUntilDue(t *testing.T) {
	ctx := context.Background()
	s := svc(t)
	q := &fakeSQS{}
	s.Queue = &awsapp.SQSQueue{Client: q, QueueURL: "u"}
	_, rs, _ := s.StartRun(ctx, pipeline.RunRequest{Scenario: "05", RunID: "R1", Start: time.Now()})
	var upTo []telemetry.Reading
	for _, r := range rs {
		if r.Tick <= 71 {
			upTo = append(upTo, r)
		}
	}
	(&awsapp.Consumer{Svc: s}).Handle(ctx, kev(t, upTo, false))
	body := aws.ToString(q.msgs[0].MessageBody)
	x := &awsapp.Explainer{Svc: s, Queue: s.Queue}
	x.Handle(ctx, events.SQSEvent{Records: []events.SQSMessage{{MessageId: "m1", Body: body}}})
	if len(q.msgs) != 2 || !strings.Contains(aws.ToString(q.msgs[1].MessageBody), `"attempt":1`) {
		t.Fatalf("not-yet-due job should be re-queued: %d msgs", len(q.msgs))
	}
	var job pipeline.ExplainJob
	json.Unmarshal([]byte(body), &job)
	if a, _ := s.Store.GetAlert(ctx, job.AlertID); a.Explain != store.ExplainPending {
		t.Fatal("explained before the settle time")
	}
}

func TestExplainerDropsMalformedAndExpired(t *testing.T) {
	x := &awsapp.Explainer{Svc: svc(t)}
	resp, err := x.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "1", Body: "garbage"}, {MessageId: "2", Body: `{"alert_id":"NF-GONE"}`},
	}})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("%v %+v", err, resp)
	}
}

type fakeKinesis struct {
	mu       sync.Mutex
	puts     [][]ktypes.PutRecordsRequestEntry
	failOnce bool
}

func (f *fakeKinesis) PutRecords(_ context.Context, in *kinesis.PutRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, in.Records)
	out := &kinesis.PutRecordsOutput{}
	for i := range in.Records {
		var res ktypes.PutRecordsResultEntry
		if f.failOnce && i == 0 {
			res.ErrorCode = aws.String("ProvisionedThroughputExceededException")
			out.FailedRecordCount = aws.Int32(1)
		}
		out.Records = append(out.Records, res)
	}
	f.failOnce = false
	return out, nil
}

func TestSimulatorPacesTicksIntoKinesis(t *testing.T) {
	ctx := context.Background()
	s := svc(t)
	start := time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC)
	run, _, _ := s.StartRun(ctx, pipeline.RunRequest{SessionID: "v0123456789abcdef", Scenario: "05", RunID: "R1", Start: start})
	k := &fakeKinesis{failOnce: true}
	clock := start
	var slept time.Duration
	sm := &awsapp.Simulator{Svc: s, Producer: &awsapp.KinesisProducer{Client: k, Stream: "st"},
		Now:   func() time.Time { return clock },
		Sleep: func(_ context.Context, d time.Duration) error { slept += d; clock = clock.Add(d); return nil }}
	if err := sm.Handle(ctx, awsapp.SimulateEvent{SessionID: run.SessionID, RunID: run.ID}); err != nil {
		t.Fatal(err)
	}
	// 120 ticks, one PutRecords per tick, plus one retry of the throttled record
	if len(k.puts) != 121 || len(k.puts[1]) != 1 {
		t.Fatalf("%d puts", len(k.puts))
	}
	if want := time.Duration(119) * 400 * time.Millisecond; slept != want {
		t.Fatalf("slept %v, want %v (paced at the run's tick interval)", slept, want)
	}
	if pk := aws.ToString(k.puts[0][0].PartitionKey); pk != "v0123456789abcdef#FARM-FX" {
		t.Fatalf("partition key %q", pk)
	}
}

type fakeLambda struct{ in *lambda.InvokeInput }

func (f *fakeLambda) Invoke(_ context.Context, in *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	f.in = in
	return &lambda.InvokeOutput{StatusCode: 202}, nil
}

type fakeSSM struct{ calls int }

func (f *fakeSSM) GetParameter(_ context.Context, in *ssm.GetParameterInput, _ ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
	f.calls++
	if f.calls == 1 {
		return nil, errors.New("transient")
	}
	return &ssm.GetParameterOutput{Parameter: &ssmtypes.Parameter{Value: aws.String("s3cret")}}, nil
}

func TestWebhook(t *testing.T) {
	s := svc(t)
	fl := &fakeLambda{}
	k := &fakeKinesis{}
	sec := &awsapp.Secret{Client: &fakeSSM{}, Name: "/northfen-telemetry/webhook-shared-secret"}
	h := &awsapp.Webhook{Svc: s, Starter: &awsapp.LambdaStarter{Client: fl, Function: "sim"},
		Producer: &awsapp.KinesisProducer{Client: k, Stream: "st"}, Token: sec.Get}
	do := func(path, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		if token != "" {
			r.Header.Set(awsapp.TokenHeader, token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := do("/demo/simulate", "s3cret", `{"scenario":"04"}`); w.Code != http.StatusUnauthorized {
		t.Fatalf("SSM failure must fail closed: %d", w.Code)
	}
	if w := do("/demo/simulate", "wrong", `{"scenario":"04"}`); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", w.Code)
	}
	w := do("/demo/simulate", "s3cret", `{"scenario":"04"}`)
	if w.Code != http.StatusAccepted || fl.in == nil || !strings.Contains(string(fl.in.Payload), `"session_id":"webhook"`) {
		t.Fatalf("simulate: %d %s", w.Code, w.Body)
	}
	if w := do("/demo/simulate", "s3cret", `{"scenario":"nope"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("unknown scenario: %d", w.Code)
	}
	r := `{"session_id":"plant-a","equipment_id":"FARM-LGT","sensor_id":"node07_gpu_temp","sensor_type":"temperature","tick":0,"ts":"2026-01-05T02:00:00Z","value":60.1}`
	if w := do("/demo/readings", "s3cret", "["+r+"]"); w.Code != http.StatusAccepted || len(k.puts) != 1 {
		t.Fatalf("readings: %d %s", w.Code, w.Body)
	}
	sand := strings.Replace(r, "plant-a", "v0123456789abcdef", 1)
	if w := do("/demo/readings", "s3cret", `{"readings":[`+sand+`]}`); w.Code != http.StatusForbidden {
		t.Fatalf("webhook must not write into visitor sandboxes: %d", w.Code)
	}
	if w := do("/demo/readings", "s3cret", `[{"equipment_id":"FARM-LGT"}]`); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid reading: %d", w.Code)
	}
}
