// Package awsapp holds the Lambda handlers and the AWS producers/queues.
// Handlers are plain methods over the pipeline service, unit-tested with
// synthetic events and fake clients (no AWS in tests), and driven locally by
// the Kinesis/SQS stand-ins in local.go.
//
//	simulate webhook / dashboard -> SimulatorFunction (async invoke)
//	SimulatorFunction  paces a scenario's readings into Kinesis, tick by tick
//	ConsumerFunction   Kinesis batch -> deterministic scoring -> DynamoDB;
//	                   new/changed alerts -> SQS (delayed by the settle time)
//	ExplainFunction    SQS job -> one Bedrock call -> dispatch table -> action
package awsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// ---- Kinesis consumer ------------------------------------------------------------

// Consumer is the Kinesis stream consumer: windowed scoring only, never a
// model call.
type Consumer struct {
	Svc *pipeline.Service
	Log *slog.Logger
}

func logger(l *slog.Logger) *slog.Logger {
	if l != nil {
		return l
	}
	return slog.Default()
}

// Handle processes one Kinesis batch. Undecodable records are logged and
// skipped (a poison record must not block the shard). If scoring fails, the
// whole batch is reported failed from its first record: Kinesis retries it,
// and Process is idempotent (replayed readings are skipped, IDs are
// deterministic, detector state is the checkpoint and written last).
func (c *Consumer) Handle(ctx context.Context, ev events.KinesisEvent) (events.KinesisEventResponse, error) {
	log := logger(c.Log)
	rs := make([]telemetry.Reading, 0, len(ev.Records))
	for _, rec := range ev.Records {
		var r telemetry.Reading
		if err := json.Unmarshal(rec.Kinesis.Data, &r); err != nil {
			log.Warn("skipping undecodable record", "seq", rec.Kinesis.SequenceNumber, "err", err)
			continue
		}
		rs = append(rs, r)
	}
	res, err := c.Svc.Process(ctx, rs)
	if err != nil {
		log.Error("batch failed; Kinesis will retry it", "records", len(ev.Records), "err", err)
		if len(ev.Records) == 0 {
			return events.KinesisEventResponse{}, err
		}
		return events.KinesisEventResponse{BatchItemFailures: []events.KinesisBatchItemFailure{
			{ItemIdentifier: ev.Records[0].Kinesis.SequenceNumber},
		}}, nil
	}
	log.Info("nf_batch", "records", len(ev.Records), "readings", res.Readings, "skipped", res.Skipped,
		"windows", res.Windows, "flags", res.Flags, "new_alerts", res.NewAlerts, "joined", res.Joined)
	return events.KinesisEventResponse{}, nil
}

// ---- SQS explain worker ------------------------------------------------------------

// Explainer resolves alerts: the one bounded model call, then the dispatch
// table.
type Explainer struct {
	Svc   *pipeline.Service
	Queue pipeline.JobQueue // for re-queueing jobs that aren't due yet
	Log   *slog.Logger
	// MaxRequeues bounds waiting for the settle time (then it explains with
	// what it has). RequeueDelay is the wait between checks.
	MaxRequeues  int
	RequeueDelay time.Duration
}

// Handle processes SQS explain jobs, reporting per-message failures.
func (x *Explainer) Handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	log := logger(x.Log)
	var resp events.SQSEventResponse
	for _, m := range ev.Records {
		if err := x.handle(ctx, m.Body); err != nil {
			log.Error("explain job failed", "message", m.MessageId, "err", err)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: m.MessageId})
		}
	}
	return resp, nil
}

func (x *Explainer) handle(ctx context.Context, body string) error {
	var job pipeline.ExplainJob
	if err := json.Unmarshal([]byte(body), &job); err != nil || job.AlertID == "" {
		logger(x.Log).Warn("dropping malformed explain job", "body", body)
		return nil // retrying can't fix it
	}
	a, err := x.Svc.Store.GetAlert(ctx, job.AlertID)
	if err != nil {
		if isNotFound(err) {
			return nil // expired sandbox
		}
		return err
	}
	if !x.Svc.NeedsResolve(a) {
		return nil // already done (duplicate delivery, or a later job covered it)
	}
	maxRequeues := x.MaxRequeues
	if maxRequeues == 0 {
		maxRequeues = 6
	}
	if due, err := x.Svc.Due(ctx, a); err != nil {
		return err
	} else if !due && job.Attempt < maxRequeues && x.Queue != nil {
		job.Attempt++
		job.Delay = x.RequeueDelay
		if job.Delay == 0 {
			job.Delay = 2 * time.Second
		}
		return x.Queue.EnqueueExplain(ctx, job)
	}
	a, err = x.Svc.Resolve(ctx, job.AlertID, "explain-worker")
	if err != nil {
		return err
	}
	s := pipeline.Summarize(a)
	logger(x.Log).Info("nf_resolved", "alert", a.ID, "kind", a.Kind, "explain", a.Explain, "severity", s.Severity,
		"confidence", s.Confidence, "action", s.Action)
	return nil
}

// ---- simulator ---------------------------------------------------------------------

// SimulateEvent asks the simulator to stream one recorded run.
type SimulateEvent struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
}

// Producer puts readings on the stream.
type Producer interface {
	Put(ctx context.Context, rs []telemetry.Reading) error
}

// Simulator replays a run's synthetic readings into Kinesis at the run's
// pace. It regenerates the readings from the run record (scenarios are
// deterministic), so the invoke payload is tiny.
type Simulator struct {
	Svc      *pipeline.Service
	Producer Producer
	Log      *slog.Logger
	Sleep    func(ctx context.Context, d time.Duration) error // tests
	Now      func() time.Time
}

// Handle streams the run. If the invocation starts late it catches up
// instead of sleeping, so the stream stays aligned with the run's clock.
func (sm *Simulator) Handle(ctx context.Context, ev SimulateEvent) error {
	run, err := sm.Svc.Store.GetRun(ctx, ev.SessionID, ev.RunID)
	if err != nil {
		return err
	}
	rs, err := sm.Svc.Readings(run)
	if err != nil {
		return err
	}
	now, sleep := time.Now, sleepCtx
	if sm.Now != nil {
		now = sm.Now
	}
	if sm.Sleep != nil {
		sleep = sm.Sleep
	}
	interval := time.Duration(run.TickSeconds * float64(time.Second))
	sent := 0
	for tick, batch := range pipeline.ByTick(rs) {
		if d := run.StartedAt.Add(time.Duration(tick) * interval).Sub(now()); d > 0 {
			if err := sleep(ctx, d); err != nil {
				return err
			}
		}
		if err := sm.Producer.Put(ctx, batch); err != nil {
			return fmt.Errorf("tick %d: %w", tick, err)
		}
		sent += len(batch)
	}
	logger(sm.Log).Info("nf_simulated", "session", run.SessionID, "run", run.ID, "scenario", run.Scenario, "readings", sent)
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isNotFound(err error) bool { return err != nil && errorsIs(err, store.ErrNotFound) }
