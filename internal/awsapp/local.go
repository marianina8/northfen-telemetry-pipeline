package awsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-lambda-go/events"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// memQueue is the local SQS stand-in: it keeps explain jobs in memory.
type memQueue struct {
	mu   sync.Mutex
	jobs []pipeline.ExplainJob
	sent int
}

func (q *memQueue) EnqueueExplain(_ context.Context, j pipeline.ExplainJob) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
	q.sent++
	return nil
}

func (q *memQueue) take() []pipeline.ExplainJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.jobs
	q.jobs = nil
	return out
}

// LocalOptions configure the local Kinesis/SQS stand-ins.
type LocalOptions struct {
	BatchSize int // records per consumer invocation (the event source mapping's BatchSize)
}

// LocalStats reports what the stand-ins delivered.
type LocalStats struct {
	Records             int
	ConsumerInvocations int
	ExplainJobs         int
	ExplainInvocations  int
}

// RunLocal is the Phase 2 local stand-in for the AWS path. Readings are
// encoded exactly as the producer puts them on Kinesis, grouped into
// event-source-mapping-sized batches and handed to the real Consumer
// handler; the jobs it enqueues go through an in-memory "SQS" into the real
// Explainer handler. Same handlers, same encoding, no AWS.
func RunLocal(ctx context.Context, svc *pipeline.Service, rs []telemetry.Reading, o LocalOptions) (LocalStats, error) {
	var st LocalStats
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	q := &memQueue{}
	local := *svc
	local.Queue = q
	consumer := &Consumer{Svc: &local, Log: svc.Log}
	explainer := &Explainer{Svc: &local, Queue: q, Log: svc.Log}

	// deliver hands the visible jobs to the explainer once; jobs it re-queues
	// (not due yet) become visible on the next round, like an SQS delay.
	deliver := func(untilEmpty bool) error {
		for {
			jobs := q.take()
			if len(jobs) == 0 {
				return nil
			}
			ev := events.SQSEvent{}
			for i, j := range jobs {
				b, _ := json.Marshal(j)
				ev.Records = append(ev.Records, events.SQSMessage{MessageId: fmt.Sprintf("m-%d-%d", st.ExplainInvocations, i), Body: string(b)})
			}
			st.ExplainInvocations++
			resp, err := explainer.Handle(ctx, ev)
			if err != nil {
				return err
			}
			if len(resp.BatchItemFailures) > 0 {
				return fmt.Errorf("explain worker reported %d failed jobs", len(resp.BatchItemFailures))
			}
			if !untilEmpty {
				return nil
			}
		}
	}

	seq := 0
	for i := 0; i < len(rs); i += o.BatchSize {
		var ev events.KinesisEvent
		for _, r := range rs[i:min(i+o.BatchSize, len(rs))] {
			b, err := json.Marshal(r) // exactly what KinesisProducer puts
			if err != nil {
				return st, err
			}
			seq++
			ev.Records = append(ev.Records, events.KinesisEventRecord{EventSource: "aws:kinesis", Kinesis: events.KinesisRecord{
				Data: b, PartitionKey: r.PartitionKey(), SequenceNumber: fmt.Sprintf("%021d", seq),
			}})
		}
		st.Records += len(ev.Records)
		st.ConsumerInvocations++
		resp, err := consumer.Handle(ctx, ev)
		if err != nil {
			return st, err
		}
		if len(resp.BatchItemFailures) > 0 {
			return st, fmt.Errorf("consumer reported a failed batch at %s", resp.BatchItemFailures[0].ItemIdentifier)
		}
		// SQS delay: jobs become visible as the stream moves on; the
		// explainer re-queues any that aren't due yet.
		if err := deliver(false); err != nil {
			return st, err
		}
	}
	if err := deliver(true); err != nil {
		return st, err
	}
	// End of stream: anything still settling is explained now (the
	// deployed explainer does the same once the run's end time passes).
	if _, err := local.Sweep(ctx, sessionOf(rs), false); err != nil {
		return st, err
	}
	st.ExplainJobs = q.sent
	return st, nil
}

func sessionOf(rs []telemetry.Reading) string {
	if len(rs) == 0 {
		return telemetry.LocalSession
	}
	return rs[0].SessionID
}
