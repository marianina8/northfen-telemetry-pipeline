package awsapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	ltypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

var errorsIs = errors.Is

// ---- Kinesis producer ------------------------------------------------------------

// KinesisAPI is the one Kinesis call used (fakeable).
type KinesisAPI interface {
	PutRecords(ctx context.Context, in *kinesis.PutRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

// KinesisProducer puts readings on the stream, partitioned by
// session#equipment so one tool's readings stay in order on one shard.
type KinesisProducer struct {
	Client KinesisAPI
	Stream string
}

// Put implements Producer (chunks of 500, retrying failed records).
func (p *KinesisProducer) Put(ctx context.Context, rs []telemetry.Reading) error {
	for i := 0; i < len(rs); i += 500 {
		var recs []ktypes.PutRecordsRequestEntry
		for _, r := range rs[i:min(i+500, len(rs))] {
			if err := r.Validate(); err != nil {
				return err
			}
			b, err := json.Marshal(r)
			if err != nil {
				return err
			}
			recs = append(recs, ktypes.PutRecordsRequestEntry{Data: b, PartitionKey: aws.String(r.PartitionKey())})
		}
		for attempt := 0; len(recs) > 0; attempt++ {
			if attempt > 4 {
				return fmt.Errorf("kinesis: %d records still failing after retries", len(recs))
			}
			if attempt > 0 {
				if err := sleepCtx(ctx, time.Duration(50<<attempt)*time.Millisecond); err != nil {
					return err
				}
			}
			out, err := p.Client.PutRecords(ctx, &kinesis.PutRecordsInput{StreamName: aws.String(p.Stream), Records: recs})
			if err != nil {
				return fmt.Errorf("kinesis put records: %w", err)
			}
			if aws.ToInt32(out.FailedRecordCount) == 0 {
				break
			}
			var retry []ktypes.PutRecordsRequestEntry
			for j, res := range out.Records {
				if res.ErrorCode != nil {
					retry = append(retry, recs[j])
				}
			}
			recs = retry
		}
	}
	return nil
}

// ---- SQS explain queue --------------------------------------------------------------

// SQSAPI is the one SQS call used (fakeable).
type SQSAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSQueue implements pipeline.JobQueue. The settle time becomes the
// message's DelaySeconds, so the explain worker runs once correlated sensors
// have had a chance to join the alert.
type SQSQueue struct {
	Client   SQSAPI
	QueueURL string
}

// EnqueueExplain implements pipeline.JobQueue.
func (q *SQSQueue) EnqueueExplain(ctx context.Context, j pipeline.ExplainJob) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	delay := int32(math.Ceil(j.Delay.Seconds()))
	delay = max(0, min(delay, 900))
	_, err = q.Client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(q.QueueURL), MessageBody: aws.String(string(b)), DelaySeconds: delay})
	if err != nil {
		return fmt.Errorf("sqs send: %w", err)
	}
	return nil
}

// ---- starting runs -------------------------------------------------------------------

// RunStarter makes a recorded run's readings flow (dashboard/webhook).
type RunStarter interface {
	Start(ctx context.Context, run store.Run) error
}

// LambdaAPI is the one Lambda call used (fakeable).
type LambdaAPI interface {
	Invoke(ctx context.Context, in *lambda.InvokeInput, opts ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// LambdaStarter invokes the simulator function asynchronously (the HTTP
// request returns at once; the simulator streams for ~50 s).
type LambdaStarter struct {
	Client   LambdaAPI
	Function string
}

// Start implements RunStarter.
func (l *LambdaStarter) Start(ctx context.Context, run store.Run) error {
	b, _ := json.Marshal(SimulateEvent{SessionID: run.SessionID, RunID: run.ID})
	_, err := l.Client.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String(l.Function), InvocationType: ltypes.InvocationTypeEvent, Payload: b})
	if err != nil {
		return fmt.Errorf("start simulator: %w", err)
	}
	return nil
}

// LocalStarter streams runs in-process (local dashboard / webhook).
type LocalStarter struct {
	Svc *pipeline.Service
	// Ctx outlives the HTTP request that started the run.
	Ctx context.Context
	wg  sync.WaitGroup
}

// Start implements RunStarter.
func (l *LocalStarter) Start(_ context.Context, run store.Run) error {
	rs, err := l.Svc.Readings(run)
	if err != nil {
		return err
	}
	ctx := l.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		if err := l.Svc.StreamLocal(ctx, run, rs, nil); err != nil && ctx.Err() == nil {
			logger(l.Svc.Log).Error("local stream failed", "run", run.ID, "err", err)
		}
	}()
	return nil
}

// Wait blocks until every started run has finished streaming.
func (l *LocalStarter) Wait() { l.wg.Wait() }

// ---- SSM ------------------------------------------------------------------------------

// SSMAPI is the one SSM call used (fakeable).
type SSMAPI interface {
	GetParameter(ctx context.Context, in *ssm.GetParameterInput, opts ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
}

// Secret reads a SecureString parameter and caches it once read
// successfully (a transient SSM error is retried on the next call rather
// than cached for the life of the Lambda container).
type Secret struct {
	Client SSMAPI
	Name   string
	mu     sync.Mutex
	val    string
}

// Get returns the parameter value.
func (s *Secret) Get(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.val != "" {
		return s.val, nil
	}
	out, err := s.Client.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(s.Name), WithDecryption: aws.Bool(true)})
	if err != nil {
		return "", fmt.Errorf("read SSM parameter %s: %w", s.Name, err)
	}
	v := aws.ToString(out.Parameter.Value)
	if v == "" {
		return "", fmt.Errorf("SSM parameter %s is empty", s.Name)
	}
	s.val = v
	return v, nil
}
