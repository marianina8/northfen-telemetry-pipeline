package awsapp

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

// Env is what the SAM template passes to every function.
type Env struct {
	ReadingsTable     string // NF_READINGS_TABLE
	StateTable        string // NF_STATE_TABLE
	AppTable          string // NF_APP_TABLE
	Stream            string // NF_STREAM_NAME
	ExplainQueueURL   string // NF_EXPLAIN_QUEUE_URL
	SimulatorFunction string // NF_SIMULATOR_FUNCTION
	ExplainProvider   string // NF_EXPLAIN_PROVIDER: bedrock | mock
	BedrockModelID    string // NF_BEDROCK_MODEL_ID
	PageTopicARN      string // NF_PAGE_TOPIC_ARN
	WebhookSecret     string // NF_WEBHOOK_SECRET_PARAM (SSM name)
	DashboardPassword string // NF_DASHBOARD_PASSWORD_PARAM (SSM name)
}

// EnvFromOS reads the environment.
func EnvFromOS() Env {
	return Env{
		ReadingsTable: os.Getenv("NF_READINGS_TABLE"), StateTable: os.Getenv("NF_STATE_TABLE"), AppTable: os.Getenv("NF_APP_TABLE"),
		Stream: os.Getenv("NF_STREAM_NAME"), ExplainQueueURL: os.Getenv("NF_EXPLAIN_QUEUE_URL"),
		SimulatorFunction: os.Getenv("NF_SIMULATOR_FUNCTION"), ExplainProvider: os.Getenv("NF_EXPLAIN_PROVIDER"),
		BedrockModelID: os.Getenv("NF_BEDROCK_MODEL_ID"), PageTopicARN: os.Getenv("NF_PAGE_TOPIC_ARN"),
		WebhookSecret: os.Getenv("NF_WEBHOOK_SECRET_PARAM"), DashboardPassword: os.Getenv("NF_DASHBOARD_PASSWORD_PARAM"),
	}
}

// App is the AWS-wired pipeline plus the clients handlers need.
type App struct {
	Svc  *pipeline.Service
	AWS  aws.Config
	Env  Env
	Log  *slog.Logger
	Cfg  *config.Config
	Cat  *sim.Catalog
	SSM  *ssm.Client
	Kin  *kinesis.Client
	Lamb *lambda.Client
}

// New builds the AWS app. Loading AWS config makes no network call.
func New(ctx context.Context, env Env) (*App, error) {
	if env.ReadingsTable == "" || env.StateTable == "" || env.AppTable == "" {
		return nil, fmt.Errorf("NF_READINGS_TABLE, NF_STATE_TABLE and NF_APP_TABLE must be set")
	}
	cfg, err := config.Load("")
	if err != nil {
		return nil, err
	}
	if env.ExplainProvider != "" {
		cfg.Explain.Provider = env.ExplainProvider
	}
	if env.BedrockModelID != "" {
		cfg.Explain.Bedrock.ModelID = env.BedrockModelID
	}
	if r := os.Getenv("AWS_REGION"); r != "" {
		cfg.Explain.Bedrock.Region = r
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Explain.Bedrock.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	cat, err := sim.LoadCatalog()
	if err != nil {
		return nil, err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	var model explain.Model = &explain.Mock{}
	if cfg.Explain.Provider == "bedrock" {
		if model, err = explain.NewBedrock(bedrockruntime.NewFromConfig(awsCfg), cfg); err != nil {
			return nil, err
		}
	}
	var sink dispatch.Sink = &dispatch.LogSink{Log: log}
	pager := &dispatch.PagerSink{PageSandboxes: cfg.Dispatch.PageSandboxSessions, Next: sink}
	if env.PageTopicARN != "" {
		pager.SNS, pager.TopicARN = sns.NewFromConfig(awsCfg), env.PageTopicARN
	}
	svc := &pipeline.Service{
		Cfg: cfg, Cat: cat, Model: model, Sink: pager, Log: log, ExpireAll: true,
		Store: &store.Dynamo{Client: dynamodb.NewFromConfig(awsCfg), ReadingsTable: env.ReadingsTable, StateTable: env.StateTable, AppTable: env.AppTable},
	}
	if env.ExplainQueueURL != "" {
		svc.Queue = &SQSQueue{Client: sqs.NewFromConfig(awsCfg), QueueURL: env.ExplainQueueURL}
	}
	return &App{Svc: svc, AWS: awsCfg, Env: env, Log: log, Cfg: cfg, Cat: cat,
		SSM: ssm.NewFromConfig(awsCfg), Kin: kinesis.NewFromConfig(awsCfg), Lamb: lambda.NewFromConfig(awsCfg)}, nil
}

// Producer returns the Kinesis producer for NF_STREAM_NAME.
func (a *App) Producer() Producer { return &KinesisProducer{Client: a.Kin, Stream: a.Env.Stream} }

// Starter returns the simulator starter for NF_SIMULATOR_FUNCTION.
func (a *App) Starter() RunStarter {
	return &LambdaStarter{Client: a.Lamb, Function: a.Env.SimulatorFunction}
}

// Secret returns a cached SSM parameter reader.
func (a *App) Secret(name string) *Secret { return &Secret{Client: a.SSM, Name: name} }
