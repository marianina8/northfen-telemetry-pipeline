// Package localapp wires the pipeline for local use (CLI, local dashboard,
// MCP server): a file-backed store shared across processes, the mock or
// Bedrock explainer, and an outbox file standing in for tickets and pages.
package localapp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

// Options select the local setup.
type Options struct {
	DataDir    string // default .northfen
	ConfigPath string // "" = embedded config/northfen.yaml
	Bedrock    bool   // use Bedrock instead of the offline mock
	Profile    string // AWS profile for Bedrock (e.g. demos-admin)
	Quiet      bool   // don't log actions to stderr
	// Store is "file" (default, DataDir) or "dynamo": the deployed stack's
	// tables, named by NF_READINGS_TABLE / NF_STATE_TABLE / NF_APP_TABLE.
	Store string
}

// App is a wired local pipeline.
type App struct {
	Svc  *pipeline.Service
	file *store.File
	Dir  string
}

// Open builds the local app. History is seeded on first use.
func Open(ctx context.Context, o Options) (*App, error) {
	if o.DataDir == "" {
		o.DataDir = ".northfen"
	}
	cfg, err := config.Load(o.ConfigPath)
	if err != nil {
		return nil, err
	}
	if o.Bedrock {
		cfg.Explain.Provider = "bedrock"
	}
	cat, err := sim.LoadCatalog()
	if err != nil {
		return nil, err
	}
	var st store.Store
	var file *store.File
	switch o.Store {
	case "", "file":
		if file, err = store.OpenFile(o.DataDir); err != nil {
			return nil, err
		}
		st = file
	case "dynamo":
		if st, err = dynamoStore(ctx, cfg.Explain.Bedrock.Region, o.Profile); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown store %q (file or dynamo)", o.Store)
	}
	model, err := explain.New(ctx, cfg, o.Profile)
	if err != nil {
		return nil, fmt.Errorf("explain provider %s: %w", cfg.Explain.Provider, err)
	}
	level := slog.LevelInfo
	if o.Quiet {
		level = slog.LevelWarn
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	sink := &dispatch.OutboxSink{Dir: o.DataDir, Next: &dispatch.PagerSink{
		PageSandboxes: cfg.Dispatch.PageSandboxSessions,
		Next:          &dispatch.LogSink{Log: log},
	}}
	svc := &pipeline.Service{Cfg: cfg, Cat: cat, Store: st, Model: model, Sink: sink, Log: log}
	if h, _ := st.History(ctx, cat.Equipment[0].ID, 1); len(h) == 0 {
		if err := svc.SeedHistory(ctx); err != nil {
			return nil, err
		}
	}
	if o.Store == "dynamo" {
		svc.ExpireAll = true // same as the deployed stack
	}
	return &App{Svc: svc, file: file, Dir: o.DataDir}, nil
}

func dynamoStore(ctx context.Context, region, profile string) (store.Store, error) {
	r, s, a := os.Getenv("NF_READINGS_TABLE"), os.Getenv("NF_STATE_TABLE"), os.Getenv("NF_APP_TABLE")
	if r == "" || s == "" || a == "" {
		return nil, fmt.Errorf("-store dynamo needs NF_READINGS_TABLE, NF_STATE_TABLE and NF_APP_TABLE (the stack outputs; see infra/README.md)")
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &store.Dynamo{Client: dynamodb.NewFromConfig(awsCfg), ReadingsTable: r, StateTable: s, AppTable: a}, nil
}

// Outbox is the local stand-in for tickets and pages.
func (a *App) Outbox() string { return filepath.Join(a.Dir, "outbox.jsonl") }

// Close releases the store lock file.
func (a *App) Close() error {
	if a.file != nil {
		return a.file.Close()
	}
	return nil
}
