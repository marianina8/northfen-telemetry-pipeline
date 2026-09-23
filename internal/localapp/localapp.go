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
}

// App is a wired local pipeline.
type App struct {
	Svc   *pipeline.Service
	Store *store.File
	Dir   string
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
	st, err := store.OpenFile(o.DataDir)
	if err != nil {
		return nil, err
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
	return &App{Svc: svc, Store: st, Dir: o.DataDir}, nil
}

// Outbox is the local stand-in for tickets and pages.
func (a *App) Outbox() string { return filepath.Join(a.Dir, "outbox.jsonl") }

// Close releases the store lock file.
func (a *App) Close() error { return a.Store.Close() }
