package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/deadline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/localapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// hostFlag collects repeated -host HOST=METRIC flags.
type hostFlag map[string]string

func (h hostFlag) String() string { return "" }
func (h hostFlag) Set(v string) error {
	host, metric, ok := strings.Cut(v, "=")
	if !ok || host == "" || metric == "" {
		return fmt.Errorf("want HOST=METRIC, got %q", v)
	}
	h[host] = metric
	return nil
}

// cmdIngest runs a studio's own render-manager export through the pipeline,
// locally: nothing leaves the machine unless -bedrock is set.
func cmdIngest(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	from := fs.String("from", "deadline-cloud", "")
	pool := fs.String("pool", "", "")
	bucket := fs.String("bucket", "5m", "")
	failed := fs.String("failed", "", "")
	session := fs.String("session", telemetry.LocalSession, "")
	hosts := hostFlag{}
	fs.Var(hosts, "host", "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: northfen ingest -pool POOL -host HOST=METRIC [-host ...] [-failed METRIC] [-bucket 5m] EXPORT.json")
	}
	if *from != "deadline-cloud" {
		return fmt.Errorf("-from %q: only deadline-cloud is supported", *from)
	}
	b, err := os.ReadFile(pos[0])
	if err != nil {
		return err
	}
	exp, err := deadline.Parse(b)
	if err != nil {
		return err
	}
	svc := app.Svc
	eq, ok := svc.Cat.Tool(*pool)
	if *pool == "" || !ok {
		var ids []string
		for _, e := range svc.Cat.Equipment {
			ids = append(ids, e.ID)
		}
		return fmt.Errorf("-pool: one of %s (render pools in demo/equipment.yaml)", strings.Join(ids, ", "))
	}
	if len(hosts) == 0 && *failed == "" {
		fmt.Fprintf(out, "hosts in %s:\n", pos[0])
		for _, h := range exp.Hosts() {
			fmt.Fprintf(out, "  %s\n", h)
		}
		fmt.Fprintf(out, "\nmetrics on %s (%s):\n", eq.ID, eq.Name)
		for _, s := range eq.Sensors {
			fmt.Fprintf(out, "  %-22s %s (%s)\n", s.ID, s.Type, s.Unit)
		}
		fmt.Fprintln(out, "\nmap hosts to frame_time metrics with -host HOST=METRIC (repeatable), and failed task runs to an error_count metric with -failed METRIC.")
		return nil
	}
	src := sim.Source{Format: "deadline-cloud", Bucket: *bucket, FrameTime: hosts, FailedFrames: *failed}
	vals, bk, err := sim.FromDeadline(eq, exp, src, 0)
	if err != nil {
		return err
	}
	run := store.Run{
		ID: pipeline.NewRunID(), SessionID: *session, Scenario: "ingest", Title: "Deadline Cloud export: " + pos[0],
		EquipmentID: eq.ID, Ticks: bk.N, TickSeconds: bk.Bucket.Seconds(), StartedAt: bk.Start, Source: "ingest",
	}
	if err := svc.Store.PutRun(ctx, run); err != nil {
		return err
	}
	rs := sim.Replay(eq, vals, bk.N, sim.Options{SessionID: run.SessionID, RunID: run.ID, Start: bk.Start, Interval: bk.Bucket})

	runs := exp.TaskRuns()
	ok2, bad := 0, 0
	for _, r := range runs {
		switch r.Status {
		case "SUCCEEDED":
			ok2++
		case "FAILED":
			bad++
		}
	}
	var mapped []string
	for h, m := range hosts {
		mapped = append(mapped, h+" -> "+m)
	}
	sort.Strings(mapped)
	fmt.Fprintf(out, "Deadline Cloud export %s: %d task runs (%d succeeded, %d failed) on %d hosts\n", pos[0], len(runs), ok2, bad, len(exp.Hosts()))
	fmt.Fprintf(out, "resampled into %d x %s buckets from %s; %s\n", bk.N, bk.Bucket, bk.Start.Format(time.RFC3339), strings.Join(mapped, ", "))
	fmt.Fprintf(out, "run %s  session %s  on %s (%s)\n\n", run.ID, run.SessionID, eq.Name, eq.ID)

	for i := 0; i < len(rs); i += 100 {
		if _, err := svc.Process(ctx, rs[i:min(i+100, len(rs))]); err != nil {
			return err
		}
	}
	if _, err := svc.Sweep(ctx, run.SessionID, false); err != nil {
		return err
	}
	return printRun(ctx, svc, run, out)
}
