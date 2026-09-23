// Command northfen is the Northfen telemetry pipeline CLI.
//
//	northfen scenarios                      list the synthetic scenarios
//	northfen simulate <scenario> [-live]    stream a scenario through the pipeline
//	northfen score [-scenario X | -file F]  windowed detection only (no store, no AWS)
//	northfen explain <window-id|alert-id>   run / show the Bedrock explain step
//	northfen feed                           the anomaly feed
//	northfen status <alert-id>              one alert: flags, explanation, dispatch, audit trail
//	northfen ack <alert-id> [-dismiss|-escalate] -note "..."
//	northfen history <equipment> <sensor>   a sensor's readings and scored windows
//	northfen mcp [-allow-write ack,...]     MCP server on stdio
//	northfen reset                          delete local data
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	northfen "github.com/marianina8/northfen-telemetry-pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/awsapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/detect"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/localapp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/mcp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

const version = "0.1.0"

type globals struct {
	data, config, profile, store string
	bedrock, quiet               bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, "northfen:", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `northfen - Northfen Semiconductor telemetry pipeline (synthetic data)

Detection is deterministic statistics. The model only explains windows the
detector already flagged. Plain code decides what happens next.

Usage: northfen [global flags] <command> [args]

Commands:
  scenarios                       list the synthetic scenarios
  simulate <scenario>             stream a scenario through the pipeline
      -live                       pace it in real time (tick by tick)
      -via-kinesis                deliver through the local Kinesis/SQS stand-ins
                                  into the real Lambda handlers
      -session ID                 tag readings with a session (default local)
  score                           windowed detection only - no store, no AWS
      -scenario NAME | -file F    readings from a scenario or a JSONL file (- = stdin)
      -json                       machine-readable output
  explain <window-id|alert-id>    explain a flagged window (refuses unflagged ones)
      -rerun                      dry-run the model again (not recorded)
  feed                            anomaly feed (newest first)
      -session ID | -all
  status <alert-id> [-json]       one alert with explanation, dispatch, audit trail
  ack <alert-id>                  acknowledge (default), or -dismiss / -escalate
      -note TEXT  -as NAME
  history <equipment> <sensor>    readings + scored windows for a sensor
      -run ID                     (default: the latest run on that tool)
  mcp                             MCP server on stdio (read-only by default)
      -allow-write T1,T2          enable write tools one by one: ack, escalate, simulate
      -actor NAME
  reset                           delete local data

Global flags:
  -data DIR       local data directory (default .northfen)
  -config FILE    config file (default: embedded config/northfen.yaml)
  -bedrock        use Amazon Bedrock for the explain step (default: offline mock)
  -profile NAME   AWS profile for Bedrock / DynamoDB (default demos-admin)
  -store KIND     file (default, -data) or dynamo (the deployed stack's tables,
                  from NF_READINGS_TABLE / NF_STATE_TABLE / NF_APP_TABLE)
  -quiet          don't log actions to stderr
`)
}

func run(ctx context.Context, args []string, out io.Writer, in io.Reader) error {
	fs := flag.NewFlagSet("northfen", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var g globals
	fs.StringVar(&g.data, "data", ".northfen", "")
	fs.StringVar(&g.config, "config", "", "")
	fs.StringVar(&g.profile, "profile", "demos-admin", "")
	fs.BoolVar(&g.bedrock, "bedrock", false, "")
	fs.BoolVar(&g.quiet, "quiet", false, "")
	fs.StringVar(&g.store, "store", "file", "")
	if err := fs.Parse(args); err != nil {
		usage(out)
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		usage(out)
		return nil
	}
	cmd, cargs := rest[0], rest[1:]
	switch cmd {
	case "help", "-h", "--help":
		usage(out)
		return nil
	case "version":
		fmt.Fprintln(out, "northfen", version)
		return nil
	case "scenarios":
		return cmdScenarios(out)
	case "score":
		return cmdScore(g, cargs, out, in)
	case "reset":
		if err := os.RemoveAll(g.data); err != nil {
			return err
		}
		fmt.Fprintf(out, "removed %s\n", g.data)
		return nil
	}

	open := func() (*localapp.App, error) {
		return localapp.Open(ctx, localapp.Options{DataDir: g.data, ConfigPath: g.config, Bedrock: g.bedrock, Profile: g.profile, Quiet: g.quiet || cmd == "mcp", Store: g.store})
	}
	app, err := open()
	if err != nil {
		return err
	}
	defer app.Close()
	switch cmd {
	case "simulate":
		return cmdSimulate(ctx, app, cargs, out)
	case "explain":
		return cmdExplain(ctx, app, cargs, out)
	case "feed":
		return cmdFeed(ctx, app, cargs, out)
	case "status":
		return cmdStatus(ctx, app, cargs, out)
	case "ack":
		return cmdAck(ctx, app, cargs, out)
	case "history":
		return cmdHistory(ctx, app, cargs, out)
	case "mcp":
		return cmdMCP(ctx, app, cargs, in, out)
	}
	usage(out)
	return fmt.Errorf("unknown command %q", cmd)
}

// parse lets flags appear before or after positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	fs.SetOutput(io.Discard)
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

// ---- scenarios ---------------------------------------------------------------

func cmdScenarios(out io.Writer) error {
	cat, err := sim.LoadCatalog()
	if err != nil {
		return err
	}
	exp := loadExpected()
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SCENARIO\tTOOL\tCATEGORY\tEXPECTED\tTITLE")
	for _, s := range cat.Scenarios {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", s.File, s.EquipmentID, s.Category, exp[s.File], s.Title)
	}
	return tw.Flush()
}

func loadExpected() map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile("demo/expected.json")
	if err != nil {
		b, err = embeddedExpected()
		if err != nil {
			return out
		}
	}
	var doc struct {
		Scenarios map[string]struct {
			ShouldFlag bool `json:"should_flag"`
			Alerts     []struct {
				Kind string `json:"kind"`
			} `json:"alerts"`
		} `json:"scenarios"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return out
	}
	for k, v := range doc.Scenarios {
		switch {
		case !v.ShouldFlag:
			out[k] = "no flag"
		case len(v.Alerts) > 0 && v.Alerts[0].Kind == detect.KindSensorFault:
			out[k] = "sensor fault"
		default:
			out[k] = "anomaly"
		}
	}
	return out
}

// ---- score (standalone detection) --------------------------------------------

func cmdScore(g globals, args []string, out io.Writer, in io.Reader) error {
	fs := flag.NewFlagSet("score", flag.ContinueOnError)
	scenario := fs.String("scenario", "", "")
	file := fs.String("file", "", "")
	asJSON := fs.Bool("json", false, "")
	points := fs.Bool("points", false, "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if *scenario == "" && *file == "" && len(pos) == 1 {
		*scenario = pos[0]
	}
	cfg, err := config.Load(g.config)
	if err != nil {
		return err
	}
	var rs []telemetry.Reading
	switch {
	case *scenario != "":
		cat, err := sim.LoadCatalog()
		if err != nil {
			return err
		}
		sc, ok := cat.Scenario(*scenario)
		if !ok {
			return fmt.Errorf("unknown scenario %q", *scenario)
		}
		if rs, err = sim.Generate(cat, sc, sim.Options{}); err != nil {
			return err
		}
	case *file != "":
		r := in
		if *file != "-" {
			f, err := os.Open(*file)
			if err != nil {
				return err
			}
			defer f.Close()
			r = f
		}
		if rs, err = readJSONL(r); err != nil {
			return err
		}
	default:
		return errors.New("score needs -scenario NAME or -file readings.jsonl")
	}
	series := detect.ScoreAll(cfg.Detector, rs, *points)
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(series)
	}
	fmt.Fprintf(out, "%d readings, %d series - deterministic detection only (no model, no AWS)\n\n", len(rs), len(series))
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SERIES\tTYPE\tSIGMA\tMAX|z|\tWINDOWS\tFLAGS")
	for _, s := range series {
		fmt.Fprintf(tw, "%s\t%s\t%.4g\t%.2f\t%s\t%s\n", s.Key, s.SensorType, s.State.Sigma, maxAbsZ(s), strip(s.Windows), flags(s.Flags))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(out, "\nwindows: ~ warm-up  . normal  ! anomaly rule  x sensor fault (one character per window)")
	return nil
}

func readJSONL(r io.Reader) ([]telemetry.Reading, error) {
	var out []telemetry.Reading
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rd telemetry.Reading
		if err := json.Unmarshal([]byte(line), &rd); err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		if rd.SessionID == "" {
			rd.SessionID = telemetry.LocalSession
		}
		out = append(out, rd)
	}
	return out, sc.Err()
}

func maxAbsZ(s *detect.Series) float64 {
	m := 0.0
	for _, w := range s.Windows {
		m = math.Max(m, w.MaxAbsZ)
	}
	return m
}

func strip(ws []detect.Window) string {
	var b strings.Builder
	for _, w := range ws {
		b.WriteByte(windowChar(w.Warm, w.Rules))
	}
	return b.String()
}

func windowChar(warm bool, rules []string) byte {
	switch {
	case len(rules) > 0 && detect.KindOf(rules[0]) == detect.KindSensorFault:
		return 'x'
	case len(rules) > 0:
		return '!'
	case !warm:
		return '~'
	}
	return '.'
}

func flags(fs []detect.Flag) string {
	if len(fs) == 0 {
		return "-"
	}
	var parts []string
	for _, f := range fs {
		parts = append(parts, fmt.Sprintf("%s@%d(z=%.2f)", f.Rule, f.Tick, f.Z))
	}
	return strings.Join(parts, " ")
}

// ---- simulate -----------------------------------------------------------------

func cmdSimulate(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("simulate", flag.ContinueOnError)
	live := fs.Bool("live", false, "")
	viaKinesis := fs.Bool("via-kinesis", false, "")
	session := fs.String("session", telemetry.LocalSession, "")
	tickSec := fs.Float64("tick", 0, "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: northfen simulate <scenario> [-live] [-via-kinesis]  (see `northfen scenarios`)")
	}
	svc := app.Svc
	req := pipeline.RunRequest{SessionID: *session, Scenario: pos[0], Source: "cli", TickSeconds: *tickSec}
	if !*live {
		req.TickSeconds = 1 // timestamps only; nothing waits
	}
	run, rs, err := svc.StartRun(ctx, req)
	if err != nil {
		return err
	}
	sc, _ := svc.Cat.Scenario(run.Scenario)
	eq, _ := svc.Cat.Tool(run.EquipmentID)
	fmt.Fprintf(out, "%s - %s\n", sc.File, sc.Title)
	fmt.Fprintf(out, "run %s  session %s  %d ticks x %d sensors on %s (%s)\n\n", run.ID, run.SessionID, run.Ticks, len(eq.Sensors), eq.Name, eq.ToolType)

	switch {
	case *viaKinesis:
		fmt.Fprintln(out, "delivering through the local Kinesis + SQS stand-ins into the Lambda handlers...")
		st, err := awsapp.RunLocal(ctx, svc, rs, awsapp.LocalOptions{BatchSize: 100})
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  kinesis: %d records in %d consumer invocations; sqs: %d explain jobs\n\n", st.Records, st.ConsumerInvocations, st.ExplainJobs)
	case *live:
		if run.TickSeconds <= 0 {
			run.TickSeconds = svc.Cfg.Simulate.TickSeconds
		}
		fmt.Fprintf(out, "streaming at %.1fs per tick (Ctrl-C to stop)...\n", run.TickSeconds)
		err := svc.StreamLocal(ctx, run, rs, func(ev pipeline.TickEvent) {
			for _, id := range ev.Result.NewAlerts {
				a, _ := svc.Store.GetAlert(ctx, id)
				f := a.Flags[0]
				fmt.Fprintf(out, "  tick %3d  FLAG   %s  %s %s rule (z=%.2f) -> alert %s\n", ev.Tick, a.EquipmentID, f.SensorID, f.Rule, f.Z, id)
			}
			for _, id := range ev.Result.Joined {
				a, _ := svc.Store.GetAlert(ctx, id)
				f := a.Flags[len(a.Flags)-1]
				fmt.Fprintf(out, "  tick %3d  JOIN   %s %s rule joins alert %s (correlated)\n", ev.Tick, f.SensorID, f.Rule, id)
			}
			for _, a := range ev.Resolved {
				s := pipeline.Summarize(a)
				what := "explained"
				if a.Kind == detect.KindSensorFault {
					what = "sensor fault, no model call"
				}
				fmt.Fprintf(out, "  tick %3d  %-6s %s %s -> %s\n", ev.Tick, "RESOLVE", a.ID, what, s.Action)
			}
			if ev.Tick%20 == 0 && ev.Tick < run.Ticks {
				fmt.Fprintf(out, "  tick %3d  ...\n", ev.Tick)
			}
		})
		if err != nil {
			return err
		}
		fmt.Fprintln(out)
	default:
		for i := 0; i < len(rs); i += 100 {
			if _, err := svc.Process(ctx, rs[i:min(i+100, len(rs))]); err != nil {
				return err
			}
		}
		if _, err := svc.Sweep(ctx, run.SessionID, false); err != nil {
			return err
		}
	}
	return printRun(ctx, svc, run, out)
}

func printRun(ctx context.Context, svc *pipeline.Service, run store.Run, out io.Writer) error {
	ws, err := svc.Store.ListWindows(ctx, run.SessionID, run.ID)
	if err != nil {
		return err
	}
	eq, _ := svc.Cat.Tool(run.EquipmentID)
	bySensor := map[string][]store.Window{}
	for _, w := range ws {
		bySensor[w.SensorID] = append(bySensor[w.SensorID], w)
	}
	fmt.Fprintf(out, "%d windows scored (one character per %d-reading window: ~ warm-up  . normal  ! flagged  x sensor fault)\n",
		len(ws), svc.Cfg.Detector.WindowSize)
	for _, sn := range eq.Sensors {
		var b strings.Builder
		for _, w := range bySensor[sn.ID] {
			b.WriteByte(statusChar(w.Status))
		}
		fmt.Fprintf(out, "  %-18s %s\n", sn.ID, b.String())
	}
	as, err := svc.RunAlerts(ctx, run.SessionID, run.ID)
	if err != nil {
		return err
	}
	fmt.Fprintln(out)
	if len(as) == 0 {
		fmt.Fprintln(out, "No anomalies: every window within this tool's learned normal. No model call was made.")
		return nil
	}
	fmt.Fprintf(out, "%d alert(s):\n", len(as))
	for _, a := range as {
		printAlert(out, a, false)
	}
	return nil
}

func statusChar(s string) byte {
	switch s {
	case "warmup":
		return '~'
	case "flagged":
		return '!'
	case "fault":
		return 'x'
	}
	return '.'
}

func printAlert(out io.Writer, a store.Alert, full bool) {
	fmt.Fprintf(out, "\n  %s  %s on %s  (status %s)\n", a.ID, a.Kind, a.EquipmentID, a.Status)
	var fl []string
	for _, f := range a.Flags {
		fl = append(fl, fmt.Sprintf("%s %s@tick %d (z=%.2f)", f.SensorID, f.Rule, f.Tick, f.Z))
	}
	fmt.Fprintf(out, "    detector  %s\n", strings.Join(fl, "; "))
	fmt.Fprintf(out, "    window    %s\n", a.WindowID)
	switch a.Explain {
	case store.ExplainSkipped:
		fmt.Fprintln(out, "    explain   skipped - sensor fault: the sensor itself looks broken, so there's nothing to diagnose")
	case store.ExplainPending:
		fmt.Fprintln(out, "    explain   pending (waiting for correlated sensors to settle)")
	case store.ExplainFailed:
		fmt.Fprintf(out, "    explain   FAILED: %s\n", a.ExplainErr)
	case store.ExplainDone:
		e := a.Explanation
		fmt.Fprintf(out, "    explain   %s  severity=%s confidence=%.2f\n", e.Model, e.Severity, e.Confidence)
		fmt.Fprintf(out, "              likely causes: %s\n", strings.Join(e.LikelyCauses, ", "))
		fmt.Fprintf(out, "              %s\n", wrap(e.Explanation, 90, "              "))
		for _, c := range e.RecommendedChecks {
			fmt.Fprintf(out, "              check: %s\n", c)
		}
	}
	if d := a.Decision; d != nil {
		fmt.Fprintf(out, "    dispatch  %s  (rule %s: %s)\n", strings.ToUpper(d.Action), d.Rule, d.Reason)
	}
	for _, r := range a.Actions {
		fmt.Fprintf(out, "    action    %s\n", r.Detail)
	}
	if full {
		fmt.Fprintln(out, "    audit trail:")
		for _, e := range a.Events {
			fmt.Fprintf(out, "      %s  %-16s %-20s %s\n", e.At.Format(time.RFC3339), e.Actor, e.Type, e.Detail)
		}
	}
}

func wrap(s string, width int, indent string) string {
	words := strings.Fields(s)
	var b strings.Builder
	n := 0
	for i, w := range words {
		if i > 0 && n+len(w)+1 > width {
			b.WriteString("\n" + indent)
			n = 0
		} else if i > 0 {
			b.WriteByte(' ')
			n++
		}
		b.WriteString(w)
		n += len(w)
	}
	return b.String()
}

// ---- explain ------------------------------------------------------------------

func cmdExplain(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	rerun := fs.Bool("rerun", false, "")
	showInput := fs.Bool("input", false, "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: northfen explain <window-id|alert-id> [-rerun] [-input]")
	}
	svc := app.Svc
	id := pos[0]
	var a store.Alert
	if strings.HasPrefix(id, "W-") {
		var w store.Window
		a, w, err = svc.AlertForWindow(ctx, id)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "window %s (%s %s, ticks %d-%d) belongs to alert %s\n", w.ID, w.EquipmentID, w.SensorID, w.StartTick, w.EndTick, a.ID)
	} else if a, err = svc.Store.GetAlert(ctx, id); err != nil {
		return err
	}
	if a.Kind == detect.KindSensorFault {
		fmt.Fprintln(out, "sensor fault: explain is skipped by design (the sensor is broken; there is no process to diagnose)")
		printAlert(out, a, false)
		return nil
	}
	if svc.NeedsResolve(a) {
		if a, err = svc.Resolve(ctx, a.ID, "cli"); err != nil {
			return err
		}
		printAlert(out, a, false)
		return nil
	}
	if !*rerun {
		printAlert(out, a, false)
		if *showInput && a.Input != nil {
			b, _ := json.MarshalIndent(a.Input, "", "  ")
			fmt.Fprintf(out, "\n  model input (exactly what the model saw):\n%s\n", b)
		}
		fmt.Fprintln(out, "\n  (already explained - use -rerun to dry-run the model again, e.g. with -bedrock)")
		return nil
	}
	in, err := svc.BuildInput(ctx, a)
	if err != nil {
		return err
	}
	e, err := svc.Model.Explain(ctx, in)
	if err != nil {
		return fmt.Errorf("%s: %w (raw: %q)", svc.Model.Name(), err, e.Raw)
	}
	fmt.Fprintf(out, "DRY RUN (not recorded, no action taken) - %s\n", e.Model)
	fmt.Fprintf(out, "  severity=%s confidence=%.2f causes=%s\n  %s\n", e.Severity, e.Confidence, strings.Join(e.LikelyCauses, ", "), e.Explanation)
	for _, c := range e.RecommendedChecks {
		fmt.Fprintf(out, "  check: %s\n", c)
	}
	return nil
}

// ---- feed / status / ack / history ------------------------------------------

func cmdFeed(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("feed", flag.ContinueOnError)
	session := fs.String("session", telemetry.LocalSession, "")
	all := fs.Bool("all", false, "")
	asJSON := fs.Bool("json", false, "")
	if _, err := parse(fs, args); err != nil {
		return err
	}
	if *all {
		*session = ""
	}
	rows, err := app.Svc.Feed(ctx, *session)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	}
	if len(rows) == 0 {
		fmt.Fprintln(out, "No alerts. Try: northfen simulate 10")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ALERT\tTOOL\tKIND\tSENSORS\tSEVERITY\tCONF\tTOP CAUSE\tACTION\tSTATUS\tSCENARIO")
	for _, r := range rows {
		conf := "-"
		if r.Explain == "done" {
			conf = fmt.Sprintf("%.2f", r.Confidence)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.ID, r.EquipmentID, r.Kind, strings.Join(r.Sensors, ","),
			dash(r.Severity), conf, dash(r.TopCause), dash(r.Action), r.Status, r.Scenario)
	}
	return tw.Flush()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func cmdStatus(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: northfen status <alert-id> [-json]")
	}
	a, err := app.Svc.Store.GetAlert(ctx, pos[0])
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(a)
	}
	printAlert(out, a, true)
	return nil
}

func cmdAck(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("ack", flag.ContinueOnError)
	dismiss := fs.Bool("dismiss", false, "")
	escalate := fs.Bool("escalate", false, "")
	note := fs.String("note", "", "")
	as := fs.String("as", os.Getenv("USER"), "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 || (*dismiss && *escalate) {
		return errors.New("usage: northfen ack <alert-id> [-dismiss|-escalate] -note \"why\"")
	}
	verb := pipeline.VerbAcknowledge
	if *dismiss {
		verb = pipeline.VerbDismiss
	}
	if *escalate {
		verb = pipeline.VerbEscalate
	}
	name := *as
	if name == "" {
		name = "cli"
	}
	a, err := app.Svc.Ack(ctx, pipeline.AckRequest{AlertID: pos[0], Verb: verb, Actor: "engineer:" + name, Note: *note})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s -> %s\n", a.ID, a.Status)
	printAlert(out, a, true)
	return nil
}

func cmdHistory(ctx context.Context, app *localapp.App, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	runID := fs.String("run", "", "")
	session := fs.String("session", telemetry.LocalSession, "")
	asJSON := fs.Bool("json", false, "")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errors.New("usage: northfen history <equipment> <sensor> [-run ID]")
	}
	if *runID == "" {
		runs, err := app.Svc.Store.ListRuns(ctx, *session)
		if err != nil {
			return err
		}
		sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.After(runs[j].StartedAt) })
		for _, r := range runs {
			if r.EquipmentID == pos[0] {
				*runID = r.ID
				break
			}
		}
		if *runID == "" {
			return fmt.Errorf("no runs on %s yet", pos[0])
		}
	}
	h, err := app.Svc.SensorHistory(ctx, *session, *runID, pos[0], pos[1])
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(h)
	}
	fmt.Fprintf(out, "%s %s  run %s  %d readings\n\n", h.EquipmentID, h.SensorID, h.RunID, len(h.Readings))
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "WINDOW\tTICKS\tSTATUS\tMEAN\tMIN\tMAX\tMAX|z|\tMISSING\tRULES\tALERT")
	for _, w := range h.Windows {
		fmt.Fprintf(tw, "%s\t%d-%d\t%s\t%.4g\t%.4g\t%.4g\t%.2f\t%d\t%s\t%s\n", w.ID, w.StartTick, w.EndTick, w.Status, w.Mean, w.Min, w.Max,
			w.MaxAbsZ, w.Missing, dash(strings.Join(w.Rules, ",")), dash(w.AlertID))
	}
	return tw.Flush()
}

// ---- mcp ----------------------------------------------------------------------

func cmdMCP(ctx context.Context, app *localapp.App, args []string, in io.Reader, out io.Writer) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	allow := fs.String("allow-write", "", "")
	actor := fs.String("actor", "agent", "")
	session := fs.String("session", telemetry.LocalSession, "")
	if _, err := parse(fs, args); err != nil {
		return err
	}
	aw, err := mcp.ParseAllowWrite(*allow)
	if err != nil {
		return err
	}
	srv := &mcp.Server{Svc: app.Svc, AllowWrite: aw, Actor: *actor, Session: *session, Version: version,
		Audit: os.Stderr}
	return srv.Serve(ctx, in, out)
}

func embeddedExpected() ([]byte, error) { return fs.ReadFile(northfen.Demo, "demo/expected.json") }
