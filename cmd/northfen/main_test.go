package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func cli(t *testing.T, dir string, stdin string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := run(context.Background(), append([]string{"-data", dir, "-quiet"}, args...), &out, strings.NewReader(stdin))
	return out.String(), err
}

func TestCLIEndToEnd(t *testing.T) {
	dir := t.TempDir()
	out, err := cli(t, dir, "", "simulate", "10")
	if err != nil || !strings.Contains(out, "1 alert(s)") || !strings.Contains(out, "PAGE_ONCALL") {
		t.Fatalf("%v\n%s", err, out)
	}
	out, _ = cli(t, dir, "", "simulate", "06")
	if !strings.Contains(out, "No anomalies") || !strings.Contains(out, "No model call") {
		t.Fatal(out)
	}
	feed, _ := cli(t, dir, "", "feed", "-json")
	var rows []map[string]any
	json.Unmarshal([]byte(feed), &rows)
	if len(rows) != 1 {
		t.Fatalf("feed: %s", feed)
	}
	id := rows[0]["id"].(string)
	if _, err := cli(t, dir, "", "ack", id, "-dismiss"); err == nil {
		t.Fatal("dismiss without a note")
	}
	out, err = cli(t, dir, "", "ack", id, "-note", "on it", "-as", "sam")
	if err != nil || !strings.Contains(out, "acknowledged") || !strings.Contains(out, "engineer:sam") {
		t.Fatalf("%v %s", err, out)
	}
	hist, err := cli(t, dir, "", "history", "FARM-LGT", "nas_read_latency", "-json")
	if err != nil {
		t.Fatal(err)
	}
	var h struct {
		Windows []struct {
			ID      string `json:"id"`
			AlertID string `json:"alert_id"`
		} `json:"windows"`
	}
	json.Unmarshal([]byte(hist), &h)
	var quiet, flagged string
	for _, w := range h.Windows {
		if w.AlertID == "" && quiet == "" {
			quiet = w.ID
		} else if w.AlertID != "" && flagged == "" {
			flagged = w.ID
		}
	}
	if _, err := cli(t, dir, "", "explain", quiet); err == nil || !strings.Contains(err.Error(), "not flagged") {
		t.Fatalf("explain on an unflagged window: %v", err)
	}
	out, err = cli(t, dir, "", "explain", flagged, "-rerun")
	if err != nil || !strings.Contains(out, "DRY RUN") {
		t.Fatalf("%v %s", err, out)
	}
	out, _ = cli(t, dir, "", "status", id)
	if !strings.Contains(out, "audit trail") || !strings.Contains(out, "acknowledged") {
		t.Fatal(out)
	}
}

func TestCLIViaKinesisAndLive(t *testing.T) {
	dir := t.TempDir()
	out, err := cli(t, dir, "", "simulate", "08", "-via-kinesis")
	if err != nil || !strings.Contains(out, "consumer invocations") || !strings.Contains(out, "OPEN_TICKET") {
		t.Fatalf("%v\n%s", err, out)
	}
	out, err = cli(t, dir, "", "simulate", "05", "-live", "-tick", "0.001")
	if err != nil || !strings.Contains(out, "FLAG") || !strings.Contains(out, "RESOLVE") {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestScoreStandalone(t *testing.T) {
	out, err := cli(t, t.TempDir(), "", "score", "-scenario", "09")
	if err != nil || !strings.Contains(out, "sustained@22(z=3.00)") || !strings.Contains(out, "no model, no AWS") {
		t.Fatalf("%v\n%s", err, out)
	}
	in := `{"equipment_id":"X","sensor_id":"s","sensor_type":"temperature","tick":0,"ts":"2026-01-01T00:00:00Z","value":1}` + "\n"
	out, err = cli(t, t.TempDir(), in, "score", "-file", "-")
	if err != nil || !strings.Contains(out, "X#s") {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestMCPOverCLI(t *testing.T) {
	msg := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}` + "\n"
	out, err := cli(t, t.TempDir(), msg, "mcp")
	if err != nil || !strings.Contains(out, `"feed"`) || strings.Contains(out, `"ack"`) {
		t.Fatalf("%v %s", err, out)
	}
	if _, err := cli(t, t.TempDir(), "", "mcp", "-allow-write", "delete_everything"); err == nil {
		t.Fatal("unknown write tool accepted")
	}
}

func TestIngestDeadlineExport(t *testing.T) {
	dir := t.TempDir()
	file := "../../demo/deadline/lgt-overnight-export.json"
	out, err := cli(t, dir, "", "ingest", "-pool", "FARM-LGT", file)
	if err != nil || !strings.Contains(out, "render-node12") {
		t.Fatalf("host listing: %v\n%s", err, out)
	}
	out, err = cli(t, dir, "", "ingest", "-pool", "FARM-LGT", "-host", "render-node07=node07_frame_time", "-host", "render-node12=node12_frame_time", file)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"14 failed", "1 alert(s)", "node12_frame_time spike", "OPEN_TICKET"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if _, err := cli(t, dir, "", "ingest", "-pool", "FARM-LGT", "-host", "render-node99=node07_frame_time", file); err == nil || !strings.Contains(err.Error(), "not in the export") {
		t.Errorf("unknown host: %v", err)
	}
	if _, err := cli(t, dir, "", "ingest", "-pool", "FARM-LGT", "-host", "render-node07=node07_gpu_temp", file); err == nil {
		t.Error("mapping frame times onto a temperature metric should fail")
	}
}
