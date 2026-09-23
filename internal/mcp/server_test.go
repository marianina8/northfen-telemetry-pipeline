package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/mcp"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

func newServer(t *testing.T, allow string) (*mcp.Server, *bytes.Buffer) {
	t.Helper()
	svc := &pipeline.Service{Cfg: config.Default(), Cat: sim.MustCatalog(), Store: store.NewMem(), Model: &explain.Mock{},
		Sink: &dispatch.PagerSink{Next: &dispatch.LogSink{}}}
	svc.SeedHistory(context.Background())
	aw, err := mcp.ParseAllowWrite(allow)
	if err != nil {
		t.Fatal(err)
	}
	var audit bytes.Buffer
	return &mcp.Server{Svc: svc, AllowWrite: aw, Actor: "test-agent", Version: "t", Audit: &audit}, &audit
}

type resp struct {
	Result struct {
		Tools   []struct{ Name string } `json:"tools"`
		IsError bool                    `json:"isError"`
		Content []struct{ Text string } `json:"content"`
		Struct  map[string]any          `json:"structuredContent"`
		Proto   string                  `json:"protocolVersion"`
	} `json:"result"`
	Error *struct{ Code int } `json:"error"`
}

func call(t *testing.T, s *mcp.Server, method string, params any) resp {
	t.Helper()
	p, _ := json.Marshal(params)
	msg, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": json.RawMessage(p)})
	var out bytes.Buffer
	if err := s.Serve(context.Background(), bytes.NewReader(append(msg, '\n')), &out); err != nil {
		t.Fatal(err)
	}
	var r resp
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	return r
}

func tool(t *testing.T, s *mcp.Server, name string, args any) resp {
	return call(t, s, "tools/call", map[string]any{"name": name, "arguments": args})
}

func names(r resp) string {
	var n []string
	for _, t := range r.Result.Tools {
		n = append(n, t.Name)
	}
	return strings.Join(n, ",")
}

func TestReadOnlyByDefault(t *testing.T) {
	s, audit := newServer(t, "")
	if r := call(t, s, "initialize", map[string]any{"protocolVersion": "2025-03-26"}); r.Result.Proto != "2025-03-26" {
		t.Fatalf("protocol %q", r.Result.Proto)
	}
	if got := names(call(t, s, "tools/list", nil)); got != "feed,status,get_sensor_history,list_scenarios" {
		t.Fatalf("tools: %s", got)
	}
	for _, w := range []string{"ack", "escalate", "simulate"} {
		r := tool(t, s, w, map[string]any{"id": "NF-X", "scenario": "01", "note": "x"})
		if !r.Result.IsError || !strings.Contains(r.Result.Content[0].Text, "-allow-write "+w) {
			t.Fatalf("%s should be refused: %+v", w, r.Result)
		}
	}
	if !strings.Contains(audit.String(), "refused") {
		t.Fatal("refused write attempts should be audited")
	}
	if _, err := mcp.ParseAllowWrite("all"); err == nil {
		t.Fatal("there is no 'all' shortcut")
	}
}

func TestWriteToolsEnabledOneByOne(t *testing.T) {
	s, audit := newServer(t, "simulate,escalate")
	if got := names(call(t, s, "tools/list", nil)); got != "feed,status,get_sensor_history,list_scenarios,escalate,simulate" {
		t.Fatalf("tools: %s", got)
	}
	r := tool(t, s, "simulate", map[string]any{"scenario": "04"})
	if r.Result.IsError {
		t.Fatal(r.Result.Content[0].Text)
	}
	feed := tool(t, s, "feed", map[string]any{})
	alerts := feed.Result.Struct["alerts"].([]any)
	id := alerts[0].(map[string]any)["id"].(string)
	if r := tool(t, s, "ack", map[string]any{"id": id}); !r.Result.IsError {
		t.Fatal("ack was not enabled")
	}
	if r := tool(t, s, "escalate", map[string]any{"id": id}); !r.Result.IsError {
		t.Fatal("escalate without a note must be refused")
	}
	if r := tool(t, s, "escalate", map[string]any{"id": id, "note": "operator says it's getting worse"}); r.Result.IsError {
		t.Fatal(r.Result.Content[0].Text)
	}
	st := tool(t, s, "status", map[string]any{"id": id})
	if st.Result.Struct["status"] != "escalated" || !strings.Contains(st.Result.Content[0].Text, "mcp:test-agent") {
		t.Fatalf("status %v", st.Result.Struct["status"])
	}
	if !strings.Contains(audit.String(), `"mcp_write":"escalate"`) || !strings.Contains(audit.String(), `"mcp_write":"simulate"`) {
		t.Fatalf("audit log: %s", audit.String())
	}
	h := tool(t, s, "get_sensor_history", map[string]any{"equipment_id": "ETCH-12", "sensor_id": "chamber_pressure"})
	if h.Result.IsError || len(h.Result.Struct["windows"].([]any)) != 12 {
		t.Fatalf("history: %v", h.Result.Content[0].Text[:200])
	}
}

func TestNoToolCanUnflagOrPickTheAction(t *testing.T) {
	s, _ := newServer(t, "ack,escalate,simulate")
	for _, tl := range call(t, s, "tools/list", nil).Result.Tools {
		for _, bad := range []string{"threshold", "unflag", "set_action", "route", "reclassify", "config"} {
			if strings.Contains(tl.Name, bad) {
				t.Fatalf("tool %s must not exist", tl.Name)
			}
		}
	}
	if r := tool(t, s, "ack", map[string]any{"id": "NF-1", "action": "escalate"}); !r.Result.IsError {
		t.Fatal("ack must not escalate")
	}
}

func TestSessionScoped(t *testing.T) {
	s, _ := newServer(t, "simulate")
	tool(t, s, "simulate", map[string]any{"scenario": "05"})
	id := tool(t, s, "feed", nil).Result.Struct["alerts"].([]any)[0].(map[string]any)["id"].(string)
	other := *s
	other.Session = "someone-else"
	if r := tool(t, &other, "status", map[string]any{"id": id}); !r.Result.IsError {
		t.Fatal("a server bound to another session must not read this alert")
	}
}

func TestProtocolErrors(t *testing.T) {
	s, _ := newServer(t, "")
	if r := call(t, s, "nope", nil); r.Error == nil || r.Error.Code != -32601 {
		t.Fatal("unknown method")
	}
	if r := tool(t, s, "status", map[string]any{"id": "NF-1", "extra": 1}); !r.Result.IsError {
		t.Fatal("unknown arguments are rejected")
	}
}
