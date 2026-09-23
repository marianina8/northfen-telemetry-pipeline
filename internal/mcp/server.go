// Package mcp exposes the pipeline's CLI operations as MCP (Model Context
// Protocol) tools so an AI agent can work the anomaly feed the way an
// on-call engineer does with the CLI.
//
// Safety model (same split as the Rivergate and Amberlight demos):
//   - Read tools (feed, status, get_sensor_history, list_scenarios) are
//     always available.
//   - Write tools (ack, escalate, simulate) are OFF by default. Each must be
//     enabled by name (northfen mcp -allow-write ack,escalate). There is no
//     "all" shortcut. simulate is a write tool because it injects data.
//   - Disabled write tools are not advertised in tools/list, and calling one
//     anyway returns an error: no implicit write access.
//   - Every write call is recorded in the alert's audit trail as
//     mcp:<actor>, and logged as a JSON line to the server's audit log.
//   - There is no tool that marks something "not anomalous", changes a
//     detector threshold, or picks a dispatch action: detection and dispatch
//     stay deterministic. An agent can acknowledge, escalate (only ever
//     raises the response) or dismiss with a mandatory note.
//   - The server is bound to one session; it can't read other sessions.
//
// Small, dependency-free MCP stdio transport (newline-delimited JSON-RPC
// 2.0): initialize, ping, tools/list, tools/call.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// Supported MCP protocol revisions, newest first.
var protocolVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

// Server is an MCP server over the pipeline service.
type Server struct {
	Svc        *pipeline.Service
	AllowWrite map[string]bool // write tools explicitly enabled
	Actor      string          // audit-trail name for write calls
	Session    string          // the one session this server can see (default local)
	Version    string
	Audit      io.Writer // JSON line per write call (stderr in the CLI)

	outMu   sync.Mutex
	auditMu sync.Mutex
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	codeParse          = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// Serve reads newline-delimited JSON-RPC from in and writes responses to out.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	enc := json.NewEncoder(out)
	for sc.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if resp := s.Handle(ctx, []byte(line)); resp != nil {
			s.outMu.Lock()
			err := enc.Encode(resp)
			s.outMu.Unlock()
			if err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// Handle processes one JSON-RPC message (nil response for notifications).
func (s *Server) Handle(ctx context.Context, msg []byte) *response {
	var req request
	if err := json.Unmarshal(msg, &req); err != nil {
		return &response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{codeParse, "parse error: " + err.Error()}}
	}
	isNotification := len(req.ID) == 0
	if req.JSONRPC != "2.0" || req.Method == "" {
		if isNotification {
			return nil
		}
		return &response{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{codeInvalidRequest, "invalid request"}}
	}
	result, rerr := s.dispatch(ctx, req)
	if isNotification {
		return nil
	}
	resp := &response{JSONRPC: "2.0", ID: req.ID}
	if rerr != nil {
		resp.Error = rerr
	} else {
		resp.Result = result
	}
	return resp
}

func (s *Server) dispatch(ctx context.Context, req request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		v := protocolVersions[0]
		for _, pv := range protocolVersions {
			if pv == p.ProtocolVersion {
				v = pv
			}
		}
		return map[string]any{
			"protocolVersion": v,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "northfen-telemetry", "version": s.Version},
			"instructions": "Northfen Semiconductor fab-equipment anomaly pipeline (synthetic data). Anomaly detection is " +
				"deterministic statistics; a model only explains windows the detector already flagged, and a fixed table " +
				"decides log / ticket / page. Read tools are always available. Write tools (ack, escalate, simulate) are " +
				"enabled one by one by the operator. You cannot change thresholds, un-flag a window, or choose the " +
				"dispatch action; escalate only ever raises the response, and dismiss requires a note.",
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "notifications/initialized", "notifications/cancelled":
		return nil, nil
	case "tools/list":
		return map[string]any{"tools": s.listTools()}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || p.Name == "" {
			return nil, &rpcError{codeInvalidParams, "tools/call needs a tool name"}
		}
		return s.callTool(ctx, p.Name, p.Arguments)
	default:
		return nil, &rpcError{codeMethodNotFound, "method not found: " + req.Method}
	}
}

// ---- tools -----------------------------------------------------------------

type tool struct {
	Name        string         `json:"name"`
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
	write       bool
	run         func(ctx context.Context, s *Server, args json.RawMessage) (any, error)
}

func obj(props map[string]any, required ...string) map[string]any {
	m := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }

var alertArg = str("Alert ID, e.g. NF-1A2B3C4D")

var readOnly = map[string]any{"readOnlyHint": true}

var tools = []tool{
	{
		Name: "feed", Title: "Anomaly feed",
		Description: "The anomaly feed for this session, newest first: tool, flagged sensors, detector rules, the explanation's severity/confidence/top cause, the dispatch action, and the human status. Optionally filter by status (open, acknowledged, escalated, dismissed) or equipment.",
		InputSchema: obj(map[string]any{"status": str("Filter by status"), "equipment_id": str("Filter by tool, e.g. CMP-07")}),
		Annotations: readOnly,
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct {
				Status      string `json:"status"`
				EquipmentID string `json:"equipment_id"`
			}
			if err := decode(raw, &a); err != nil {
				return nil, errArgs(err.Error())
			}
			rows, err := s.Svc.Feed(ctx, s.session())
			if err != nil {
				return nil, err
			}
			out := []pipeline.AlertSummary{}
			for _, r := range rows {
				if (a.Status == "" || r.Status == a.Status) && (a.EquipmentID == "" || r.EquipmentID == a.EquipmentID) {
					out = append(out, r)
				}
			}
			return map[string]any{"count": len(out), "alerts": out}, nil
		},
	},
	{
		Name: "status", Title: "Alert detail",
		Description: "Everything about one alert: the detector flags (rule, tick, z-score), exactly what the model was shown, the structured explanation (likely causes, recommended checks, severity, confidence), the dispatch rule that fired, actions taken, and the full timestamped audit trail.",
		InputSchema: obj(map[string]any{"id": alertArg}, "id"),
		Annotations: readOnly,
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct {
				ID string `json:"id"`
			}
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			return s.alert(ctx, a.ID)
		},
	},
	{
		Name: "get_sensor_history", Title: "Sensor history",
		Description: "One sensor's raw readings and every scored window (mean, min/max, max |z|, rules, status) for a run. Defaults to the latest run on that tool.",
		InputSchema: obj(map[string]any{
			"equipment_id": str("Tool ID, e.g. ETCH-12"), "sensor_id": str("Sensor ID, e.g. chamber_pressure"), "run_id": str("Optional run ID"),
		}, "equipment_id", "sensor_id"),
		Annotations: readOnly,
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct {
				EquipmentID string `json:"equipment_id"`
				SensorID    string `json:"sensor_id"`
				RunID       string `json:"run_id"`
			}
			if err := decode(raw, &a); err != nil || a.EquipmentID == "" || a.SensorID == "" {
				return nil, errArgs("equipment_id and sensor_id are required")
			}
			if a.RunID == "" {
				runs, err := s.Svc.Store.ListRuns(ctx, s.session())
				if err != nil {
					return nil, err
				}
				sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.After(runs[j].StartedAt) })
				for _, r := range runs {
					if r.EquipmentID == a.EquipmentID {
						a.RunID = r.ID
						break
					}
				}
				if a.RunID == "" {
					return nil, fmt.Errorf("no runs on %s in this session: %w", a.EquipmentID, store.ErrNotFound)
				}
			}
			return s.Svc.SensorHistory(ctx, s.session(), a.RunID, a.EquipmentID, a.SensorID)
		},
	},
	{
		Name: "list_scenarios", Title: "List scenarios",
		Description: "The synthetic sensor-stream scenarios (normal baselines, gradual drift, spikes, noisy-but-normal, stuck and dropped-out sensors, a threshold-boundary case, correlated multi-sensor drift) and the tools they run on.",
		InputSchema: obj(map[string]any{}),
		Annotations: readOnly,
		run: func(_ context.Context, s *Server, _ json.RawMessage) (any, error) {
			type row struct{ Name, File, Title, Category, EquipmentID string }
			var out []row
			for _, sc := range s.Svc.Cat.Scenarios {
				out = append(out, row{sc.Name, sc.File, sc.Title, sc.Category, sc.EquipmentID})
			}
			return map[string]any{"scenarios": out}, nil
		},
	},
	{
		Name: "ack", Title: "Acknowledge or dismiss an alert", write: true,
		Description: "Record a human-side decision on an alert: acknowledge it (someone is on it), or dismiss it (requires a note saying why). Recorded in the audit trail as mcp:<actor>. Only enable this for an agent acting on instructions from the on-call engineer.",
		InputSchema: obj(map[string]any{
			"id":     alertArg,
			"action": map[string]any{"type": "string", "enum": []string{"acknowledge", "dismiss"}, "description": "Default acknowledge"},
			"note":   str("Why - required for dismiss; recorded in the audit trail"),
		}, "id"),
		Annotations: map[string]any{"readOnlyHint": false, "destructiveHint": false},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ ID, Action, Note string }
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			if a.Action == "" {
				a.Action = pipeline.VerbAcknowledge
			}
			if v, err := pipeline.ParseVerb(a.Action); err != nil || v == pipeline.VerbEscalate {
				return nil, errArgs("action must be acknowledge or dismiss (use the escalate tool to escalate)")
			}
			return s.ack(ctx, a.ID, a.Action, a.Note)
		},
	},
	{
		Name: "escalate", Title: "Escalate an alert", write: true,
		Description: "Escalate an alert to on-call: pages if no page has been sent yet (never twice). Escalation only ever raises the response. Requires a note.",
		InputSchema: obj(map[string]any{"id": alertArg, "note": str("Why - recorded in the audit trail")}, "id", "note"),
		Annotations: map[string]any{"readOnlyHint": false, "destructiveHint": false, "idempotentHint": true},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ ID, Note string }
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			if strings.TrimSpace(a.Note) == "" {
				return nil, errArgs("note is required so the audit trail explains the escalation")
			}
			return s.ack(ctx, a.ID, pipeline.VerbEscalate, a.Note)
		},
	},
	{
		Name: "simulate", Title: "Run a synthetic scenario", write: true,
		Description: "Inject a named synthetic sensor-stream scenario into this session and score it end to end (detect, explain flagged windows, dispatch). Writes synthetic data, so it is a write tool.",
		InputSchema: obj(map[string]any{"scenario": str("Scenario name or number, e.g. 10 or cmp07-correlated-drift")}, "scenario"),
		Annotations: map[string]any{"readOnlyHint": false, "destructiveHint": false, "openWorldHint": false},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ Scenario string }
			if err := decode(raw, &a); err != nil || a.Scenario == "" {
				return nil, errArgs("scenario is required")
			}
			run, as, err := s.Svc.RunBatch(ctx, pipeline.RunRequest{SessionID: s.session(), Scenario: a.Scenario, Source: "mcp:" + s.actorName(), TickSeconds: 1}, 100)
			if err != nil {
				return nil, err
			}
			out := []pipeline.AlertSummary{}
			for _, al := range as {
				out = append(out, pipeline.Summarize(al))
			}
			return map[string]any{"run": run, "alerts": out}, nil
		},
	},
}

func (s *Server) session() string {
	if s.Session == "" {
		return telemetry.LocalSession
	}
	return s.Session
}

func (s *Server) actorName() string {
	if s.Actor == "" {
		return "agent"
	}
	return s.Actor
}

func (s *Server) alert(ctx context.Context, id string) (store.Alert, error) {
	a, err := s.Svc.Store.GetAlert(ctx, id)
	if err != nil {
		return a, err
	}
	if a.SessionID != s.session() {
		return store.Alert{}, fmt.Errorf("alert %s: %w", id, store.ErrNotFound)
	}
	return a, nil
}

func (s *Server) ack(ctx context.Context, id, verb, note string) (any, error) {
	a, err := s.Svc.Ack(ctx, pipeline.AckRequest{AlertID: id, Verb: verb, Actor: "mcp:" + s.actorName(), Note: note, SessionID: s.session()})
	if err != nil {
		return nil, err
	}
	return map[string]any{"alert": pipeline.Summarize(a), "last_event": a.Events[len(a.Events)-1]}, nil
}

// WriteToolNames lists the tools that require -allow-write.
func WriteToolNames() []string {
	var out []string
	for _, t := range tools {
		if t.write {
			out = append(out, t.Name)
		}
	}
	return out
}

// ParseAllowWrite validates a comma-separated list of write tool names.
func ParseAllowWrite(spec string) (map[string]bool, error) {
	out := map[string]bool{}
	valid := map[string]bool{}
	for _, n := range WriteToolNames() {
		valid[n] = true
	}
	for _, n := range strings.Split(spec, ",") {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		if !valid[n] {
			return nil, fmt.Errorf("unknown write tool %q (valid: %s)", n, strings.Join(WriteToolNames(), ", "))
		}
		out[n] = true
	}
	return out, nil
}

func (s *Server) enabled(t tool) bool { return !t.write || s.AllowWrite[t.Name] }

func (s *Server) listTools() []tool {
	var out []tool
	for _, t := range tools {
		if s.enabled(t) {
			out = append(out, t)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return !out[i].write && out[j].write })
	return out
}

type argsError struct{ msg string }

func (e argsError) Error() string { return e.msg }
func errArgs(m string) error      { return argsError{m} }

func decode(raw json.RawMessage, v any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (s *Server) audit(name string, args json.RawMessage, err error) {
	if s.Audit == nil {
		return
	}
	rec := map[string]any{"at": time.Now().UTC(), "mcp_write": name, "actor": "mcp:" + s.actorName(), "session": s.session(), "args": args, "ok": err == nil}
	if err != nil {
		rec["error"] = err.Error()
	}
	b, _ := json.Marshal(rec)
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	fmt.Fprintln(s.Audit, string(b))
}

// callTool runs a tool. Tool failures are returned as isError results.
func (s *Server) callTool(ctx context.Context, name string, args json.RawMessage) (any, *rpcError) {
	var t *tool
	for i := range tools {
		if tools[i].Name == name {
			t = &tools[i]
		}
	}
	if t == nil {
		return nil, &rpcError{codeInvalidParams, "unknown tool: " + name}
	}
	if !s.enabled(*t) {
		s.audit(name+" (refused: not enabled)", args, errors.New("not enabled"))
		return toolError(fmt.Sprintf("tool %q is a write tool and is not enabled on this server. "+
			"An operator must start the server with -allow-write %s.", name, name)), nil
	}
	out, err := t.run(ctx, s, args)
	if t.write {
		s.audit(name, args, err)
	}
	if err != nil {
		var ae argsError
		if errors.As(err, &ae) {
			return toolError("invalid arguments: " + err.Error()), nil
		}
		return toolError(err.Error()), nil
	}
	return toolResult(out), nil
}

func toolResult(v any) map[string]any {
	b, _ := json.MarshalIndent(v, "", "  ")
	r := map[string]any{"content": []map[string]any{{"type": "text", "text": string(b)}}}
	var m map[string]any
	if json.Unmarshal(b, &m) == nil {
		r["structuredContent"] = m
	}
	return r
}

func toolError(msg string) map[string]any {
	return map[string]any{"content": []map[string]any{{"type": "text", "text": msg}}, "isError": true}
}
