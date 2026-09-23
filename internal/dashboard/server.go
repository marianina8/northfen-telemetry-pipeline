// Package dashboard is the live anomaly console: pick a synthetic scenario,
// watch its readings stream in and get scored window by window, see the
// explanation appear once the detector flags something, and acknowledge,
// dismiss or escalate. The same handler runs locally (cmd/dashboard) and on
// Lambda behind marian.online (cmd/lambda/dashboard).
package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/detect"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

//go:embed templates/*.html static/*
var assets embed.FS

// RunStarter makes a recorded run's readings flow (locally in-process; in
// AWS by invoking the simulator Lambda, which streams into Kinesis).
type RunStarter interface {
	Start(ctx context.Context, run store.Run) error
}

// Options configures the dashboard.
type Options struct {
	Svc     *pipeline.Service
	Starter RunStarter
	// Password, when set, is required for every page (shared demo login).
	Password     string
	SecureCookie bool
	// BasePath mounts the UI under a prefix, e.g. "/demos/northfen".
	BasePath string
	// SiteURL adds an "About this demo" link (e.g. /demos on marian.online).
	SiteURL string
	// Sandboxes gives every sign-in its own empty private session.
	// Requires a Password. SandboxTTL is shown to visitors.
	Sandboxes  bool
	SandboxTTL string
	// AllowedOrigins lists extra origins whose POSTs are accepted (the site
	// that proxies the dashboard).
	AllowedOrigins []string
	// Mode is shown in the footer ("Local · offline mock" / "AWS · Bedrock").
	Mode string
	Log  *slog.Logger
}

// Server is the dashboard HTTP handler.
type Server struct {
	opt  Options
	tpl  *template.Template
	auth *auth
}

// New builds the dashboard.
func New(opt Options) (*Server, error) {
	if opt.Svc == nil || opt.Starter == nil {
		return nil, errors.New("dashboard: Svc and Starter are required")
	}
	if opt.Sandboxes && opt.Password == "" {
		return nil, errors.New("dashboard: Sandboxes requires a Password (the session carries the sandbox)")
	}
	if opt.SandboxTTL == "" {
		opt.SandboxTTL = "24 hours"
	}
	opt.BasePath = "/" + strings.Trim(opt.BasePath, "/")
	if opt.BasePath == "/" {
		opt.BasePath = ""
	}
	if opt.Log == nil {
		opt.Log = slog.Default()
	}
	funcs := template.FuncMap{
		"when": func(t time.Time) string { return t.UTC().Format("Jan 2 15:04:05 UTC") },
		"human": func(s string) string {
			switch s {
			case "page_oncall":
				return "page on-call"
			}
			return strings.ReplaceAll(s, "_", " ")
		},
		"pct": func(f float64) string { return fmt.Sprintf("%.0f%%", f*100) },
		"f2":  func(f float64) string { return fmt.Sprintf("%.2f", f) },
		"json": func(v any) string {
			b, _ := json.MarshalIndent(v, "", "  ")
			return string(b)
		},
		"upper": strings.ToUpper,
		"join":  strings.Join,
	}
	tpl, err := template.New("").Funcs(funcs).ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, err
	}
	s := &Server{opt: opt, tpl: tpl}
	if opt.Password != "" {
		s.auth = newAuth(opt.Password, opt.SecureCookie)
		s.auth.path = opt.BasePath + "/"
	}
	return s, nil
}

// Handler returns the routed, authenticated handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.console)
	mux.HandleFunc("GET /alerts/{id}", s.alertPage)
	mux.HandleFunc("GET /static/{file}", s.static)
	mux.HandleFunc("GET /api/scenarios", s.apiScenarios)
	mux.HandleFunc("GET /api/runs", s.apiRuns)
	mux.HandleFunc("POST /api/runs", s.jsonPost(s.apiStartRun))
	mux.HandleFunc("GET /api/runs/{id}", s.apiRun)
	mux.HandleFunc("GET /api/alerts/{id}", s.apiAlert)
	mux.HandleFunc("POST /api/alerts/{id}/ack", s.jsonPost(s.apiAck))
	mux.HandleFunc("GET /login", s.loginForm)
	mux.HandleFunc("POST /login", s.sameOrigin(s.login))
	mux.HandleFunc("POST /logout", s.sameOrigin(s.logout))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	h := securityHeaders(s.requireAuth(mux))
	if s.opt.BasePath == "" {
		return h
	}
	outer := http.NewServeMux()
	outer.Handle(s.opt.BasePath+"/", http.StripPrefix(s.opt.BasePath, h))
	outer.HandleFunc(s.opt.BasePath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, s.opt.BasePath+"/", http.StatusMovedPermanently)
	})
	return outer
}

func (s *Server) url(p string) string { return s.opt.BasePath + p }

// session is the pipeline session for this request: the visitor's private
// sandbox, or "local" when sandboxes are off.
func (s *Server) session(r *http.Request) string {
	if ws := workspace(r); ws != "" {
		return ws
	}
	return telemetry.LocalSession
}

// ---- pages ------------------------------------------------------------------

type page struct {
	Base, SiteURL, SandboxTTL, Company, Fab, Title, Mode string
	Sandbox, Nav, Auth                                   bool
}

func (s *Server) page(title string) page {
	return page{Base: s.opt.BasePath, SiteURL: s.opt.SiteURL, SandboxTTL: s.opt.SandboxTTL, Company: s.opt.Svc.Cfg.Company,
		Fab: s.opt.Svc.Cfg.Fab, Title: title, Mode: s.opt.Mode, Sandbox: s.opt.Sandboxes, Nav: true, Auth: s.auth != nil}
}

type consoleData struct {
	Page    page
	RunID   string
	Explain string
}

func (s *Server) console(w http.ResponseWriter, r *http.Request) {
	s.render(w, "console.html", consoleData{Page: s.page("Equipment anomaly console"), RunID: r.URL.Query().Get("run"),
		Explain: s.opt.Svc.Model.Name()})
}

type alertData struct {
	Page    page
	Alert   store.Alert
	Summary pipeline.AlertSummary
	Run     store.Run
	Tool    string
}

func (s *Server) alertPage(w http.ResponseWriter, r *http.Request) {
	a, err := s.owned(r, r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	run, _ := s.opt.Svc.Store.GetRun(r.Context(), a.SessionID, a.RunID)
	tool := a.EquipmentID
	if eq, ok := s.opt.Svc.Cat.Tool(a.EquipmentID); ok {
		tool = eq.Name
	}
	s.render(w, "alert.html", alertData{Page: s.page(a.ID), Alert: a, Summary: pipeline.Summarize(a), Run: run, Tool: tool})
}

func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("file")
	b, err := assets.ReadFile("static/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case strings.HasSuffix(name, ".js"):
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	case strings.HasSuffix(name, ".css"):
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	}
	w.Header().Set("Cache-Control", "public, max-age=300")
	w.Write(b)
}

// owned loads an alert only if it belongs to the caller's session.
func (s *Server) owned(r *http.Request, id string) (store.Alert, error) {
	a, err := s.opt.Svc.Store.GetAlert(r.Context(), id)
	if err != nil {
		return a, err
	}
	if a.SessionID != s.session(r) {
		return store.Alert{}, fmt.Errorf("alert %s: %w", id, store.ErrNotFound)
	}
	return a, nil
}

// ---- API --------------------------------------------------------------------

type scenarioDTO struct {
	Name        string `json:"name"`
	File        string `json:"file"`
	Title       string `json:"title"`
	Category    string `json:"category"`
	Description string `json:"description"`
	EquipmentID string `json:"equipment_id"`
	Tool        string `json:"tool"`
	Ticks       int    `json:"ticks"`
	Seconds     int    `json:"seconds"`
}

func (s *Server) apiScenarios(w http.ResponseWriter, _ *http.Request) {
	var out []scenarioDTO
	for _, sc := range s.opt.Svc.Cat.Scenarios {
		eq, _ := s.opt.Svc.Cat.Tool(sc.EquipmentID)
		out = append(out, scenarioDTO{Name: sc.Name, File: sc.File, Title: sc.Title, Category: sc.Category,
			Description: strings.TrimSpace(sc.Description), EquipmentID: sc.EquipmentID, Tool: eq.Name, Ticks: sc.Ticks,
			Seconds: int(math.Round(float64(sc.Ticks) * s.opt.Svc.Cfg.Simulate.TickSeconds))})
	}
	writeJSON(w, http.StatusOK, map[string]any{"scenarios": out})
}

func (s *Server) apiRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.opt.Svc.Store.ListRuns(r.Context(), s.session(r))
	if err != nil {
		s.apiFail(w, err)
		return
	}
	if runs == nil {
		runs = []store.Run{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "max_runs": s.maxRuns(r)})
}

func (s *Server) maxRuns(r *http.Request) int {
	if pipeline.IsSandbox(s.session(r)) {
		return s.opt.Svc.Cfg.Simulate.MaxRunsPerSession
	}
	return 0
}

func (s *Server) apiStartRun(w http.ResponseWriter, r *http.Request, body []byte) {
	var req struct {
		Scenario string `json:"scenario"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Scenario == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pick a scenario"})
		return
	}
	// One run at a time per session keeps the live view readable (and the
	// public demo cheap).
	if runs, err := s.opt.Svc.Store.ListRuns(r.Context(), s.session(r)); err == nil {
		for _, run := range runs {
			if time.Now().Before(run.EndsAt().Add(2 * time.Second)) {
				writeJSON(w, http.StatusConflict, map[string]any{"error": "a scenario is still streaming - wait for it to finish", "run": run})
				return
			}
		}
	}
	run, _, err := s.opt.Svc.StartRun(r.Context(), pipeline.RunRequest{SessionID: s.session(r), Scenario: req.Scenario, Source: "dashboard"})
	switch {
	case errors.Is(err, pipeline.ErrRunLimit):
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": fmt.Sprintf("This sandbox has used its %d scenario runs. Sign out and back in for a fresh one.", s.opt.Svc.Cfg.Simulate.MaxRunsPerSession)})
		return
	case err != nil:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.opt.Starter.Start(context.WithoutCancel(r.Context()), run); err != nil {
		s.apiFail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"run": run})
}

// Live view payload.

type pointDTO struct {
	T int      `json:"t"`
	V *float64 `json:"v"`
	Z *float64 `json:"z,omitempty"`
	B float64  `json:"b,omitempty"` // baseline
	S float64  `json:"s,omitempty"` // sigma
	R []string `json:"r,omitempty"` // rules active
}

type sensorDTO struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Unit     string         `json:"unit"`
	Decimals int            `json:"decimals"`
	Points   []pointDTO     `json:"points"`
	Windows  []store.Window `json:"windows"`
	Flags    []detect.Flag  `json:"flags"`
	State    string         `json:"state"` // warmup | normal | anomaly | sensor_fault
}

type alertDTO struct {
	pipeline.AlertSummary
	Flags       []store.SeriesFlag   `json:"flags"`
	Explanation any                  `json:"explanation,omitempty"`
	ExplainErr  string               `json:"explain_error,omitempty"`
	Decision    *store.Decision      `json:"decision,omitempty"`
	Actions     []store.ActionRecord `json:"actions,omitempty"`
	LastEvent   store.Event          `json:"last_event"`
}

type runView struct {
	Run       store.Run          `json:"run"`
	Tool      string             `json:"tool"`
	ToolType  string             `json:"tool_type"`
	Line      string             `json:"line"`
	Tick      int                `json:"tick"` // last tick scored
	Streaming bool               `json:"streaming"`
	Settling  bool               `json:"settling"` // alerts still waiting on the model
	Sensors   []sensorDTO        `json:"sensors"`
	Alerts    []alertDTO         `json:"alerts"`
	Counts    map[string]int     `json:"counts"`
	Detector  map[string]float64 `json:"detector"`
}

func (s *Server) apiRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	svc := s.opt.Svc
	run, err := svc.Store.GetRun(ctx, s.session(r), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such run in this session"})
		return
	}
	if err != nil {
		s.apiFail(w, err)
		return
	}
	eq, _ := svc.Cat.Tool(run.EquipmentID)
	v := runView{Run: run, Tool: eq.Name, ToolType: eq.ToolType, Line: eq.Line, Tick: -1, Counts: map[string]int{},
		Detector: map[string]float64{"sustained_z": svc.Cfg.Detector.SustainedZ, "spike_z": svc.Cfg.Detector.SpikeZ,
			"warmup": float64(svc.Cfg.Detector.Warmup), "window": float64(svc.Cfg.Detector.WindowSize)}}
	ws, err := svc.Store.ListWindows(ctx, run.SessionID, run.ID)
	if err != nil {
		s.apiFail(w, err)
		return
	}
	bySensor := map[string][]store.Window{}
	for _, w := range ws {
		bySensor[w.SensorID] = append(bySensor[w.SensorID], w)
		v.Counts["windows"]++
		if w.Status == "flagged" || w.Status == "fault" {
			v.Counts["windows_flagged"]++
		}
	}
	for _, sn := range eq.Sensors {
		rs, err := svc.Store.Readings(ctx, run.SessionID, run.EquipmentID, sn.ID, run.StartedAt, run.EndsAt().Add(time.Minute))
		if err != nil {
			s.apiFail(w, err)
			return
		}
		var mine []telemetry.Reading
		for _, rd := range rs {
			if rd.RunID == run.ID {
				mine = append(mine, rd)
			}
		}
		sort.Slice(mine, func(i, j int) bool { return mine[i].Tick < mine[j].Tick })
		dto := sensorDTO{ID: sn.ID, Type: sn.Type, Unit: sn.Unit, Decimals: sn.Decimals, Windows: bySensor[sn.ID], Points: []pointDTO{}, Flags: []detect.Flag{}, State: "warmup"}
		if dto.Windows == nil {
			dto.Windows = []store.Window{}
		}
		// Replay the same deterministic detector over the stored readings to
		// draw z-scores and the baseline band (identical to what the
		// consumer computed - it is pure statistics).
		for _, se := range detect.ScoreAll(svc.Cfg.Detector, mine, true) {
			dto.Flags = append(dto.Flags, se.Flags...)
			for _, p := range se.Points {
				dto.Points = append(dto.Points, pointDTO{T: p.Tick, V: p.Value, Z: p.Z, B: p.Baseline, S: p.Sigma, R: p.Rules})
			}
			switch {
			case se.State.InEpisode:
				dto.State = se.State.Episode
			case se.State.Ready:
				dto.State = "normal"
			}
		}
		if n := len(dto.Points); n > 0 {
			v.Tick = max(v.Tick, dto.Points[n-1].T)
		}
		v.Counts["readings"] += len(mine)
		v.Sensors = append(v.Sensors, dto)
	}
	as, err := svc.RunAlerts(ctx, run.SessionID, run.ID)
	if err != nil {
		s.apiFail(w, err)
		return
	}
	for _, a := range as {
		v.Alerts = append(v.Alerts, toDTO(a))
		if a.Explains > 0 {
			v.Counts["model_calls"] += a.Explains
		}
		v.Counts["actions"] += len(a.Actions)
		if svc.NeedsResolve(a) {
			v.Settling = true
		}
	}
	if v.Alerts == nil {
		v.Alerts = []alertDTO{}
	}
	v.Streaming = v.Tick < run.Ticks-1 && time.Now().Before(run.EndsAt().Add(90*time.Second))
	writeJSON(w, http.StatusOK, v)
}

func toDTO(a store.Alert) alertDTO {
	d := alertDTO{AlertSummary: pipeline.Summarize(a), Flags: a.Flags, ExplainErr: a.ExplainErr, Decision: a.Decision, Actions: a.Actions}
	if a.Explanation != nil && a.Explain == store.ExplainDone {
		d.Explanation = a.Explanation
	}
	if len(a.Events) > 0 {
		d.LastEvent = a.Events[len(a.Events)-1]
	}
	return d
}

func (s *Server) apiAlert(w http.ResponseWriter, r *http.Request) {
	a, err := s.owned(r, r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		s.apiFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) apiAck(w http.ResponseWriter, r *http.Request, body []byte) {
	var req struct {
		Action string `json:"action"`
		Note   string `json:"note"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = "engineer"
	}
	if len(name) > 40 {
		name = name[:40]
	}
	a, err := s.opt.Svc.Ack(r.Context(), pipeline.AckRequest{AlertID: r.PathValue("id"), Verb: req.Action, Note: req.Note,
		Actor: "dashboard:" + name, SessionID: s.session(r)})
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	case err != nil && a.ID == "":
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case err != nil:
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusOK, toDTO(a))
	}
}

// ---- plumbing ---------------------------------------------------------------

// jsonPost guards state-changing API calls: same-origin (or an allowed
// origin) and a JSON body - a cross-site form can't send application/json
// without a CORS preflight, which this server never grants.
func (s *Server) jsonPost(h func(http.ResponseWriter, *http.Request, []byte)) http.HandlerFunc {
	return s.sameOrigin(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "send application/json"})
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unreadable body"})
			return
		}
		h(w, r, body)
	})
}

func (s *Server) sameOrigin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" && !s.originAllowed(o, r.Host) {
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
		h(w, r)
	}
}

func (s *Server) originAllowed(origin, host string) bool {
	if u, err := url.Parse(origin); err == nil && u.Host == host {
		return true
	}
	for _, a := range s.opt.AllowedOrigins {
		if strings.EqualFold(strings.TrimRight(strings.TrimSpace(a), "/"), origin) {
			return true
		}
	}
	return false
}

func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'")
		if w.Header().Get("Cache-Control") == "" {
			w.Header().Set("Cache-Control", "no-store")
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	s.renderStatus(w, http.StatusOK, name, data)
}

func (s *Server) renderStatus(w http.ResponseWriter, code int, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	if err := s.tpl.ExecuteTemplate(w, name, data); err != nil {
		s.opt.Log.Error("template", "name", name, "err", err)
	}
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.opt.Log.Error("dashboard request failed", "err", err)
	http.Error(w, "Something went wrong. Please try again.", http.StatusInternalServerError)
}

func (s *Server) apiFail(w http.ResponseWriter, err error) {
	s.opt.Log.Error("dashboard api failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "something went wrong - please try again"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
