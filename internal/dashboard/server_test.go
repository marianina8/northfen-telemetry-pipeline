package dashboard_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dashboard"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/dispatch"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

// syncStarter streams the whole run immediately (no pacing).
type syncStarter struct {
	svc  *pipeline.Service
	mu   sync.Mutex
	runs []store.Run
}

func (s *syncStarter) Start(ctx context.Context, run store.Run) error {
	s.mu.Lock()
	s.runs = append(s.runs, run)
	s.mu.Unlock()
	rs, err := s.svc.Readings(run)
	if err != nil {
		return err
	}
	run.TickSeconds = 0
	return s.svc.StreamLocal(ctx, run, rs, nil)
}

func setup(t *testing.T, password string) (http.Handler, *pipeline.Service, *syncStarter) {
	t.Helper()
	svc := &pipeline.Service{Cfg: config.Default(), Cat: sim.MustCatalog(), Store: store.NewMem(), Model: &explain.Mock{},
		Sink: &dispatch.PagerSink{Next: &dispatch.LogSink{}}}
	svc.SeedHistory(context.Background())
	st := &syncStarter{svc: svc}
	srv, err := dashboard.New(dashboard.Options{Svc: svc, Starter: st, Password: password, Sandboxes: password != "",
		BasePath: "/demos/northfen", AllowedOrigins: []string{"https://marian.online"}})
	if err != nil {
		t.Fatal(err)
	}
	return srv.Handler(), svc, st
}

type client struct {
	h      http.Handler
	cookie string
}

func (c *client) do(method, path, body, origin string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		if strings.HasPrefix(body, "{") {
			r.Header.Set("Content-Type", "application/json")
		} else {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if c.cookie != "" {
		r.Header.Set("Cookie", c.cookie)
	}
	w := httptest.NewRecorder()
	c.h.ServeHTTP(w, r)
	return w
}

func login(t *testing.T, h http.Handler) *client {
	c := &client{h: h}
	w := c.do("POST", "/demos/northfen/login", "password=pw&next=%2F", "https://marian.online")
	if w.Code != http.StatusSeeOther {
		t.Fatalf("login: %d", w.Code)
	}
	c.cookie = strings.Split(w.Header().Get("Set-Cookie"), ";")[0]
	return c
}

func TestAuthAndSandboxes(t *testing.T) {
	h, _, _ := setup(t, "pw")
	anon := &client{h: h}
	if w := anon.do("GET", "/demos/northfen/", "", ""); w.Code != http.StatusSeeOther {
		t.Fatalf("anonymous page: %d", w.Code)
	}
	if w := anon.do("GET", "/demos/northfen/api/runs", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous api: %d", w.Code)
	}
	if w := anon.do("GET", "/demos/northfen/static/app.css", "", ""); w.Code != 200 {
		t.Fatalf("static assets are public: %d", w.Code)
	}
	if w := anon.do("POST", "/demos/northfen/login", "password=nope", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("bad password: %d", w.Code)
	}
	a, b := login(t, h), login(t, h)
	w := a.do("POST", "/demos/northfen/api/runs", `{"scenario":"05"}`, "https://marian.online")
	if w.Code != http.StatusAccepted {
		t.Fatalf("start run: %d %s", w.Code, w.Body)
	}
	var started struct{ Run store.Run }
	json.Unmarshal(w.Body.Bytes(), &started)
	if !pipeline.IsSandbox(started.Run.SessionID) || started.Run.ExpiresAt == 0 {
		t.Fatalf("run not in a sandbox: %+v", started.Run)
	}
	var view struct {
		Sensors []struct {
			Points []any
			State  string
		}
		Alerts []struct {
			ID     string
			Action string
		}
		Counts map[string]int
	}
	w = a.do("GET", "/demos/northfen/api/runs/"+started.Run.ID, "", "")
	json.Unmarshal(w.Body.Bytes(), &view)
	if len(view.Sensors) != 4 || len(view.Sensors[0].Points) != 120 || len(view.Alerts) != 1 || view.Alerts[0].Action != "page_oncall" || view.Counts["readings"] != 480 {
		t.Fatalf("live view: %s", w.Body.String()[:300])
	}
	// visitor B sees none of it
	if w := b.do("GET", "/demos/northfen/api/runs/"+started.Run.ID, "", ""); w.Code != 404 {
		t.Fatalf("other sandbox run: %d", w.Code)
	}
	if w := b.do("GET", "/demos/northfen/alerts/"+view.Alerts[0].ID, "", ""); w.Code != 404 {
		t.Fatalf("other sandbox alert: %d", w.Code)
	}
	if w := b.do("POST", "/demos/northfen/api/alerts/"+view.Alerts[0].ID+"/ack", `{"action":"dismiss","note":"x"}`, ""); w.Code != 404 {
		t.Fatalf("other sandbox ack: %d", w.Code)
	}
	// owner acks
	w = a.do("POST", "/demos/northfen/api/alerts/"+view.Alerts[0].ID+"/ack", `{"action":"acknowledge","name":"sam"}`, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"acknowledged"`) {
		t.Fatalf("ack: %d %s", w.Code, w.Body)
	}
	if w := a.do("GET", "/demos/northfen/alerts/"+view.Alerts[0].ID, "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "dashboard:sam") {
		t.Fatalf("alert page: %d", w.Code)
	}
}

func TestCSRFGuards(t *testing.T) {
	h, _, _ := setup(t, "pw")
	c := login(t, h)
	if w := c.do("POST", "/demos/northfen/api/runs", `{"scenario":"05"}`, "https://evil.example"); w.Code != http.StatusForbidden {
		t.Fatalf("cross-site origin: %d", w.Code)
	}
	if w := c.do("POST", "/demos/northfen/api/runs", "scenario=05", ""); w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("form-encoded post: %d", w.Code)
	}
}

func TestRunLimitAndOneAtATime(t *testing.T) {
	h, svc, _ := setup(t, "pw")
	svc.Cfg.Simulate.MaxRunsPerSession = 2
	c := login(t, h)
	for i := 0; i < 2; i++ {
		if w := c.do("POST", "/demos/northfen/api/runs", `{"scenario":"01"}`, ""); w.Code != http.StatusConflict && w.Code != http.StatusAccepted {
			t.Fatalf("run %d: %d", i, w.Code)
		}
	}
	// the first run is still "streaming" by wall clock (ends ~48 s from now)
	w := c.do("POST", "/demos/northfen/api/runs", `{"scenario":"01"}`, "")
	if w.Code != http.StatusConflict && w.Code != http.StatusTooManyRequests {
		t.Fatalf("third run: %d %s", w.Code, w.Body)
	}
}

func TestLocalNoPassword(t *testing.T) {
	h, _, st := setup(t, "")
	c := &client{h: h}
	if w := c.do("GET", "/demos/northfen/", "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "Render farm console") {
		t.Fatalf("console: %d", w.Code)
	}
	if w := c.do("POST", "/demos/northfen/api/runs", `{"scenario":"nope"}`, ""); w.Code != 400 {
		t.Fatalf("bad scenario: %d", w.Code)
	}
	if w := c.do("POST", "/demos/northfen/api/runs", `{"scenario":"06"}`, ""); w.Code != 202 || st.runs[0].SessionID != "local" {
		t.Fatalf("local run: %d", w.Code)
	}
	if w := c.do("GET", "/demos/northfen", "", ""); w.Code != http.StatusMovedPermanently {
		t.Fatalf("base redirect: %d", w.Code)
	}
	w := c.do("GET", "/demos/northfen/api/scenarios", "", "")
	var sc struct{ Scenarios []struct{ Name string } }
	json.Unmarshal(w.Body.Bytes(), &sc)
	if len(sc.Scenarios) != 11 {
		t.Fatal(w.Body.String())
	}
}
