package awsapp

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// TokenHeader carries the webhook shared secret.
const TokenHeader = "X-Northfen-Token"

// MaxWebhookReadings bounds one POST /readings.
const MaxWebhookReadings = 500

// Webhook is the API Gateway entry point for callers that aren't a Kinesis
// producer (public site visitors, curl, other systems):
//
//	POST /simulate  {"scenario": "04", "session_id": "optional"}  -> starts a streamed run
//	POST /readings  [reading, ...] or {"readings": [...]}          -> puts raw readings on the stream
//	GET  /healthz
//
// Both POSTs require the shared secret in X-Northfen-Token.
type Webhook struct {
	Svc      *pipeline.Service
	Starter  RunStarter
	Producer Producer
	Token    func(ctx context.Context) (string, error)
	Log      *slog.Logger
}

// ServeHTTP implements http.Handler.
func (h *Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		path = path[i:] // tolerate a stage prefix (/demo/simulate)
	}
	if path == "/healthz" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if r.Method != http.MethodPost || (path != "/simulate" && path != "/readings") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "POST /simulate or POST /readings"})
		return
	}
	if err := h.authorize(r); err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unreadable body"})
		return
	}
	if path == "/simulate" {
		h.simulate(w, r.Context(), body)
		return
	}
	h.readings(w, r.Context(), body)
}

func (h *Webhook) authorize(r *http.Request) error {
	if h.Token == nil {
		return errors.New("webhook secret not configured")
	}
	want, err := h.Token(r.Context())
	if err != nil {
		logger(h.Log).Error("webhook secret unavailable", "err", err)
		return errors.New("webhook secret unavailable")
	}
	got := r.Header.Get(TokenHeader)
	a, b := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(want))
	if got == "" || subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
		return fmt.Errorf("missing or wrong %s header", TokenHeader)
	}
	return nil
}

func (h *Webhook) simulate(w http.ResponseWriter, ctx context.Context, body []byte) {
	var req struct {
		Scenario  string `json:"scenario"`
		SessionID string `json:"session_id"`
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || req.Scenario == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": `body must be {"scenario": "<name or number>", "session_id": "optional"}`})
		return
	}
	if req.SessionID == "" {
		req.SessionID = "webhook"
	}
	run, _, err := h.Svc.StartRun(ctx, pipeline.RunRequest{SessionID: req.SessionID, Scenario: req.Scenario, Source: "webhook"})
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, pipeline.ErrRunLimit) {
			status = http.StatusTooManyRequests
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	if err := h.Starter.Start(ctx, run); err != nil {
		logger(h.Log).Error("start run", "run", run.ID, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not start the simulator"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id": run.ID, "session_id": run.SessionID, "scenario": run.Scenario, "equipment_id": run.EquipmentID,
		"ticks": run.Ticks, "tick_seconds": run.TickSeconds, "ends_at": run.EndsAt(),
		"next": fmt.Sprintf("northfen feed -session %s  (or the MCP feed tool) once the run has streamed", run.SessionID),
	})
}

func (h *Webhook) readings(w http.ResponseWriter, ctx context.Context, body []byte) {
	var rs []telemetry.Reading
	trimmed := strings.TrimSpace(string(body))
	var err error
	if strings.HasPrefix(trimmed, "[") {
		err = json.Unmarshal(body, &rs)
	} else {
		var wrapped struct {
			Readings []telemetry.Reading `json:"readings"`
		}
		err = json.Unmarshal(body, &wrapped)
		rs = wrapped.Readings
	}
	if err != nil || len(rs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be a JSON array of readings or {\"readings\": [...]}"})
		return
	}
	if len(rs) > MaxWebhookReadings {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": fmt.Sprintf("at most %d readings per request", MaxWebhookReadings)})
		return
	}
	for i := range rs {
		if rs[i].SessionID == "" {
			rs[i].SessionID = "webhook"
		}
		if pipeline.IsSandbox(rs[i].SessionID) {
			// visitor sandboxes are only written by the dashboard
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "session ids starting with 'v' + 16 hex are reserved for dashboard sandboxes"})
			return
		}
		if err := rs[i].Validate(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("reading %d: %v", i, err)})
			return
		}
	}
	if err := h.Producer.Put(ctx, rs); err != nil {
		logger(h.Log).Error("put readings", "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not write to the stream"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": len(rs)})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
