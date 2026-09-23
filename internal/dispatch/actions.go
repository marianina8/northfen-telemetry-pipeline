package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
)

// Sink performs one action for an alert and says what it did.
type Sink interface {
	Do(ctx context.Context, a *store.Alert, action string, sandbox bool) (store.ActionRecord, error)
}

// Message is the payload every action carries (log line, outbox entry,
// ticket body, page text).
type Message struct {
	Action      string   `json:"action"`
	AlertID     string   `json:"alert_id"`
	SessionID   string   `json:"session_id"`
	EquipmentID string   `json:"equipment_id"`
	Kind        string   `json:"kind"`
	Sensors     []string `json:"sensors"`
	Severity    string   `json:"severity,omitempty"`
	Confidence  float64  `json:"confidence,omitempty"`
	Summary     string   `json:"summary"`
	Checks      []string `json:"recommended_checks,omitempty"`
	Rule        string   `json:"dispatch_rule,omitempty"`
	Reason      string   `json:"dispatch_reason,omitempty"`
}

// NewMessage builds the payload, including the reasoning that triggered it.
func NewMessage(a *store.Alert, action string) Message {
	m := Message{Action: action, AlertID: a.ID, SessionID: a.SessionID, EquipmentID: a.EquipmentID, Kind: a.Kind}
	for _, f := range a.Flags {
		m.Sensors = append(m.Sensors, f.SensorID+" ("+f.Rule+")")
	}
	if e := a.Explanation; e != nil {
		m.Severity, m.Confidence, m.Summary, m.Checks = e.Severity, e.Confidence, e.Explanation, e.RecommendedChecks
	} else if a.Kind == "sensor_fault" {
		m.Summary = fmt.Sprintf("Sensor fault on %s: %s. The sensor itself looks broken; no model call.", a.EquipmentID, strings.Join(m.Sensors, ", "))
	} else {
		m.Summary = "No explanation available: " + a.ExplainErr
	}
	if d := a.Decision; d != nil {
		m.Rule, m.Reason = d.Rule, d.Reason
	}
	return m
}

// LogSink records every action as a structured "nf_action" log line (in AWS
// that's CloudWatch). log_only and open_ticket are handled entirely here;
// open_ticket gets a stub ticket ID (plug a real tracker in behind Sink).
type LogSink struct {
	Log *slog.Logger
	Now func() time.Time
}

// Do implements Sink.
func (s *LogSink) Do(_ context.Context, a *store.Alert, action string, sandbox bool) (store.ActionRecord, error) {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	l := s.Log
	if l == nil {
		l = slog.Default()
	}
	m := NewMessage(a, action)
	l.Info("nf_action", "action", action, "alert_id", a.ID, "session", a.SessionID, "equipment", a.EquipmentID,
		"kind", a.Kind, "severity", m.Severity, "confidence", m.Confidence, "rule", m.Rule, "reason", m.Reason, "summary", m.Summary)
	rec := store.ActionRecord{Action: action, At: now().UTC()}
	switch action {
	case config.ActionOpenTicket:
		rec.Ref = "TKT-" + strings.TrimPrefix(a.ID, "NF-")
		rec.Detail = "Ticket " + rec.Ref + " opened for the equipment owner (stub tracker)"
	case config.ActionLogOnly:
		rec.Detail = "Logged; no one is interrupted"
	default:
		rec.Detail = "Logged"
	}
	return rec, nil
}

// OutboxSink appends each action to <dir>/outbox.jsonl (local stand-in for
// tickets and pages), then delegates.
type OutboxSink struct {
	Dir  string
	Next Sink
	mu   sync.Mutex
}

// Do implements Sink.
func (s *OutboxSink) Do(ctx context.Context, a *store.Alert, action string, sandbox bool) (store.ActionRecord, error) {
	rec, err := s.Next.Do(ctx, a, action, sandbox)
	if err != nil {
		return rec, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return rec, err
	}
	f, err := os.OpenFile(filepath.Join(s.Dir, "outbox.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return rec, err
	}
	defer f.Close()
	line := struct {
		At time.Time `json:"at"`
		Message
		Record store.ActionRecord `json:"record"`
	}{rec.At, NewMessage(a, action), rec}
	b, _ := json.Marshal(line)
	_, err = f.Write(append(b, '\n'))
	return rec, err
}

// SNSAPI is the one SNS call used (fakeable).
type SNSAPI interface {
	Publish(ctx context.Context, in *sns.PublishInput, opts ...func(*sns.Options)) (*sns.PublishOutput, error)
}

// PagerSink sends page_oncall to SNS; everything else goes to Next.
// Pages from public sandbox sessions are recorded as simulated unless
// config allows them (dispatch.page_sandbox_sessions). With no topic
// configured, pages are simulated too.
type PagerSink struct {
	SNS           SNSAPI
	TopicARN      string
	PageSandboxes bool
	Next          Sink
}

// Do implements Sink.
func (s *PagerSink) Do(ctx context.Context, a *store.Alert, action string, sandbox bool) (store.ActionRecord, error) {
	rec, err := s.Next.Do(ctx, a, action, sandbox)
	if err != nil || action != config.ActionPageOnCall {
		return rec, err
	}
	switch {
	case sandbox && !s.PageSandboxes:
		rec.Simulated = true
		rec.Detail = "On-call page simulated (public demo sandbox - no real page is sent)"
		return rec, nil
	case s.SNS == nil || s.TopicARN == "":
		rec.Simulated = true
		rec.Detail = "On-call page simulated (no SNS topic configured)"
		return rec, nil
	}
	m := NewMessage(a, action)
	body, _ := json.MarshalIndent(m, "", "  ")
	subject := fmt.Sprintf("[Northfen] %s %s on %s", strings.ToUpper(m.Severity), a.Kind, a.EquipmentID)
	if len(subject) > 99 {
		subject = subject[:99]
	}
	out, err := s.SNS.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(s.TopicARN), Subject: aws.String(subject), Message: aws.String(string(body))})
	if err != nil {
		return rec, fmt.Errorf("sns publish: %w", err)
	}
	rec.Ref = aws.ToString(out.MessageId)
	rec.Detail = "On-call paged via SNS"
	return rec, nil
}
