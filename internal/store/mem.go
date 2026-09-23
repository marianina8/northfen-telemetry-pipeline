package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// memData is the whole local dataset (also the File store's on-disk format).
type memData struct {
	Readings map[string]map[string]telemetry.Reading `json:"readings"` // series -> ts -> reading
	States   map[string]SeriesState                  `json:"states"`   // session|equip#sensor
	Windows  map[string]Window                       `json:"windows"`
	Alerts   map[string]Alert                        `json:"alerts"`
	Runs     map[string]Run                          `json:"runs"` // session|run
	Sessions map[string]Session                      `json:"sessions"`
	History  map[string][]sim.HistoryRecord          `json:"history"`
}

func newMemData() *memData {
	return &memData{
		Readings: map[string]map[string]telemetry.Reading{}, States: map[string]SeriesState{},
		Windows: map[string]Window{}, Alerts: map[string]Alert{}, Runs: map[string]Run{},
		Sessions: map[string]Session{}, History: map[string][]sim.HistoryRecord{},
	}
}

func (d *memData) fill() {
	if d.Readings == nil {
		d.Readings = map[string]map[string]telemetry.Reading{}
	}
	if d.States == nil {
		d.States = map[string]SeriesState{}
	}
	if d.Windows == nil {
		d.Windows = map[string]Window{}
	}
	if d.Alerts == nil {
		d.Alerts = map[string]Alert{}
	}
	if d.Runs == nil {
		d.Runs = map[string]Run{}
	}
	if d.Sessions == nil {
		d.Sessions = map[string]Session{}
	}
	if d.History == nil {
		d.History = map[string][]sim.HistoryRecord{}
	}
}

// Mem is an in-memory Store. Safe for concurrent use.
type Mem struct {
	mu sync.Mutex
	d  *memData
	// Now is used to hide expired records (like DynamoDB TTL).
	Now func() time.Time
	// hooks used by File to load/persist around every operation.
	before func(write bool) error
	after  func(write bool) error
}

// NewMem returns an empty store.
func NewMem() *Mem { return &Mem{d: newMemData(), Now: time.Now} }

// TSKey formats a timestamp as a fixed-width, sortable key (what DynamoDB
// uses as the readings sort key).
func TSKey(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// ReadingSeries is the readings partition key: "<session>#<equipment>#<sensor>".
func ReadingSeries(sessionID, equipmentID, sensorID string) string {
	return sessionID + "#" + equipmentID + "#" + sensorID
}

func (m *Mem) do(write bool, f func(d *memData) error) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.before != nil {
		if err := m.before(write); err != nil {
			return err
		}
	}
	err = f(m.d)
	if m.after != nil {
		if aerr := m.after(write && err == nil); aerr != nil && err == nil {
			err = aerr
		}
	}
	return err
}

func (m *Mem) expired(exp int64) bool {
	return exp > 0 && m.Now().Unix() > exp
}

func clone[T any](v T) T {
	b, err := json.Marshal(v)
	if err != nil {
		// Everything the pipeline stores must survive a JSON round trip
		// (DynamoDB stores the same documents). Fail loudly, never silently.
		panic(fmt.Sprintf("store: value is not JSON-serializable: %v", err))
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		panic(fmt.Sprintf("store: value does not round-trip through JSON: %v", err))
	}
	return out
}

// PutReadings implements Store.
func (m *Mem) PutReadings(_ context.Context, rs []telemetry.Reading, _ int64) error {
	return m.do(true, func(d *memData) error {
		for _, r := range rs {
			k := ReadingSeries(r.SessionID, r.EquipmentID, r.SensorID)
			if d.Readings[k] == nil {
				d.Readings[k] = map[string]telemetry.Reading{}
			}
			d.Readings[k][TSKey(r.TS)] = r
		}
		return nil
	})
}

// Readings implements Store (inclusive range, ordered by time).
func (m *Mem) Readings(_ context.Context, sessionID, equipmentID, sensorID string, from, to time.Time) ([]telemetry.Reading, error) {
	var out []telemetry.Reading
	err := m.do(false, func(d *memData) error {
		lo, hi := TSKey(from), TSKey(to)
		for k, r := range d.Readings[ReadingSeries(sessionID, equipmentID, sensorID)] {
			if k >= lo && k <= hi {
				out = append(out, r)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].TS.Before(out[j].TS) })
	return out, err
}

func stateKey(sessionID, key string) string { return sessionID + "|" + key }

// GetStates implements Store.
func (m *Mem) GetStates(_ context.Context, sessionID string, keys []string) (map[string]SeriesState, error) {
	out := map[string]SeriesState{}
	err := m.do(false, func(d *memData) error {
		for _, k := range keys {
			if s, ok := d.States[stateKey(sessionID, k)]; ok && !m.expired(s.ExpiresAt) {
				out[k] = clone(s)
			}
		}
		return nil
	})
	return out, err
}

// PutStates implements Store.
func (m *Mem) PutStates(_ context.Context, states []SeriesState) error {
	return m.do(true, func(d *memData) error {
		for _, s := range states {
			d.States[stateKey(s.SessionID, s.Key())] = clone(s)
		}
		return nil
	})
}

// ListStates implements Store.
func (m *Mem) ListStates(_ context.Context, sessionID string) ([]SeriesState, error) {
	var out []SeriesState
	err := m.do(false, func(d *memData) error {
		for k, s := range d.States {
			if strings.HasPrefix(k, sessionID+"|") && !m.expired(s.ExpiresAt) {
				out = append(out, clone(s))
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, err
}

// PutWindows implements Store.
func (m *Mem) PutWindows(_ context.Context, ws []Window) error {
	return m.do(true, func(d *memData) error {
		for _, w := range ws {
			d.Windows[w.ID] = clone(w)
		}
		return nil
	})
}

// GetWindow implements Store.
func (m *Mem) GetWindow(_ context.Context, id string) (Window, error) {
	var out Window
	err := m.do(false, func(d *memData) error {
		w, ok := d.Windows[id]
		if !ok || m.expired(w.ExpiresAt) {
			return fmt.Errorf("window %s: %w", id, ErrNotFound)
		}
		out = clone(w)
		return nil
	})
	return out, err
}

// ListWindows implements Store.
func (m *Mem) ListWindows(_ context.Context, sessionID, runID string) ([]Window, error) {
	var out []Window
	err := m.do(false, func(d *memData) error {
		for _, w := range d.Windows {
			if w.SessionID == sessionID && (runID == "" || w.RunID == runID) && !m.expired(w.ExpiresAt) {
				out = append(out, clone(w))
			}
		}
		return nil
	})
	sortWindows(out)
	return out, err
}

func sortWindows(ws []Window) {
	sort.Slice(ws, func(i, j int) bool {
		a, b := ws[i], ws[j]
		if a.RunID != b.RunID {
			return a.StartTS.Before(b.StartTS)
		}
		if a.EquipmentID+a.SensorID != b.EquipmentID+b.SensorID {
			return a.EquipmentID+a.SensorID < b.EquipmentID+b.SensorID
		}
		return a.StartTick < b.StartTick
	})
}

// CreateAlert implements Store.
func (m *Mem) CreateAlert(_ context.Context, a Alert) (bool, error) {
	created := false
	err := m.do(true, func(d *memData) error {
		if _, ok := d.Alerts[a.ID]; ok {
			return nil
		}
		a.Version = 1
		d.Alerts[a.ID] = clone(a)
		created = true
		return nil
	})
	return created, err
}

// GetAlert implements Store.
func (m *Mem) GetAlert(_ context.Context, id string) (Alert, error) {
	var out Alert
	err := m.do(false, func(d *memData) error {
		a, ok := d.Alerts[id]
		if !ok || m.expired(a.ExpiresAt) {
			return fmt.Errorf("alert %s: %w", id, ErrNotFound)
		}
		out = clone(a)
		return nil
	})
	return out, err
}

// UpdateAlert implements Store.
func (m *Mem) UpdateAlert(_ context.Context, a *Alert) error {
	return m.do(true, func(d *memData) error {
		cur, ok := d.Alerts[a.ID]
		if !ok {
			return fmt.Errorf("alert %s: %w", a.ID, ErrNotFound)
		}
		if cur.Version != a.Version {
			return fmt.Errorf("alert %s: %w", a.ID, ErrConflict)
		}
		a.Version++
		d.Alerts[a.ID] = clone(*a)
		return nil
	})
}

// ListAlerts implements Store (newest first).
func (m *Mem) ListAlerts(_ context.Context, sessionID string) ([]Alert, error) {
	var out []Alert
	err := m.do(false, func(d *memData) error {
		for _, a := range d.Alerts {
			if (sessionID == "" || a.SessionID == sessionID) && !m.expired(a.ExpiresAt) {
				out = append(out, clone(a))
			}
		}
		return nil
	})
	SortAlerts(out)
	return out, err
}

// SortAlerts orders newest first (stable on ID).
func SortAlerts(as []Alert) {
	sort.Slice(as, func(i, j int) bool {
		if !as[i].CreatedAt.Equal(as[j].CreatedAt) {
			return as[i].CreatedAt.After(as[j].CreatedAt)
		}
		return as[i].ID < as[j].ID
	})
}

func runKey(sessionID, runID string) string { return sessionID + "|" + runID }

// PutRun implements Store.
func (m *Mem) PutRun(_ context.Context, r Run) error {
	return m.do(true, func(d *memData) error {
		d.Runs[runKey(r.SessionID, r.ID)] = r
		return nil
	})
}

// GetRun implements Store.
func (m *Mem) GetRun(_ context.Context, sessionID, runID string) (Run, error) {
	var out Run
	err := m.do(false, func(d *memData) error {
		r, ok := d.Runs[runKey(sessionID, runID)]
		if !ok || m.expired(r.ExpiresAt) {
			return fmt.Errorf("run %s: %w", runID, ErrNotFound)
		}
		out = r
		return nil
	})
	return out, err
}

// ListRuns implements Store (newest first).
func (m *Mem) ListRuns(_ context.Context, sessionID string) ([]Run, error) {
	var out []Run
	err := m.do(false, func(d *memData) error {
		for _, r := range d.Runs {
			if r.SessionID == sessionID && !m.expired(r.ExpiresAt) {
				out = append(out, r)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, err
}

// GetSession implements Store.
func (m *Mem) GetSession(_ context.Context, id string) (Session, error) {
	var out Session
	err := m.do(false, func(d *memData) error {
		s, ok := d.Sessions[id]
		if !ok || m.expired(s.ExpiresAt) {
			return fmt.Errorf("session %s: %w", id, ErrNotFound)
		}
		out = s
		return nil
	})
	return out, err
}

// Incr implements Store.
func (m *Mem) Incr(_ context.Context, id, counter string, max int, expiresAt int64) (int, error) {
	n := 0
	err := m.do(true, func(d *memData) error {
		s, ok := d.Sessions[id]
		if !ok || m.expired(s.ExpiresAt) {
			s = Session{ID: id, CreatedAt: m.Now().UTC(), ExpiresAt: expiresAt}
		}
		p := &s.Runs
		if counter == "explains" {
			p = &s.Explains
		} else if counter != "runs" {
			return fmt.Errorf("unknown counter %q", counter)
		}
		if max > 0 && *p >= max {
			n = *p
			return fmt.Errorf("session %s %s: %w", id, counter, ErrLimit)
		}
		*p++
		n = *p
		d.Sessions[id] = s
		return nil
	})
	return n, err
}

// PutHistory implements Store (replaces a tool's history).
func (m *Mem) PutHistory(_ context.Context, hs []sim.HistoryRecord) error {
	return m.do(true, func(d *memData) error {
		by := map[string][]sim.HistoryRecord{}
		for _, h := range hs {
			by[h.EquipmentID] = append(by[h.EquipmentID], h)
		}
		for k, v := range by {
			sort.SliceStable(v, func(i, j int) bool { return v[i].Date.After(v[j].Date) })
			d.History[k] = v
		}
		return nil
	})
}

// History implements Store (newest first).
func (m *Mem) History(_ context.Context, equipmentID string, limit int) ([]sim.HistoryRecord, error) {
	var out []sim.HistoryRecord
	err := m.do(false, func(d *memData) error {
		h := d.History[equipmentID]
		if limit > 0 && len(h) > limit {
			h = h[:limit]
		}
		out = append(out, h...)
		return nil
	})
	return out, err
}
