package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/detect"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// Contract runs the behaviour every Store must have. newStore gets a clock
// it must use for TTL checks.
func Contract(t *testing.T, newStore func(t *testing.T, now func() time.Time) store.Store) {
	ctx := context.Background()
	t0 := time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC)
	clock := t0
	now := func() time.Time { return clock }

	t.Run("readings", func(t *testing.T) {
		st := newStore(t, now)
		var rs []telemetry.Reading
		for i := 0; i < 60; i++ {
			v := float64(i)
			var p *float64 = &v
			if i == 30 {
				p = nil
			}
			rs = append(rs, telemetry.Reading{SessionID: "s1", RunID: "R1", EquipmentID: "CMP-07", SensorID: "pad_temp",
				SensorType: "temperature", Tick: i, TS: t0.Add(time.Duration(i) * 400 * time.Millisecond), Value: p})
		}
		other := rs[0]
		other.SessionID = "s2"
		if err := st.PutReadings(ctx, append(rs, other, rs[5]), 0); err != nil { // duplicate in batch is fine
			t.Fatal(err)
		}
		got, err := st.Readings(ctx, "s1", "CMP-07", "pad_temp", t0.Add(4*time.Second), t0.Add(8*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 11 || got[0].Tick != 10 || got[10].Tick != 20 {
			t.Fatalf("range read: %d readings %+v", len(got), got)
		}
		all, _ := st.Readings(ctx, "s1", "CMP-07", "pad_temp", t0, t0.Add(time.Hour))
		if len(all) != 60 || all[30].Value != nil || *all[31].Value != 31 {
			t.Fatalf("full read: %d, missing value not preserved", len(all))
		}
	})

	t.Run("states", func(t *testing.T) {
		st := newStore(t, now)
		mk := func(sess, sensor string, lastTick int) store.SeriesState {
			return store.SeriesState{SessionID: sess, EquipmentID: "ETCH-12", SensorID: sensor, SensorType: "rf_power",
				State: detect.State{RunID: "R1", LastTick: lastTick, Ready: true, Sigma: 1.5, Started: true}}
		}
		if err := st.PutStates(ctx, []store.SeriesState{mk("s1", "a", 5), mk("s1", "b", 6), mk("s2", "a", 7)}); err != nil {
			t.Fatal(err)
		}
		if err := st.PutStates(ctx, []store.SeriesState{mk("s1", "a", 9)}); err != nil { // overwrite
			t.Fatal(err)
		}
		got, err := st.GetStates(ctx, "s1", []string{"ETCH-12#a", "ETCH-12#zz"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got["ETCH-12#a"].State.LastTick != 9 || got["ETCH-12#a"].State.Sigma != 1.5 {
			t.Fatalf("%+v", got)
		}
		list, _ := st.ListStates(ctx, "s1")
		if len(list) != 2 {
			t.Fatalf("list states: %d", len(list))
		}
	})

	t.Run("windows", func(t *testing.T) {
		st := newStore(t, now)
		var ws []store.Window
		for i := 0; i < 30; i++ {
			ws = append(ws, store.Window{ID: fmt.Sprintf("W-%d", i), SessionID: "s1", RunID: "R1", EquipmentID: "CVD-04",
				SensorID: []string{"a", "b"}[i%2], StartTick: (i / 2) * 10, Status: "normal"})
		}
		ws[4].AlertID, ws[4].Status = "NF-1", "flagged"
		if err := st.PutWindows(ctx, ws); err != nil {
			t.Fatal(err)
		}
		w, err := st.GetWindow(ctx, "W-4")
		if err != nil || w.AlertID != "NF-1" {
			t.Fatalf("%v %+v", err, w)
		}
		if _, err := st.GetWindow(ctx, "W-nope"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("missing window: %v", err)
		}
		list, _ := st.ListWindows(ctx, "s1", "R1")
		if len(list) != 30 || list[0].StartTick != 0 || list[29].StartTick != 140 {
			t.Fatalf("list windows: %d", len(list))
		}
		if other, _ := st.ListWindows(ctx, "s1", "R2"); len(other) != 0 {
			t.Fatal("windows leaked across runs")
		}
	})

	t.Run("alerts", func(t *testing.T) {
		st := newStore(t, now)
		a := store.Alert{ID: "NF-1", SessionID: "s1", RunID: "R1", EquipmentID: "CMP-07", Kind: "anomaly",
			Status: store.StatusOpen, CreatedAt: t0, Flags: []store.SeriesFlag{{SensorID: "pad_temp", Rule: "drift"}}}
		created, err := st.CreateAlert(ctx, a)
		if err != nil || !created {
			t.Fatalf("create: %v %v", created, err)
		}
		if created, _ := st.CreateAlert(ctx, a); created {
			t.Fatal("second create must be a no-op (replayed batch)")
		}
		got, err := st.GetAlert(ctx, "NF-1")
		if err != nil || got.Version != 1 {
			t.Fatalf("%v version=%d", err, got.Version)
		}
		stale := got
		got.Status = store.StatusAcknowledged
		if err := st.UpdateAlert(ctx, &got); err != nil || got.Version != 2 {
			t.Fatalf("update: %v v=%d", err, got.Version)
		}
		stale.Status = store.StatusDismissed
		if err := st.UpdateAlert(ctx, &stale); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("stale update must conflict: %v", err)
		}
		b := a
		b.ID, b.SessionID, b.CreatedAt = "NF-2", "s2", t0.Add(time.Second)
		c := a
		c.ID, c.CreatedAt = "NF-3", t0.Add(2*time.Second)
		st.CreateAlert(ctx, b)
		st.CreateAlert(ctx, c)
		s1, _ := st.ListAlerts(ctx, "s1")
		if len(s1) != 2 || s1[0].ID != "NF-3" || s1[1].Status != store.StatusAcknowledged {
			t.Fatalf("session list: %+v", s1)
		}
		all, _ := st.ListAlerts(ctx, "")
		if len(all) != 3 {
			t.Fatalf("all sessions: %d", len(all))
		}
	})

	t.Run("ttl", func(t *testing.T) {
		st := newStore(t, now)
		exp := t0.Add(time.Hour).Unix()
		st.CreateAlert(ctx, store.Alert{ID: "NF-9", SessionID: "v1", CreatedAt: t0, ExpiresAt: exp})
		st.PutRun(ctx, store.Run{ID: "R9", SessionID: "v1", StartedAt: t0, ExpiresAt: exp})
		if _, err := st.GetAlert(ctx, "NF-9"); err != nil {
			t.Fatal(err)
		}
		clock = t0.Add(2 * time.Hour)
		defer func() { clock = t0 }()
		if _, err := st.GetAlert(ctx, "NF-9"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("expired alert still visible: %v", err)
		}
		if l, _ := st.ListAlerts(ctx, "v1"); len(l) != 0 {
			t.Fatal("expired alert listed")
		}
		if l, _ := st.ListRuns(ctx, "v1"); len(l) != 0 {
			t.Fatal("expired run listed")
		}
	})

	t.Run("runs and counters", func(t *testing.T) {
		st := newStore(t, now)
		st.PutRun(ctx, store.Run{ID: "R1", SessionID: "s1", Scenario: "a", StartedAt: t0})
		st.PutRun(ctx, store.Run{ID: "R2", SessionID: "s1", Scenario: "b", StartedAt: t0.Add(time.Minute)})
		st.PutRun(ctx, store.Run{ID: "R3", SessionID: "s2", StartedAt: t0})
		runs, _ := st.ListRuns(ctx, "s1")
		if len(runs) != 2 || runs[0].ID != "R2" {
			t.Fatalf("%+v", runs)
		}
		r, err := st.GetRun(ctx, "s1", "R1")
		if err != nil || r.Scenario != "a" {
			t.Fatal(err)
		}
		for i := 1; i <= 3; i++ {
			n, err := st.Incr(ctx, "s1", "runs", 3, 0)
			if err != nil || n != i {
				t.Fatalf("incr %d: %d %v", i, n, err)
			}
		}
		if _, err := st.Incr(ctx, "s1", "runs", 3, 0); !errors.Is(err, store.ErrLimit) {
			t.Fatalf("over limit: %v", err)
		}
		if n, err := st.Incr(ctx, "s1", "explains", 0, 0); err != nil || n != 1 {
			t.Fatalf("unlimited counter: %d %v", n, err)
		}
		sess, err := st.GetSession(ctx, "s1")
		if err != nil || sess.Runs != 3 || sess.Explains != 1 {
			t.Fatalf("%+v %v", sess, err)
		}
	})

	t.Run("history", func(t *testing.T) {
		st := newStore(t, now)
		cat := sim.MustCatalog()
		var hs []sim.HistoryRecord
		for _, eq := range cat.Equipment {
			hs = append(hs, eq.HistoryAsOf(t0)...)
		}
		st.PutHistory(ctx, hs)
		st.PutHistory(ctx, hs) // re-seeding doesn't duplicate
		var later []sim.HistoryRecord
		for _, eq := range cat.Equipment {
			later = append(later, eq.HistoryAsOf(t0.AddDate(0, 0, 3))...)
		}
		st.PutHistory(ctx, later) // nor does re-seeding on another day
		h, _ := st.History(ctx, "FARM-LGT", 3)
		if len(h) != 3 || !h[0].Date.After(h[1].Date) {
			t.Fatalf("%+v", h)
		}
		all, _ := st.History(ctx, "FARM-LGT", 0)
		if len(all) != 5 {
			t.Fatalf("history duplicated: %d records", len(all))
		}
	})
}
