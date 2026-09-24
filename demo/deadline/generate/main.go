// Command generate writes demo/deadline/lgt-overnight-export.json: a
// SYNTHETIC overnight of Deadline Cloud history for two lighting-pool render
// nodes, in the same JSON shape the export script produces from a real farm.
//
//	go run ./demo/deadline/generate > demo/deadline/lgt-overnight-export.json
//
// The night: two lighting jobs render on render-node07 and render-node12
// (8 GPUs each, one Deadline worker per GPU, frames ~11 minutes). At 02:50
// render-node12's frames get ~25% slower and a few start failing; node07 is
// unaffected. Everything is deterministic (fixed seed).
package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/deadline"
)

var rng = rand.New(rand.NewPCG(20260105, 11))

func id(prefix string) string {
	return fmt.Sprintf("%s-%016x%016x", prefix, rng.Uint64(), rng.Uint64())
}

func tp(t time.Time) *time.Time { return &t }

type host struct {
	name    string
	base    float64 // minutes per frame
	slowAt  time.Time
	slowBy  float64 // minutes added per frame from slowAt
	failPct float64 // share of frames that fail from slowAt
}

func main() {
	start := time.Date(2026, 1, 4, 21, 0, 0, 0, time.UTC) // 9pm: overnight renders start
	end := start.Add(10*time.Hour + 5*time.Minute)
	switchAt := start.Add(5 * time.Hour) // second job picks up at 02:00
	slowAt := time.Date(2026, 1, 5, 2, 50, 0, 0, time.UTC)

	exp := deadline.Export{
		Format:     deadline.Format,
		ExportedAt: end.Add(20 * time.Minute),
		Note:       "SYNTHETIC sample in the Deadline Cloud export format (demo/deadline/export-deadline-cloud.sh). Invented farm, jobs and hosts.",
		FarmID:     id("farm"),
		QueueID:    id("queue"),
		Jobs: []deadline.Job{
			{JobID: id("job"), Name: "ep104_sq030_lighting_v012"},
			{JobID: id("job"), Name: "ep104_sq040_lighting_v007"},
		},
		SessionActions: map[string][]deadline.SessionAction{},
	}
	fleet := id("fleet")
	steps := []string{id("step"), id("step")}
	taskSeq := []int{0, 0}

	hosts := []host{
		{name: "render-node07", base: 11.0},
		{name: "render-node12", base: 11.2, slowAt: slowAt, slowBy: 2.6, failPct: 0.08},
	}
	for _, h := range hosts {
		for gpu := 0; gpu < 8; gpu++ {
			w := deadline.Worker{WorkerID: id("worker"), FleetID: fleet, Status: "IDLE"}
			w.HostProperties.HostName = h.name
			exp.Workers = append(exp.Workers, w)

			// Stagger the GPUs so frames don't all finish together.
			t := start.Add(time.Duration(rng.IntN(600)) * time.Second)
			for j := range exp.Jobs {
				jobEnd := switchAt
				if j == 1 {
					jobEnd = end
				}
				s := deadline.Session{SessionID: id("session"), JobID: exp.Jobs[j].JobID, WorkerID: w.WorkerID,
					FleetID: fleet, StartedAt: t, LifecycleStatus: "ENDED"}
				sid := s.SessionID[len("session-"):]
				var acts []deadline.SessionAction
				n := 0
				add := func(a deadline.SessionAction) {
					a.SessionActionID = fmt.Sprintf("sessionaction-%s-%d", sid, n)
					n++
					acts = append(acts, a)
				}
				// Environment setup (scene load, sync inputs) - not a frame; the
				// adapter must skip it.
				envEnd := t.Add(time.Duration(40+rng.IntN(40)) * time.Second)
				add(deadline.SessionAction{Status: "SUCCEEDED", StartedAt: tp(t), EndedAt: tp(envEnd)})
				t = envEnd
				for t.Before(jobEnd) {
					mins := h.base + rng.NormFloat64()*0.9
					status := "SUCCEEDED"
					if !h.slowAt.IsZero() && !t.Before(h.slowAt) {
						mins += h.slowBy
						if rng.Float64() < h.failPct {
							status, mins = "FAILED", 1.5+rng.Float64()*3 // crashed partway
						}
					}
					fin := t.Add(time.Duration(mins * float64(time.Minute))).Truncate(time.Second)
					a := deadline.SessionAction{Status: status, StartedAt: tp(t), EndedAt: tp(fin)}
					a.Definition.TaskRun = &struct {
						TaskID string `json:"taskId"`
						StepID string `json:"stepId"`
					}{TaskID: fmt.Sprintf("task-%s-%d", steps[j][len("step-"):len("step-")+32], taskSeq[j]), StepID: steps[j]}
					taskSeq[j]++
					add(a)
					t = fin.Add(time.Duration(5+rng.IntN(15)) * time.Second) // next task dispatch
				}
				s.EndedAt = tp(t)
				exp.Sessions = append(exp.Sessions, s)
				exp.SessionActions[s.SessionID] = acts
			}
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(exp); err != nil {
		panic(err)
	}
}
