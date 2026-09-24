package deadline_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/deadline"
)

func at(min, sec int) *time.Time {
	t := time.Date(2026, 1, 5, 1, 0, 0, 0, time.UTC).Add(time.Duration(min)*time.Minute + time.Duration(sec)*time.Second)
	return &t
}

func action(id, status string, start, end *time.Time, task bool) deadline.SessionAction {
	a := deadline.SessionAction{SessionActionID: id, Status: status, StartedAt: start, EndedAt: end}
	if task {
		a.Definition.TaskRun = &struct {
			TaskID string `json:"taskId"`
			StepID string `json:"stepId"`
		}{TaskID: "task-" + id, StepID: "step-1"}
	}
	return a
}

// A small hand-built export: host "a" finishes three frames in bucket 0
// (10, 12 and 30 minutes -> median 12), fails one in bucket 1, then nothing;
// host "b" finishes one frame in bucket 2. The envEnter action (no taskRun)
// must be ignored.
func sample() deadline.Export {
	w := func(id, host string) deadline.Worker {
		x := deadline.Worker{WorkerID: id, FleetID: "fleet-1"}
		x.HostProperties.HostName = host
		return x
	}
	return deadline.Export{
		Format:  deadline.Format,
		Workers: []deadline.Worker{w("worker-a1", "a"), w("worker-a2", "a"), w("worker-b1", "b")},
		Sessions: []deadline.Session{
			{SessionID: "s-a1", WorkerID: "worker-a1", StartedAt: *at(-40, 0)},
			{SessionID: "s-a2", WorkerID: "worker-a2", StartedAt: *at(-40, 0)},
			{SessionID: "s-b1", WorkerID: "worker-b1", StartedAt: *at(0, 0)},
		},
		SessionActions: map[string][]deadline.SessionAction{
			"s-a1": {
				action("0", "SUCCEEDED", at(-40, 0), at(-39, 0), false), // envEnter
				action("1", "SUCCEEDED", at(-8, 0), at(2, 0), true),     // 10 min, bucket 0
				action("2", "SUCCEEDED", at(-26, 0), at(4, 0), true),    // 30 min, bucket 0
				action("3", "FAILED", at(4, 10), at(6, 0), true),        // bucket 1
			},
			"s-a2": {
				action("1", "SUCCEEDED", at(-9, 0), at(3, 0), true), // 12 min, bucket 0
			},
			"s-b1": {
				action("1", "SUCCEEDED", at(0, 0), at(11, 30), true), // bucket 2
			},
		},
	}
}

func TestResample(t *testing.T) {
	e := sample()
	b, err := e.Resample(deadline.Options{Bucket: 5 * time.Minute, Start: *at(0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if b.N != 3 {
		t.Fatalf("N=%d, want 3", b.N)
	}
	a := b.FrameTime["a"]
	if a[0] == nil || *a[0] != 12*time.Minute {
		t.Errorf("host a bucket 0 = %v, want median 12m", a[0])
	}
	if a[1] != nil || a[2] != nil {
		t.Errorf("host a finished nothing after bucket 0: %v %v", a[1], a[2])
	}
	if b.Failed["a"][1] != 1 || b.Succeeded["a"][0] != 3 {
		t.Errorf("counts: failed %v succeeded %v", b.Failed["a"], b.Succeeded["a"])
	}
	if x := b.FrameTime["b"][2]; x == nil || *x != 11*time.Minute+30*time.Second {
		t.Errorf("host b bucket 2 = %v", x)
	}
}

func TestResampleDefaultsAndCap(t *testing.T) {
	e := sample()
	b, err := e.Resample(deadline.Options{Max: 2})
	if err != nil {
		t.Fatal(err)
	}
	if b.Bucket != 5*time.Minute || !b.Start.Equal(*at(0, 0)) || b.N != 2 {
		t.Fatalf("start %v bucket %v N %d", b.Start, b.Bucket, b.N)
	}
}

func TestParse(t *testing.T) {
	e := sample()
	raw, _ := json.Marshal(e)
	got, err := deadline.Parse(raw)
	if err != nil || len(got.TaskRuns()) != 5 {
		t.Fatalf("parse: %v, %d task runs", err, len(got.TaskRuns()))
	}
	if h := got.Hosts(); strings.Join(h, ",") != "a,b" {
		t.Errorf("hosts %v", h)
	}
	if _, err := deadline.Parse([]byte(`{"format":"something-else"}`)); err == nil {
		t.Error("wrong format accepted")
	}
}
