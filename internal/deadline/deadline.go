// Package deadline reads render-manager history exported from AWS Deadline
// Cloud and turns it into evenly spaced per-host metrics the detector can use.
//
// Deadline Cloud reports work as it happens (a task run starts, succeeds or
// fails on some worker), at irregular times. The detector wants one reading
// per metric per tick. This package does that resampling:
//
//   - a host's frame time for a bucket is the median duration of the task
//     runs that finished successfully on that host inside the bucket;
//   - a bucket where the host finished nothing is reported as missing (nil),
//     so a host that goes quiet shows up as a monitoring fault rather than as
//     "zero seconds per frame";
//   - failed frames are counted per bucket.
//
// The export format mirrors the Deadline Cloud API's own JSON output (the
// field names of ListSessions, GetWorker and ListSessionActions) so a studio
// can produce it with the AWS CLI; see demo/deadline/export-deadline-cloud.sh.
package deadline

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Format identifies the export file layout.
const Format = "northfen.deadline-cloud-export/v1"

// Export is one exported slice of farm history.
type Export struct {
	Format     string    `json:"format"`
	ExportedAt time.Time `json:"exportedAt"`
	Note       string    `json:"note,omitempty"`
	FarmID     string    `json:"farmId"`
	QueueID    string    `json:"queueId"`
	Jobs       []Job     `json:"jobs"`
	Workers    []Worker  `json:"workers"`
	Sessions   []Session `json:"sessions"`
	// SessionActions is keyed by sessionId (ListSessionActions is called per
	// session and its summaries don't repeat the session ID).
	SessionActions map[string][]SessionAction `json:"sessionActions"`
}

// Job is a trimmed GetJob/ListJobs summary.
type Job struct {
	JobID string `json:"jobId"`
	Name  string `json:"name"`
}

// Worker is a trimmed GetWorker response.
type Worker struct {
	WorkerID       string         `json:"workerId"`
	FleetID        string         `json:"fleetId"`
	Status         string         `json:"status,omitempty"`
	HostProperties HostProperties `json:"hostProperties"`
}

// HostProperties is the worker's host.
type HostProperties struct {
	HostName string `json:"hostName"`
}

// Session is a ListSessions summary: one worker's stint on one job.
type Session struct {
	SessionID       string     `json:"sessionId"`
	JobID           string     `json:"jobId"`
	WorkerID        string     `json:"workerId"`
	FleetID         string     `json:"fleetId"`
	StartedAt       time.Time  `json:"startedAt"`
	EndedAt         *time.Time `json:"endedAt,omitempty"`
	LifecycleStatus string     `json:"lifecycleStatus"`
}

// SessionAction is a ListSessionActions summary. Only taskRun actions (one
// frame or chunk of frames) are used.
type SessionAction struct {
	SessionActionID string     `json:"sessionActionId"`
	Status          string     `json:"status"`
	StartedAt       *time.Time `json:"startedAt,omitempty"`
	EndedAt         *time.Time `json:"endedAt,omitempty"`
	Definition      struct {
		TaskRun *struct {
			TaskID string `json:"taskId"`
			StepID string `json:"stepId"`
		} `json:"taskRun,omitempty"`
	} `json:"definition"`
}

// Parse decodes and checks an export.
func Parse(b []byte) (*Export, error) {
	var e Export
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("deadline export: %w", err)
	}
	if e.Format != Format {
		return nil, fmt.Errorf("deadline export: format %q, want %q", e.Format, Format)
	}
	if len(e.Workers) == 0 || len(e.Sessions) == 0 {
		return nil, errors.New("deadline export: no workers or sessions")
	}
	return &e, nil
}

// TaskRun is one finished task run, resolved to its host.
type TaskRun struct {
	Host     string
	Status   string // SUCCEEDED, FAILED, ...
	Started  time.Time
	Ended    time.Time
	Duration time.Duration
}

// TaskRuns flattens the export into finished task runs, oldest first.
func (e *Export) TaskRuns() []TaskRun {
	host := map[string]string{}
	for _, w := range e.Workers {
		host[w.WorkerID] = w.HostProperties.HostName
	}
	var out []TaskRun
	for _, s := range e.Sessions {
		for _, a := range e.SessionActions[s.SessionID] {
			if a.Definition.TaskRun == nil || a.StartedAt == nil || a.EndedAt == nil {
				continue
			}
			out = append(out, TaskRun{Host: host[s.WorkerID], Status: a.Status, Started: *a.StartedAt, Ended: *a.EndedAt,
				Duration: a.EndedAt.Sub(*a.StartedAt)})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Ended.Before(out[j].Ended) })
	return out
}

// Hosts lists the host names in the export.
func (e *Export) Hosts() []string {
	seen := map[string]bool{}
	var out []string
	for _, w := range e.Workers {
		if h := w.HostProperties.HostName; h != "" && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	sort.Strings(out)
	return out
}

// Options control resampling.
type Options struct {
	Bucket time.Duration // tick length (default 5m)
	Start  time.Time     // start of bucket 0 (default: the first task run's end, truncated to Bucket)
	Max    int           // cap on buckets (0 = through the last finished task run)
}

// Buckets is the resampled history.
type Buckets struct {
	Start  time.Time
	Bucket time.Duration
	N      int
	// FrameTime[host][i] is the median successful task-run duration that
	// ended in bucket i, or nil when the host finished nothing.
	FrameTime map[string][]*time.Duration
	// Failed[host][i] counts failed task runs that ended in bucket i.
	Failed map[string][]int
	// Succeeded[host][i] counts successful task runs that ended in bucket i.
	Succeeded map[string][]int
}

// Resample buckets the export's task runs.
func (e *Export) Resample(o Options) (*Buckets, error) {
	runs := e.TaskRuns()
	if len(runs) == 0 {
		return nil, errors.New("deadline export: no finished task runs")
	}
	if o.Bucket <= 0 {
		o.Bucket = 5 * time.Minute
	}
	if o.Start.IsZero() {
		// History starts when the first frame finishes: before that no host
		// has anything to report, and those empty buckets would read as
		// every host going quiet.
		o.Start = runs[0].Ended.UTC().Truncate(o.Bucket)
	}
	n := int(runs[len(runs)-1].Ended.Sub(o.Start)/o.Bucket) + 1
	if o.Max > 0 && n > o.Max {
		n = o.Max
	}
	b := &Buckets{Start: o.Start, Bucket: o.Bucket, N: n,
		FrameTime: map[string][]*time.Duration{}, Failed: map[string][]int{}, Succeeded: map[string][]int{}}
	durs := map[string][][]time.Duration{}
	for _, h := range e.Hosts() {
		durs[h] = make([][]time.Duration, n)
		b.Failed[h] = make([]int, n)
		b.Succeeded[h] = make([]int, n)
	}
	for _, r := range runs {
		if r.Ended.Before(o.Start) {
			continue
		}
		i := int(r.Ended.Sub(o.Start) / o.Bucket)
		if i >= n || durs[r.Host] == nil {
			continue
		}
		switch r.Status {
		case "SUCCEEDED":
			durs[r.Host][i] = append(durs[r.Host][i], r.Duration)
			b.Succeeded[r.Host][i]++
		case "FAILED":
			b.Failed[r.Host][i]++
		}
	}
	for h, per := range durs {
		col := make([]*time.Duration, n)
		for i, ds := range per {
			if len(ds) > 0 {
				m := median(ds)
				col[i] = &m
			}
		}
		b.FrameTime[h] = col
	}
	return b, nil
}

func median(ds []time.Duration) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}
