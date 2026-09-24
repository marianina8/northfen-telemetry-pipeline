package sim

import (
	"fmt"
	"io/fs"
	"math"
	"strings"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/deadline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// Source replaces some of a scenario's synthetic metrics with metrics
// derived from a render-manager export. Metrics it doesn't name (GPU
// temperature, storage latency, ...) stay synthetic: a render manager doesn't
// report them, a studio's node and storage monitoring does.
type Source struct {
	Format string `yaml:"format" json:"format"` // deadline-cloud
	File   string `yaml:"file" json:"file"`     // path inside the repo (embedded)
	Bucket string `yaml:"bucket,omitempty" json:"bucket,omitempty"`
	// FrameTime maps a host name to the frame_time sensor fed by it.
	FrameTime map[string]string `yaml:"frame_time,omitempty" json:"frame_time,omitempty"`
	// FailedFrames names an error_count sensor fed by failed task runs
	// (summed over every host in the export).
	FailedFrames string `yaml:"failed_frames,omitempty" json:"failed_frames,omitempty"`
}

// BucketDuration parses Bucket (default 5m).
func (s Source) BucketDuration() (time.Duration, error) {
	if s.Bucket == "" {
		return 5 * time.Minute, nil
	}
	d, err := time.ParseDuration(s.Bucket)
	if err != nil || d < time.Minute || d > 24*time.Hour {
		return 0, fmt.Errorf("bucket %q: want a duration between 1m and 24h", s.Bucket)
	}
	return d, nil
}

// Replayed is one sensor's values per tick taken from an export.
type Replayed map[string][]*float64

// FromDeadline resamples an export into per-tick values for the pool's
// sensors. max caps the number of ticks (0 = all). The returned buckets say
// where tick 0 starts and how many ticks there are.
func FromDeadline(eq Equipment, exp *deadline.Export, src Source, max int) (Replayed, *deadline.Buckets, error) {
	bucket, err := src.BucketDuration()
	if err != nil {
		return nil, nil, err
	}
	if len(src.FrameTime) == 0 && src.FailedFrames == "" {
		return nil, nil, fmt.Errorf("source maps no metrics (set frame_time and/or failed_frames)")
	}
	b, err := exp.Resample(deadline.Options{Bucket: bucket, Max: max})
	if err != nil {
		return nil, nil, err
	}
	out := Replayed{}
	for host, sid := range src.FrameTime {
		s, ok := eq.Sensor(sid)
		if !ok {
			return nil, nil, fmt.Errorf("pool %s has no metric %q", eq.ID, sid)
		}
		if s.Type != telemetry.FrameTime {
			return nil, nil, fmt.Errorf("metric %s is %s, not frame_time", sid, s.Type)
		}
		col, ok := b.FrameTime[host]
		if !ok {
			return nil, nil, fmt.Errorf("host %q is not in the export (hosts: %s)", host, strings.Join(exp.Hosts(), ", "))
		}
		per, err := perFrame(s.Unit)
		if err != nil {
			return nil, nil, fmt.Errorf("metric %s: %w", sid, err)
		}
		vals := make([]*float64, b.N)
		for i, d := range col {
			if d != nil {
				v := roundTo(d.Seconds()/per.Seconds(), s.Decimals)
				vals[i] = &v
			}
		}
		out[sid] = vals
	}
	if sid := src.FailedFrames; sid != "" {
		s, ok := eq.Sensor(sid)
		if !ok {
			return nil, nil, fmt.Errorf("pool %s has no metric %q", eq.ID, sid)
		}
		if s.Type != telemetry.ErrorCount {
			return nil, nil, fmt.Errorf("metric %s is %s, not error_count", sid, s.Type)
		}
		per := time.Minute
		if strings.HasSuffix(s.Unit, "/h") {
			per = time.Hour
		}
		vals := make([]*float64, b.N)
		for i := 0; i < b.N; i++ {
			n := 0
			for _, col := range b.Failed {
				n += col[i]
			}
			v := roundTo(float64(n)*per.Seconds()/bucket.Seconds(), s.Decimals)
			vals[i] = &v
		}
		out[sid] = vals
	}
	return out, b, nil
}

// perFrame reads the time unit out of a frame_time unit ("min/frame",
// "s/frame").
func perFrame(unit string) (time.Duration, error) {
	switch strings.SplitN(unit, "/", 2)[0] {
	case "s", "sec":
		return time.Second, nil
	case "min":
		return time.Minute, nil
	case "h":
		return time.Hour, nil
	}
	return 0, fmt.Errorf("can't convert a duration to %q", unit)
}

func roundTo(v float64, decimals int) float64 {
	p := math.Pow(10, float64(decimals))
	return math.Round(v*p) / p
}

// loadReplay resolves a scenario's source at catalog load.
func (s *Scenario) loadReplay(fsys fs.FS, c *Catalog) error {
	if s.Source == nil {
		return nil
	}
	if s.Source.Format != "deadline-cloud" {
		return fmt.Errorf("source format %q: only deadline-cloud is supported", s.Source.Format)
	}
	b, err := fs.ReadFile(fsys, s.Source.File)
	if err != nil {
		return err
	}
	exp, err := deadline.Parse(b)
	if err != nil {
		return err
	}
	eq, _ := c.Tool(s.EquipmentID)
	r, bk, err := FromDeadline(eq, exp, *s.Source, s.Ticks)
	if err != nil {
		return err
	}
	if bk.N < s.Ticks {
		return fmt.Errorf("export covers %d ticks, scenario wants %d", bk.N, s.Ticks)
	}
	s.replay = r
	return nil
}

// Replay builds readings for a pool from replayed values only (no synthetic
// metrics): what `northfen ingest` feeds the pipeline.
func Replay(eq Equipment, r Replayed, ticks int, o Options) []telemetry.Reading {
	if o.SessionID == "" {
		o.SessionID = telemetry.LocalSession
	}
	if o.Interval <= 0 {
		o.Interval = time.Second
	}
	var out []telemetry.Reading
	for t := 0; t < ticks; t++ {
		ts := o.Start.Add(time.Duration(t) * o.Interval).UTC()
		for _, s := range eq.Sensors {
			col, ok := r[s.ID]
			if !ok {
				continue
			}
			out = append(out, telemetry.Reading{
				SessionID: o.SessionID, RunID: o.RunID, EquipmentID: eq.ID, ToolType: eq.ToolType,
				SensorID: s.ID, SensorType: s.Type, Unit: s.Unit, Tick: t, TS: ts, Value: col[t],
			})
		}
	}
	return out
}
