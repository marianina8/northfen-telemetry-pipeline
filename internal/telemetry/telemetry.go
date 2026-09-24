// Package telemetry defines the one message shape that flows through the
// pipeline: a single sensor reading from one tool, tagged with the visitor
// session (sandbox) that produced it.
package telemetry

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Metric types. Detector parameters (min std, overrides) are keyed by these.
const (
	FrameTime   = "frame_time"  // render time per frame
	Temperature = "temperature" // CPU / GPU / host temperature
	Memory      = "memory"      // memory in use
	IOLatency   = "io_latency"  // storage / cache latency
	ErrorCount  = "error_count" // failed frames per minute
	License     = "license"     // license seats in use, checkout wait
)

// SensorTypes lists every metric type the pipeline knows about.
var SensorTypes = []string{FrameTime, Temperature, Memory, IOLatency, ErrorCount, License}

// NonNegative reports whether a metric type can never go below zero
// (counts, latencies, waits); the simulator clamps those at 0.
func NonNegative(t string) bool {
	return t == ErrorCount || t == IOLatency || t == License || t == Memory || t == FrameTime
}

// LocalSession is the session used by the CLI and local tools when no
// visitor sandbox is involved.
const LocalSession = "local"

// Reading is one sample from one sensor. Value is nil when the sensor reported
// nothing (a dropout) — that is a first-class, detectable condition, not a
// parse error.
type Reading struct {
	SessionID   string    `json:"session_id"`
	RunID       string    `json:"run_id,omitempty"`
	EquipmentID string    `json:"equipment_id"`
	ToolType    string    `json:"tool_type"`
	SensorID    string    `json:"sensor_id"`
	SensorType  string    `json:"sensor_type"`
	Unit        string    `json:"unit"`
	Tick        int       `json:"tick"`
	TS          time.Time `json:"ts"`
	Value       *float64  `json:"value"`
}

// SeriesKey identifies one sensor's time series inside a session:
// "<equipment_id>#<sensor_id>".
func (r Reading) SeriesKey() string { return SeriesKey(r.EquipmentID, r.SensorID) }

// PartitionKey is the Kinesis partition key: every reading for one tool in
// one session lands on the same shard, in order.
func (r Reading) PartitionKey() string { return r.SessionID + "#" + r.EquipmentID }

// SeriesKey builds "<equipment_id>#<sensor_id>".
func SeriesKey(equipmentID, sensorID string) string { return equipmentID + "#" + sensorID }

// SplitSeriesKey is the inverse of SeriesKey.
func SplitSeriesKey(k string) (equipmentID, sensorID string, ok bool) {
	return strings.Cut(k, "#")
}

var idRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,40}$`)

// ValidID reports whether s is safe to use as a session/equipment/sensor ID
// (it ends up in storage keys and file names).
func ValidID(s string) bool { return idRe.MatchString(s) }

// Validate checks the fields every stage relies on.
func (r Reading) Validate() error {
	switch {
	case !ValidID(r.SessionID):
		return fmt.Errorf("invalid session_id %q", r.SessionID)
	case !ValidID(r.EquipmentID):
		return fmt.Errorf("invalid equipment_id %q", r.EquipmentID)
	case !ValidID(r.SensorID):
		return fmt.Errorf("invalid sensor_id %q", r.SensorID)
	case r.SensorType == "":
		return fmt.Errorf("sensor_type is required")
	case r.TS.IsZero():
		return fmt.Errorf("ts is required")
	}
	return nil
}

// F returns a pointer to v (handy for building readings in tests/fixtures).
func F(v float64) *float64 { return &v }
