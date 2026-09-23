package detect

import (
	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// Point is one scored reading.
type Point struct {
	Tick     int      `json:"tick"`
	Value    *float64 `json:"value"`
	Z        *float64 `json:"z,omitempty"`
	Rules    []string `json:"rules,omitempty"`
	Baseline float64  `json:"baseline"`
	Sigma    float64  `json:"sigma"`
}

// Series is the scored result for one sensor on one tool.
type Series struct {
	Key         string   `json:"key"`
	EquipmentID string   `json:"equipment_id"`
	SensorID    string   `json:"sensor_id"`
	SensorType  string   `json:"sensor_type"`
	Unit        string   `json:"unit"`
	Flags       []Flag   `json:"flags"`
	Windows     []Window `json:"windows"`
	Points      []Point  `json:"points,omitempty"`
	State       State    `json:"-"`
}

// ScoreAll runs the detector over a batch of readings with fresh state per
// series — no store, no AWS. Readings may interleave series; each series is
// scored in the order its readings appear. Series come back in order of
// first appearance.
func ScoreAll(cfg config.Detector, readings []telemetry.Reading, keepPoints bool) []*Series {
	var order []*Series
	byKey := map[string]*Series{}
	for _, r := range readings {
		k := r.SessionID + "|" + r.RunID + "|" + r.SeriesKey()
		s := byKey[k]
		if s == nil {
			s = &Series{Key: r.SeriesKey(), EquipmentID: r.EquipmentID, SensorID: r.SensorID, SensorType: r.SensorType, Unit: r.Unit}
			byKey[k] = s
			order = append(order, s)
		}
		if s.State.Started && r.Tick <= s.State.LastTick {
			continue // replayed reading
		}
		res := Step(cfg.For(r.SensorType), &s.State, r.Tick, r.Value)
		if res.Flag != nil {
			s.Flags = append(s.Flags, *res.Flag)
		}
		if res.Window != nil {
			s.Windows = append(s.Windows, *res.Window)
		}
		if keepPoints {
			s.Points = append(s.Points, Point{Tick: r.Tick, Value: r.Value, Z: res.Z, Rules: res.Rules, Baseline: res.Baseline, Sigma: res.Sigma})
		}
	}
	return order
}
