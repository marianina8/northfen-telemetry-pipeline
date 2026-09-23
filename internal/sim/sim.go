// Package sim generates synthetic sensor telemetry from named scenarios.
// Generation is deterministic: the same scenario and seed always produce the
// same readings, so detection outcomes can be asserted in tests and the
// public demo behaves the same for every visitor.
package sim

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// Pattern types.
const (
	PatternDrift   = "drift"   // value += per_tick * (tick - start), from start (to end, then holds)
	PatternSpike   = "spike"   // value += magnitude for duration ticks from at
	PatternStep    = "step"    // value += magnitude from start on
	PatternStuck   = "stuck"   // from start the sensor repeats one value (value, or its last reading)
	PatternDropout = "dropout" // from start, length readings are missing
	PatternValues  = "values"  // explicit readings from start (overrides everything else)
)

// Pattern is one injected behaviour on one sensor.
type Pattern struct {
	Type      string    `yaml:"type" json:"type"`
	Start     int       `yaml:"start,omitempty" json:"start,omitempty"`
	End       int       `yaml:"end,omitempty" json:"end,omitempty"`
	At        int       `yaml:"at,omitempty" json:"at,omitempty"`
	PerTick   float64   `yaml:"per_tick,omitempty" json:"per_tick,omitempty"`
	Magnitude float64   `yaml:"magnitude,omitempty" json:"magnitude,omitempty"`
	Duration  int       `yaml:"duration,omitempty" json:"duration,omitempty"`
	Length    int       `yaml:"length,omitempty" json:"length,omitempty"`
	Value     *float64  `yaml:"value,omitempty" json:"value,omitempty"`
	Values    []float64 `yaml:"values,omitempty" json:"values,omitempty"`
}

// SensorPlan overrides one sensor's nominal behaviour for a scenario.
type SensorPlan struct {
	Noise      *float64  `yaml:"noise,omitempty" json:"noise,omitempty"`
	NoiseModel string    `yaml:"noise_model,omitempty" json:"noise_model,omitempty"` // gaussian (default) | alternate
	Patterns   []Pattern `yaml:"patterns,omitempty" json:"patterns,omitempty"`
}

// Scenario is one named, replayable stream.
type Scenario struct {
	Name        string                `yaml:"name" json:"name"`
	Title       string                `yaml:"title" json:"title"`
	Description string                `yaml:"description" json:"description"`
	Category    string                `yaml:"category" json:"category"`
	EquipmentID string                `yaml:"equipment_id" json:"equipment_id"`
	Ticks       int                   `yaml:"ticks" json:"ticks"`
	Seed        uint64                `yaml:"seed" json:"seed"`
	NoiseModel  string                `yaml:"noise_model,omitempty" json:"noise_model,omitempty"`
	Sensors     map[string]SensorPlan `yaml:"sensors,omitempty" json:"sensors,omitempty"`
	File        string                `yaml:"-" json:"file"`
}

func (s Scenario) validate(c *Catalog) error {
	if !telemetry.ValidID(s.Name) {
		return fmt.Errorf("invalid scenario name %q", s.Name)
	}
	eq, ok := c.Tool(s.EquipmentID)
	if !ok {
		return fmt.Errorf("unknown equipment_id %q", s.EquipmentID)
	}
	if s.Ticks < 1 || s.Ticks > 600 {
		return fmt.Errorf("ticks must be 1..600")
	}
	for id, p := range s.Sensors {
		if _, ok := eq.Sensor(id); !ok {
			return fmt.Errorf("equipment %s has no sensor %q", eq.ID, id)
		}
		switch p.NoiseModel {
		case "", "gaussian", "alternate":
		default:
			return fmt.Errorf("sensor %s: unknown noise_model %q", id, p.NoiseModel)
		}
		for _, pt := range p.Patterns {
			switch pt.Type {
			case PatternDrift, PatternSpike, PatternStep, PatternStuck, PatternDropout, PatternValues:
			default:
				return fmt.Errorf("sensor %s: unknown pattern %q", id, pt.Type)
			}
		}
	}
	return nil
}

// Options control timestamps and tagging.
type Options struct {
	SessionID string
	RunID     string
	Start     time.Time     // timestamp of tick 0
	Interval  time.Duration // time between ticks
}

// Generate produces every reading for the scenario, tick by tick (all of a
// tick's sensors together, in catalog order).
func Generate(c *Catalog, sc Scenario, o Options) ([]telemetry.Reading, error) {
	eq, ok := c.Tool(sc.EquipmentID)
	if !ok {
		return nil, fmt.Errorf("unknown equipment %q", sc.EquipmentID)
	}
	if o.SessionID == "" {
		o.SessionID = telemetry.LocalSession
	}
	if o.Interval <= 0 {
		o.Interval = time.Second
	}
	if o.Start.IsZero() {
		o.Start = time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC) // "2am on-call"
	}
	gens := make([]*sensorGen, len(eq.Sensors))
	for i, s := range eq.Sensors {
		gens[i] = newSensorGen(sc, s)
	}
	out := make([]telemetry.Reading, 0, sc.Ticks*len(eq.Sensors))
	for t := 0; t < sc.Ticks; t++ {
		ts := o.Start.Add(time.Duration(t) * o.Interval).UTC()
		for i, s := range eq.Sensors {
			out = append(out, telemetry.Reading{
				SessionID: o.SessionID, RunID: o.RunID, EquipmentID: eq.ID, ToolType: eq.ToolType,
				SensorID: s.ID, SensorType: s.Type, Unit: s.Unit, Tick: t, TS: ts, Value: gens[i].next(t),
			})
		}
	}
	return out, nil
}

type sensorGen struct {
	s      Sensor
	plan   SensorPlan
	noise  float64
	model  string
	rng    *rand.Rand
	last   float64
	stuckV *float64
}

func newSensorGen(sc Scenario, s Sensor) *sensorGen {
	p := sc.Sensors[s.ID]
	g := &sensorGen{s: s, plan: p, noise: s.Noise, model: sc.NoiseModel}
	if p.Noise != nil {
		g.noise = *p.Noise
	}
	if p.NoiseModel != "" {
		g.model = p.NoiseModel
	}
	h := fnv.New64a()
	h.Write([]byte(sc.Name + "/" + s.ID))
	g.rng = rand.New(rand.NewPCG(sc.Seed, h.Sum64()))
	return g
}

func (g *sensorGen) next(t int) *float64 {
	// The noise draw happens every tick so a pattern on one sensor never
	// shifts the random sequence of the others (or of later ticks).
	var n float64
	switch g.model {
	case "alternate":
		n = g.noise
		if t%2 == 1 {
			n = -g.noise
		}
	default:
		n = g.rng.NormFloat64() * g.noise
	}
	v := g.s.Base + n
	for _, p := range g.plan.Patterns {
		switch p.Type {
		case PatternDrift:
			if t >= p.Start {
				k := t - p.Start
				if p.End > p.Start && t > p.End {
					k = p.End - p.Start
				}
				v += p.PerTick * float64(k)
			}
		case PatternSpike:
			d := p.Duration
			if d < 1 {
				d = 1
			}
			if t >= p.At && t < p.At+d {
				v += p.Magnitude
			}
		case PatternStep:
			if t >= p.Start {
				v += p.Magnitude
			}
		}
	}
	v = g.round(v)
	for _, p := range g.plan.Patterns {
		switch p.Type {
		case PatternValues:
			if i := t - p.Start; t >= p.Start && i < len(p.Values) {
				v = p.Values[i]
			}
		case PatternStuck:
			if t >= p.Start {
				if g.stuckV == nil {
					sv := g.last
					if p.Value != nil {
						sv = *p.Value
					}
					g.stuckV = &sv
				}
				v = *g.stuckV
			}
		case PatternDropout:
			if t >= p.Start && t < p.Start+p.Length {
				return nil
			}
		}
	}
	g.last = v
	return &v
}

func (g *sensorGen) round(v float64) float64 {
	if g.s.Type == telemetry.ParticleCount && v < 0 {
		v = 0
	}
	p := math.Pow(10, float64(g.s.Decimals))
	return math.Round(v*p) / p
}
