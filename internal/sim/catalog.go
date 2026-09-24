package sim

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-yaml"

	northfen "github.com/marianina8/northfen-telemetry-pipeline"
)

// Limits are fixed spec-sheet alarm limits (used only by the naive
// comparison in compare/).
type Limits struct {
	Lo float64 `yaml:"lo" json:"lo"`
	Hi float64 `yaml:"hi" json:"hi"`
}

// Sensor is one channel on a tool.
type Sensor struct {
	ID       string  `yaml:"id" json:"id"`
	Type     string  `yaml:"type" json:"type"`
	Unit     string  `yaml:"unit" json:"unit"`
	Base     float64 `yaml:"base" json:"base"`
	Noise    float64 `yaml:"noise" json:"noise"`
	Decimals int     `yaml:"decimals" json:"decimals"`
	Spec     Limits  `yaml:"spec" json:"spec"`
}

// HistoryEntry is one synthetic maintenance/incident record.
type HistoryEntry struct {
	DaysAgo int    `yaml:"days_ago" json:"days_ago"`
	Kind    string `yaml:"kind" json:"kind"`
	Summary string `yaml:"summary" json:"summary"`
}

// Equipment is one render pool (the unit alerts are grouped by).
type Equipment struct {
	ID       string         `yaml:"id" json:"id"`
	Name     string         `yaml:"name" json:"name"`
	ToolType string         `yaml:"tool_type" json:"tool_type"`
	Line     string         `yaml:"line" json:"line"`
	Sensors  []Sensor       `yaml:"sensors" json:"sensors"`
	History  []HistoryEntry `yaml:"history" json:"history"`
}

// Sensor looks up a sensor by ID.
func (e Equipment) Sensor(id string) (Sensor, bool) {
	for _, s := range e.Sensors {
		if s.ID == id {
			return s, true
		}
	}
	return Sensor{}, false
}

// Catalog is demo/equipment.yaml plus the scenarios.
type Catalog struct {
	Equipment []Equipment `yaml:"equipment"`
	Scenarios []Scenario  `yaml:"-"`
}

// Tool looks up equipment by ID.
func (c *Catalog) Tool(id string) (Equipment, bool) {
	for _, e := range c.Equipment {
		if e.ID == id {
			return e, true
		}
	}
	return Equipment{}, false
}

// Scenario looks up a scenario by name (or its numeric prefix, e.g. "04").
func (c *Catalog) Scenario(name string) (Scenario, bool) {
	for _, s := range c.Scenarios {
		if s.Name == name || s.File == name || strings.HasPrefix(s.File, name+"-") {
			return s, true
		}
	}
	return Scenario{}, false
}

// HistoryRecord is a history entry resolved to a date, as stored.
type HistoryRecord struct {
	EquipmentID string    `json:"equipment_id"`
	Date        time.Time `json:"date"`
	Kind        string    `json:"kind"`
	Summary     string    `json:"summary"`
}

// HistoryAsOf resolves days_ago against now (newest first).
func (e Equipment) HistoryAsOf(now time.Time) []HistoryRecord {
	day := now.UTC().Truncate(24 * time.Hour)
	out := make([]HistoryRecord, 0, len(e.History))
	for _, h := range e.History {
		out = append(out, HistoryRecord{EquipmentID: e.ID, Date: day.AddDate(0, 0, -h.DaysAgo), Kind: h.Kind, Summary: h.Summary})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Date.After(out[j].Date) })
	return out
}

// LoadCatalog reads the embedded demo data.
func LoadCatalog() (*Catalog, error) { return LoadCatalogFS(northfen.Demo) }

// MustCatalog is LoadCatalog for callers that can't recover (tests prevent
// a broken embedded catalog from shipping).
func MustCatalog() *Catalog {
	c, err := LoadCatalog()
	if err != nil {
		panic(err)
	}
	return c
}

// LoadCatalogFS reads demo/equipment.yaml and demo/scenarios/*.yaml.
func LoadCatalogFS(fsys fs.FS) (*Catalog, error) {
	b, err := fs.ReadFile(fsys, "demo/equipment.yaml")
	if err != nil {
		return nil, err
	}
	var c Catalog
	if err := yaml.UnmarshalWithOptions(b, &c, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("equipment.yaml: %w", err)
	}
	files, err := fs.Glob(fsys, "demo/scenarios/*.yaml")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	for _, f := range files {
		b, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, err
		}
		var s Scenario
		if err := yaml.UnmarshalWithOptions(b, &s, yaml.Strict()); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		s.File = strings.TrimSuffix(f[strings.LastIndex(f, "/")+1:], ".yaml")
		if err := s.validate(&c); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if err := s.loadReplay(fsys, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		c.Scenarios = append(c.Scenarios, s)
	}
	return &c, nil
}
