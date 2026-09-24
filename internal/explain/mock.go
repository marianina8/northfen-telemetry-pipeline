package explain

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Mock is a deterministic, offline stand-in for the Bedrock call, so the
// whole pipeline runs and tests without AWS. It is a small lookup table from
// the sensor types involved to plausible cause categories — not a model, and
// labelled "mock" everywhere it appears.
type Mock struct {
	Now func() time.Time
	// Fail makes every call fail (to exercise the explain-failed path).
	Fail bool
}

// Name implements Model.
func (m *Mock) Name() string { return "mock:heuristic" }

var causesByType = map[string][]string{
	"frame_time":  {"driver_or_image_update", "asset_or_scene_change", "node_hardware"},
	"temperature": {"node_hardware", "monitoring_or_telemetry"},
	"memory":      {"memory_pressure", "asset_or_scene_change"},
	"io_latency":  {"storage_io", "network"},
	"error_count": {"asset_or_scene_change", "node_hardware", "license_server"},
	"license":     {"license_server", "scheduler_or_queue"},
}

var checksByCause = map[string]string{
	"storage_io":              "Check NAS/filer latency and throughput for the pool's volume, and any snapshot or rebuild running now",
	"node_hardware":           "Check the node's GPU/CPU temperature, fan speed and throttling flags; drain it from the pool if it's hot",
	"driver_or_image_update":  "Compare frame times on nodes with the new driver/image against nodes without it",
	"asset_or_scene_change":   "Check which asset or scene versions were published just before the change and diff their render stats",
	"memory_pressure":         "Check peak memory per job against node memory, and whether jobs started swapping",
	"license_server":          "Check license checkout waits and seats in use on the license server",
	"network":                 "Check the pool's uplink and switch error counters between nodes and storage",
	"scheduler_or_queue":      "Check render-manager chunking and slot limits for the pool",
	"monitoring_or_telemetry": "Cross-check the metric against the node or service directly before touching the farm",
}

// Explain implements Model.
func (m *Mock) Explain(_ context.Context, in Input) (Explanation, error) {
	if m.Fail {
		return Explanation{}, fmt.Errorf("mock explain configured to fail")
	}
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}
	// Moving sensors: flagged, or unflagged but with a real shift in its
	// recent mean (one noisy reading past 3 sigma is not "moving").
	type mover struct {
		s     SensorSummary
		score float64
	}
	var movers []mover
	for _, s := range in.Sensors {
		sc := math.Abs(s.ShiftSigma)
		if s.Flagged {
			sc = math.Max(sc, s.MaxDevSigma)
		}
		if s.Flagged || sc >= 3 {
			movers = append(movers, mover{s, sc})
		}
	}
	sort.SliceStable(movers, func(i, j int) bool {
		if movers[i].s.Flagged != movers[j].s.Flagged {
			return movers[i].s.Flagged
		}
		return movers[i].score > movers[j].score
	})
	types := map[string]bool{}
	var names []string
	for _, mv := range movers {
		types[mv.s.SensorType] = true
		names = append(names, mv.s.SensorID)
	}

	var causes []string
	switch {
	case types["io_latency"] && types["frame_time"]:
		causes = []string{"storage_io", "network", "scheduler_or_queue"}
	case types["temperature"] && types["frame_time"]:
		causes = []string{"node_hardware", "driver_or_image_update"}
	case types["memory"] && types["error_count"]:
		causes = []string{"memory_pressure", "asset_or_scene_change"}
	default:
		for _, mv := range movers {
			for _, c := range causesByType[mv.s.SensorType] {
				causes = appendUnique(causes, c)
			}
		}
	}
	if len(causes) == 0 {
		causes = []string{"unknown"}
	}
	if len(causes) > 3 {
		causes = causes[:3]
	}

	maxZ, maxShift, rules := 0.0, 0.0, map[string]bool{}
	for _, t := range in.Triggers {
		maxZ = math.Max(maxZ, math.Abs(t.Z))
		rules[t.Rule] = true
	}
	for _, mv := range movers {
		maxZ = math.Max(maxZ, mv.s.MaxDevSigma)
		maxShift = math.Max(maxShift, math.Abs(mv.s.ShiftSigma))
	}
	correlated := len(movers) >= 2

	sev := "low"
	switch {
	case correlated || (rules["spike"] && maxZ >= 8):
		sev = "high"
	case rules["sustained"] && maxZ >= 4:
		sev = "medium"
	case rules["drift"] && maxShift >= 6:
		sev = "medium"
	}

	conf := 0.7
	if correlated {
		conf = 0.82
	}
	if !correlated && maxZ < 3.5 && !rules["drift"] {
		conf = 0.5 // a lone, barely-over-threshold excursion: weak evidence
	}
	if historyMentions(in.History, causes[0]) {
		conf = math.Min(0.9, conf+0.05)
	}

	var text string
	if correlated {
		text = fmt.Sprintf("%s on %s are moving together, which points to %s rather than a single faulty node or metric.",
			joinNames(names), in.Equipment.Name, human(causes[0]))
	} else if len(names) > 0 {
		text = fmt.Sprintf("%s on %s moved on its own while the pool's other metrics stayed normal, most consistent with %s.",
			names[0], in.Equipment.Name, human(causes[0]))
	} else {
		text = fmt.Sprintf("The flagged pattern on %s doesn't match a known signature.", in.Equipment.Name)
	}
	text += " (Offline mock explanation - deploy with Bedrock for a real diagnosis.)"

	var checks []string
	for _, c := range causes {
		if ck, ok := checksByCause[c]; ok {
			checks = append(checks, ck)
		}
	}
	if len(checks) == 0 {
		checks = []string{"Review the tool's recent changes with the equipment owner"}
	}
	return Explanation{
		LikelyCauses: causes, Explanation: text, RecommendedChecks: checks,
		Severity: sev, Confidence: math.Round(conf*100) / 100, Model: m.Name(), At: now().UTC(),
	}, nil
}

var historyWords = map[string][]string{
	"storage_io":             {"nas", "filer", "snapshot", "cache", "storage"},
	"node_hardware":          {"fan", "thermal", "gpu", "paste"},
	"driver_or_image_update": {"driver", "image"},
	"asset_or_scene_change":  {"asset", "publish", "texture", "scene"},
	"license_server":         {"license"},
	"scheduler_or_queue":     {"render manager", "chunking"},
}

func historyMentions(h []HistoryItem, cause string) bool {
	for _, it := range h {
		s := strings.ToLower(it.Summary)
		for _, w := range historyWords[cause] {
			if strings.Contains(s, w) {
				return true
			}
		}
	}
	return false
}

func human(cause string) string { return strings.ReplaceAll(cause, "_", " ") }

func joinNames(ns []string) string {
	switch len(ns) {
	case 0:
		return ""
	case 1:
		return ns[0]
	case 2:
		return ns[0] + " and " + ns[1]
	}
	return strings.Join(ns[:len(ns)-1], ", ") + " and " + ns[len(ns)-1]
}
