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
	"rf_power":         {"rf_delivery", "consumable_end_of_life", "sensor_or_instrumentation"},
	"chamber_pressure": {"vacuum_or_leak", "gas_delivery", "sensor_or_instrumentation"},
	"temperature":      {"thermal_control", "sensor_or_instrumentation"},
	"vibration_rms":    {"mechanical_wear", "consumable_end_of_life"},
	"particle_count":   {"contamination", "consumable_end_of_life"},
}

var checksByCause = map[string]string{
	"rf_delivery":               "Check RF generator forward/reflected power logs and the match network tune positions",
	"vacuum_or_leak":            "Run a rate-of-rise leak check and inspect the throttle valve position trend",
	"gas_delivery":              "Compare MFC setpoints vs. actual flows on the active recipe step",
	"thermal_control":           "Check heater/chiller loop setpoint vs. actual and thermocouple readings",
	"mechanical_wear":           "Inspect spindle bearing and pad conditioner; compare vibration spectrum to the last PM baseline",
	"consumable_end_of_life":    "Check consumable usage counters (pad, rings, liners) against their limits",
	"contamination":             "Pull a particle monitor wafer and inspect the chamber for flaking",
	"sensor_or_instrumentation": "Cross-check the sensor against a secondary gauge before touching the process",
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
	case types["vibration_rms"] && types["temperature"]:
		causes = []string{"mechanical_wear", "consumable_end_of_life", "thermal_control"}
	case types["chamber_pressure"] && types["rf_power"]:
		causes = []string{"vacuum_or_leak", "rf_delivery", "gas_delivery"}
	case types["particle_count"] && types["chamber_pressure"]:
		causes = []string{"contamination", "vacuum_or_leak"}
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
		text = fmt.Sprintf("%s on %s are moving together, which points to %s rather than a single faulty sensor.",
			joinNames(names), in.Equipment.Name, human(causes[0]))
	} else if len(names) > 0 {
		text = fmt.Sprintf("%s on %s moved on its own while the other sensors stayed normal, most consistent with %s.",
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
	"mechanical_wear":        {"bearing", "spindle", "conditioner"},
	"vacuum_or_leak":         {"pressure", "foreline", "throttle", "vacuum", "leak"},
	"contamination":          {"particle", "flaking", "clean"},
	"rf_delivery":            {"rf", "match"},
	"thermal_control":        {"heater", "thermocouple", "chiller"},
	"consumable_end_of_life": {"replaced", "end of life"},
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
