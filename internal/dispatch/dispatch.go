// Package dispatch turns an explained alert into an action with a small,
// ordered rules table from config — the same principle as the ICR router:
// the model outputs structured judgment (severity, confidence); plain code
// decides what happens.
package dispatch

import (
	"fmt"
	"math"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
)

// Facts are the inputs the table matches on.
type Facts struct {
	Kind          string  // anomaly | sensor_fault
	Severity      string  // from the model ("" when there is no explanation)
	Confidence    float64 // from the model (0 when there is no explanation)
	ExplainFailed bool
	MaxAbsZ       float64 // from the detector
}

// Decision is the table's verdict.
type Decision struct {
	Action string `json:"action"`
	Rule   string `json:"rule"`
	Reason string `json:"reason"`
	Floor  string `json:"floor,omitempty"`
}

// Decide applies the rules (first match wins), then the floors (which can
// only raise the action). If nothing matches, the alert goes to a human.
func Decide(c config.Dispatch, f Facts) Decision {
	d := Decision{Action: config.ActionPageOnCall, Rule: "no-rule-matched", Reason: "no dispatch rule matched; defaulting to a human"}
	for _, r := range c.Rules {
		if matches(r.When, f) {
			d = Decision{Action: r.Action, Rule: r.Name, Reason: reason(r, f)}
			break
		}
	}
	for _, fl := range c.Floors {
		if matches(fl.When, f) && config.ActionRank(fl.Action) > config.ActionRank(d.Action) {
			d.Floor = fl.Name
			d.Reason += fmt.Sprintf("; raised to %s by floor %s", fl.Action, fl.Name)
			d.Action = fl.Action
		}
	}
	return d
}

func matches(w config.Condition, f Facts) bool {
	if w.Kind != "" && w.Kind != f.Kind {
		return false
	}
	if w.ExplainFailed != nil && *w.ExplainFailed != f.ExplainFailed {
		return false
	}
	explained := !f.ExplainFailed && f.Kind != "sensor_fault"
	if w.Severity != "" && (!explained || w.Severity != f.Severity) {
		return false
	}
	if w.ConfidenceBelow != nil && (!explained || f.Confidence >= *w.ConfidenceBelow) {
		return false
	}
	if w.MinAbsZ != nil && math.Abs(f.MaxAbsZ) < *w.MinAbsZ {
		return false
	}
	return true
}

func reason(r config.Rule, f Facts) string {
	switch {
	case r.When.ExplainFailed != nil:
		return "no explanation available (model call failed) - a human diagnoses it"
	case r.When.Kind == "sensor_fault":
		return "sensor fault: the sensor itself looks broken, so a ticket goes to the equipment owner (no model call)"
	case r.When.Kind != "":
		return fmt.Sprintf("kind=%s", f.Kind)
	case r.When.ConfidenceBelow != nil:
		return fmt.Sprintf("confidence %.2f < %.2f - uncertain explanations go to a human regardless of severity", f.Confidence, *r.When.ConfidenceBelow)
	case r.When.Severity != "":
		return fmt.Sprintf("severity=%s, confidence %.2f", f.Severity, f.Confidence)
	}
	return r.Name
}
