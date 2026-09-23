package explain_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/explain"
)

var cats = config.Default().Explain.CauseCategories

func TestParseValid(t *testing.T) {
	in := "```json\n" + `{"likely_causes":["Mechanical_Wear","thermal_control","mechanical_wear"],"explanation":"Vibration and pad temperature rise together.","recommended_checks":["Inspect the spindle bearing"],"severity":"HIGH","confidence":0.8}` + "\n```"
	e, err := explain.Parse(in, cats)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.LikelyCauses) != 2 || e.LikelyCauses[0] != "mechanical_wear" || e.Severity != "high" || e.Confidence != 0.8 || e.Raw != in {
		t.Fatalf("%+v", e)
	}
}

func TestParseRejects(t *testing.T) {
	base := func(mod string) string {
		m := map[string]string{
			"causes": `["mechanical_wear"]`, "expl": `"x"`, "checks": `["y"]`, "sev": `"low"`, "conf": `0.5`, "extra": ``,
		}
		k, v, _ := strings.Cut(mod, "=")
		m[k] = v
		s := `{"likely_causes":` + m["causes"] + `,"explanation":` + m["expl"] + `,"recommended_checks":` + m["checks"] + `,"severity":` + m["sev"]
		if m["conf"] != "" {
			s += `,"confidence":` + m["conf"]
		}
		return s + m["extra"] + `}`
	}
	for _, mod := range []string{
		"causes=[\"gremlins\"]", // not a configured category
		"causes=[]",
		"expl=\"\"",
		"checks=[]",
		"sev=\"critical\"",
		"conf=1.2",
		"conf=-0.1",
		"conf=",                             // missing
		"extra=,\"action\":\"page_oncall\"", // the model may not pick the action
		"extra=,\"is_anomalous\":false",     // ... nor re-decide detection
	} {
		if _, err := explain.Parse(base(mod), cats); !errors.Is(err, explain.ErrBadOutput) {
			t.Errorf("%s: accepted: %v", mod, err)
		}
	}
	if _, err := explain.Parse("the tool looks fine to me", cats); !errors.Is(err, explain.ErrBadOutput) {
		t.Error("prose accepted")
	}
}

func TestSystemPromptBoundsTheModel(t *testing.T) {
	p := explain.SystemPrompt(cats)
	for _, want := range []string{"ALREADY happened", "Do not second-guess", "correlation", "below 0.6", "ONLY a JSON object"} {
		if !strings.Contains(p, want) {
			t.Errorf("system prompt missing %q", want)
		}
	}
	for _, c := range cats {
		if !strings.Contains(p, c) {
			t.Errorf("category %s not listed", c)
		}
	}
}

type fakeConverse struct {
	reply string
	err   error
	in    *bedrockruntime.ConverseInput
}

func (f *fakeConverse) Converse(_ context.Context, in *bedrockruntime.ConverseInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error) {
	f.in = in
	if f.err != nil {
		return nil, f.err
	}
	return &bedrockruntime.ConverseOutput{Output: &types.ConverseOutputMemberMessage{Value: types.Message{
		Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: f.reply}},
	}}}, nil
}

func input() explain.Input {
	return explain.Input{Equipment: explain.Equipment{ID: "CMP-07", Name: "CMP Polisher 7"}, CauseCategories: cats,
		Triggers: []explain.Trigger{{SensorID: "spindle_vib", Rule: "drift"}},
		Sensors:  []explain.SensorSummary{{SensorID: "spindle_vib", SensorType: "vibration_rms", Flagged: true, ShiftSigma: 5}}}
}

func TestBedrockOneBoundedCall(t *testing.T) {
	f := &fakeConverse{reply: `{"likely_causes":["mechanical_wear"],"explanation":"Spindle vibration is creeping up.","recommended_checks":["Check the bearing"],"severity":"medium","confidence":0.7}`}
	b, err := explain.NewBedrock(f, config.Default())
	if err != nil {
		t.Fatal(err)
	}
	e, err := b.Explain(context.Background(), input())
	if err != nil {
		t.Fatal(err)
	}
	if e.Severity != "medium" || !strings.HasPrefix(e.Model, "bedrock:") {
		t.Fatalf("%+v", e)
	}
	if len(f.in.Messages) != 1 || f.in.ToolConfig != nil || *f.in.InferenceConfig.Temperature != 0 {
		t.Fatal("expected one user message, no tools, temperature 0")
	}
	if !strings.Contains(f.in.Messages[0].Content[0].(*types.ContentBlockMemberText).Value, `"spindle_vib"`) {
		t.Fatal("input not sent")
	}
}

func TestBedrockBadReplyKeepsRaw(t *testing.T) {
	f := &fakeConverse{reply: "Looks like bearing wear, page someone."}
	b, _ := explain.NewBedrock(f, config.Default())
	e, err := b.Explain(context.Background(), input())
	if !errors.Is(err, explain.ErrBadOutput) || e.Raw == "" {
		t.Fatalf("err=%v raw=%q", err, e.Raw)
	}
}

func TestMockCorrelation(t *testing.T) {
	in := input()
	in.Sensors = append(in.Sensors, explain.SensorSummary{SensorID: "pad_temp", SensorType: "temperature", Flagged: true, ShiftSigma: 4})
	e, err := (&explain.Mock{}).Explain(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if e.LikelyCauses[0] != "mechanical_wear" || e.Severity != "high" || !strings.Contains(e.Explanation, "together") || !strings.Contains(e.Model, "mock") {
		t.Fatalf("%+v", e)
	}
	// every mock output passes the same strict schema as Bedrock's
	if _, err := explain.Parse(mustJSON(e), cats); err != nil {
		t.Fatal(err)
	}
}

func mustJSON(e explain.Explanation) string {
	q := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"` }
	var cs, ks []string
	for _, c := range e.LikelyCauses {
		cs = append(cs, q(c))
	}
	for _, k := range e.RecommendedChecks {
		ks = append(ks, q(k))
	}
	return `{"likely_causes":[` + strings.Join(cs, ",") + `],"explanation":` + q(e.Explanation) +
		`,"recommended_checks":[` + strings.Join(ks, ",") + `],"severity":` + q(e.Severity) + `,"confidence":0.5}`
}
