package config_test

import (
	"strings"
	"testing"

	northfen "github.com/marianina8/northfen-telemetry-pipeline"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/config"
)

func TestDefaultIsValid(t *testing.T) {
	c := config.Default()
	if c.Detector.SustainedZ != 3 || c.Detector.SustainedCount != 3 || c.Dispatch.LowConfidenceBelow != 0.6 {
		t.Fatalf("unexpected defaults: %+v", c.Detector)
	}
	if c.Explain.Provider != "mock" {
		t.Fatal("the repo default must run offline (mock); the deployed stack sets bedrock")
	}
}

func TestValidation(t *testing.T) {
	base := string(northfen.DefaultConfig)
	for name, mut := range map[string][2]string{
		"spike below sustained": {"spike_z: 5.0", "spike_z: 2.0"},
		"unknown action":        {"action: log_only", "action: shrug"},
		"unknown severity":      {"when: { severity: high }", "when: { severity: urgent }"},
		"unknown provider":      {"provider: mock", "provider: gpt"},
		"unknown key":           {"warmup: 20", "warmup: 20\n  wamrup: 3"},
		"bad per-type spike":    {"error_count: { min_sigma: 1.0, spike_z: 6.0 }", "error_count: { min_sigma: 1.0, spike_z: 1.0 }"},
	} {
		if !strings.Contains(base, mut[0]) {
			t.Fatalf("%s: fixture text %q not in config", name, mut[0])
		}
		if _, err := config.Parse([]byte(strings.Replace(base, mut[0], mut[1], 1))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
