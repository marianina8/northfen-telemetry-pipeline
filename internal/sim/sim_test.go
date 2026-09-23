package sim_test

import (
	"reflect"
	"testing"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
)

func TestCatalog(t *testing.T) {
	c := sim.MustCatalog()
	if len(c.Equipment) != 4 || len(c.Scenarios) != 10 {
		t.Fatalf("%d tools, %d scenarios", len(c.Equipment), len(c.Scenarios))
	}
	cats := map[string]int{}
	for _, s := range c.Scenarios {
		cats[s.Category]++
	}
	for _, want := range []string{"baseline", "gradual_drift", "spike", "noisy_normal", "sensor_fault", "threshold_boundary", "correlated_drift"} {
		if cats[want] == 0 {
			t.Errorf("no %s scenario", want)
		}
	}
	if _, ok := c.Scenario("04"); !ok {
		t.Error("lookup by numeric prefix")
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	c := sim.MustCatalog()
	for _, sc := range c.Scenarios {
		a, _ := sim.Generate(c, sc, sim.Options{})
		b, _ := sim.Generate(c, sc, sim.Options{})
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("%s: not deterministic", sc.Name)
		}
		eq, _ := c.Tool(sc.EquipmentID)
		if len(a) != sc.Ticks*len(eq.Sensors) {
			t.Fatalf("%s: %d readings", sc.Name, len(a))
		}
	}
}

func TestPatterns(t *testing.T) {
	c := sim.MustCatalog()
	get := func(name, sensor string) []*float64 {
		sc, _ := c.Scenario(name)
		rs, _ := sim.Generate(c, sc, sim.Options{})
		var out []*float64
		for _, r := range rs {
			if r.SensorID == sensor {
				out = append(out, r.Value)
			}
		}
		return out
	}
	stuck := get("07", "beamline_vacuum")
	for i := 51; i < len(stuck); i++ {
		if *stuck[i] != *stuck[50] {
			t.Fatalf("stuck sensor moved at tick %d", i)
		}
	}
	drop := get("08", "esc_temp")
	for i, v := range drop {
		if (i >= 60 && i < 68) != (v == nil) {
			t.Fatalf("dropout wrong at tick %d", i)
		}
	}
	b := get("09", "heater_temp")
	if *b[20] != 401.5 || *b[0] != 400.5 || *b[1] != 399.5 {
		t.Fatalf("boundary fixture values: %v %v %v", *b[0], *b[1], *b[20])
	}
}
