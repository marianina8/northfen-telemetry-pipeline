package sim_test

import (
	"reflect"
	"testing"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
)

func TestCatalog(t *testing.T) {
	c := sim.MustCatalog()
	if len(c.Equipment) != 4 || len(c.Scenarios) != 11 {
		t.Fatalf("%d tools, %d scenarios", len(c.Equipment), len(c.Scenarios))
	}
	cats := map[string]int{}
	for _, s := range c.Scenarios {
		cats[s.Category]++
	}
	for _, want := range []string{"baseline", "gradual_drift", "spike", "noisy_normal", "sensor_fault", "threshold_boundary", "correlated_drift", "render_manager_replay"} {
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
	stuck := get("07", "license_seats_used")
	for i := 51; i < len(stuck); i++ {
		if *stuck[i] != *stuck[50] {
			t.Fatalf("stuck sensor moved at tick %d", i)
		}
	}
	drop := get("08", "node07_gpu_temp")
	for i, v := range drop {
		if (i >= 60 && i < 68) != (v == nil) {
			t.Fatalf("dropout wrong at tick %d", i)
		}
	}
	b := get("09", "sim_node21_cpu_temp")
	if *b[20] != 69.5 || *b[0] != 68.5 || *b[1] != 67.5 {
		t.Fatalf("boundary fixture values: %v %v %v", *b[0], *b[1], *b[20])
	}
}

func TestDeadlineReplay(t *testing.T) {
	c := sim.MustCatalog()
	sc, ok := c.Scenario("11")
	if !ok || !sc.Replayed() {
		t.Fatal("scenario 11 should replay a Deadline export")
	}
	rs, err := sim.Generate(c, sc, sim.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// node12's frame time (minutes, from the export) is ~11 before 02:50
	// (tick 70) and ~13.8 after; node07 stays ~11.
	avg := func(sensor string, from, to int) float64 {
		sum, n := 0.0, 0
		for _, r := range rs {
			if r.SensorID == sensor && r.Tick >= from && r.Tick < to && r.Value != nil {
				sum += *r.Value
				n++
			}
		}
		return sum / float64(n)
	}
	if b, a := avg("node12_frame_time", 20, 70), avg("node12_frame_time", 76, 120); b < 10.5 || b > 12 || a-b < 2 {
		t.Errorf("node12 before %.2f after %.2f", b, a)
	}
	if b, a := avg("node07_frame_time", 20, 70), avg("node07_frame_time", 76, 120); a-b > 0.5 || a-b < -0.5 {
		t.Errorf("node07 moved: %.2f -> %.2f", b, a)
	}
	// The non-Deadline metrics are still synthetic and present every tick.
	n := 0
	for _, r := range rs {
		if r.SensorID == "nas_read_latency" && r.Value != nil {
			n++
		}
	}
	if n != sc.Ticks {
		t.Errorf("nas readings %d", n)
	}
}
