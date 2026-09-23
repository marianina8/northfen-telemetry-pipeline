package store_test

import (
	"testing"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/store/storetest"
)

func TestMemContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T, now func() time.Time) store.Store {
		m := store.NewMem()
		m.Now = now
		return m
	})
}

func TestFileContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T, now func() time.Time) store.Store {
		f, err := store.OpenFile(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		f.Mem.Now = now
		return f
	})
}

func TestDynamoContract(t *testing.T) {
	for _, pageSize := range []int{0, 2} {
		storetest.Contract(t, func(t *testing.T, now func() time.Time) store.Store {
			fake := storetest.NewFakeDynamo("readings", "state", "app")
			fake.PageSize = pageSize
			fake.UnprocessedOnce = true
			return &store.Dynamo{Client: fake, ReadingsTable: "readings", StateTable: "state", AppTable: "app", Now: now}
		})
	}
}

// Two File stores on the same directory (CLI + dashboard) see each other's
// writes.
func TestFileSharedAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	a, _ := store.OpenFile(dir)
	b, _ := store.OpenFile(dir)
	defer a.Close()
	defer b.Close()
	ctx := t.Context()
	if err := a.PutRun(ctx, store.Run{ID: "R1", SessionID: "local"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetRun(ctx, "local", "R1"); err != nil {
		t.Fatalf("second handle can't see the write: %v", err)
	}
}
