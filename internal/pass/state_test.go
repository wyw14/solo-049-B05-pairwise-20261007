package pass

import (
	"os"
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

func TestUpdateRollsBackInMemoryStateWhenFlushFails(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	service, err := NewService(paths, nil, audit.NewTrail(store.NewAppendLog(paths)))
	if err != nil {
		t.Fatal(err)
	}
	value, err := service.Create(CreateRequest{
		SatelliteID: "SAT-T", AntennaID: "ANT-01",
		RFChains: []string{"RF-01"}, RequiredReceiverChains: []string{"RF-01"},
		AOS: time.Now().UTC().Add(time.Hour), LOS: time.Now().UTC().Add(2 * time.Hour),
		Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Make the snapshot unwritable: renaming a file onto a directory fails.
	snapshot := paths.Snapshot("passes")
	if err := os.Remove(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(snapshot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := service.Fail(value.ID, "storm"); err == nil {
		t.Fatal("fail unexpectedly committed with an unwritable snapshot")
	}
	current, found := service.Get(value.ID)
	if !found || current.Phase != Planned {
		t.Fatalf("uncommitted phase leaked into memory: %#v", current)
	}

	// After the snapshot is repaired the mutation commits again.
	if err := os.Remove(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := service.Fail(value.ID, "storm"); err != nil {
		t.Fatal(err)
	}
	current, _ = service.Get(value.ID)
	if current.Phase != Failed || current.FailureReason != "storm" {
		t.Fatalf("fail did not commit after repair: %#v", current)
	}
}
