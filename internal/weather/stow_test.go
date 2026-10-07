package weather_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/store"
	"orbitlink/internal/weather"
)

type fakeRF struct {
	inhibited    map[string]string
	inhibitCalls int
}

func (f *fakeRF) Inhibit(_ context.Context, chainID, operationID string) (rf.InhibitProof, error) {
	f.inhibitCalls++
	f.inhibited[chainID] = operationID
	return rf.InhibitProof{ChainID: chainID, Operation: operationID, Confirmed: true, ConfirmedAt: time.Now().UTC()}, nil
}

func (f *fakeRF) VerifyInhibit(chainID, operationID string) bool {
	return f.inhibited[chainID] == operationID
}

type fakeStower struct {
	stowed map[string]bool
	calls  int
}

func (f *fakeStower) Stow(antennaID, _ string, inhibitConfirmed bool) error {
	f.calls++
	if !inhibitConfirmed {
		return fmt.Errorf("RF inhibit confirmation is required before stow")
	}
	f.stowed[antennaID] = true
	return nil
}

func TestHandleCommitsStormSaga(t *testing.T) {
	coordinator, passes, _, _, _ := newStormRig(t)
	value := createPass(t, passes)
	result, err := coordinator.Handle(context.Background(), gust(25), value.ID, value.AntennaID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if !result.InhibitConfirmed || !result.Stowed || !result.PassFailed {
		t.Fatalf("storm saga did not complete: %#v", result)
	}
	current, _ := passes.Get(value.ID)
	if current.Phase != passsvc.Failed {
		t.Fatalf("pass was not failed: %s", current.Phase)
	}
	if pending := coordinator.Pending(); len(pending) != 0 {
		t.Fatalf("completed saga left pending operations: %#v", pending)
	}
}

func TestHandleRejectsUnknownPassBeforeSideEffects(t *testing.T) {
	coordinator, _, radio, stower, _ := newStormRig(t)
	_, err := coordinator.Handle(context.Background(), gust(25), "pass-missing", "ANT-01", "RF-01")
	if err == nil {
		t.Fatal("expected an error for an unknown pass")
	}
	if radio.inhibitCalls != 0 || stower.calls != 0 {
		t.Fatalf("hardware side effects happened before validation: inhibits=%d stows=%d", radio.inhibitCalls, stower.calls)
	}
	if pending := coordinator.Pending(); len(pending) != 0 {
		t.Fatalf("rejected request left a journal operation: %#v", pending)
	}
}

func TestHandleLeavesRecoverableOperationWhenPassCommitFails(t *testing.T) {
	coordinator, passes, _, _, paths := newStormRig(t)
	value := createPass(t, passes)
	sabotagePassSnapshot(t, paths)

	result, err := coordinator.Handle(context.Background(), gust(30), value.ID, value.AntennaID, "RF-01")
	if err == nil || !strings.Contains(err.Error(), "recoverable") {
		t.Fatalf("expected a recoverable commit failure, got: %v", err)
	}
	if !result.InhibitConfirmed || !result.Stowed || result.PassFailed {
		t.Fatalf("unexpected partial result: %#v", result)
	}
	// The failed pass commit must not leak into memory...
	current, _ := passes.Get(value.ID)
	if current.Phase != passsvc.Planned {
		t.Fatalf("failed pass commit leaked into memory: %s", current.Phase)
	}
	// ...and the interrupted saga must be durably pending recovery.
	if pending := coordinator.Pending(); len(pending) != 1 {
		t.Fatalf("expected one recoverable operation, got %#v", pending)
	}

	repairPassSnapshot(t, paths)
	if err := coordinator.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, _ = passes.Get(value.ID)
	if current.Phase != passsvc.Failed {
		t.Fatalf("recovery did not fail the pass: %s", current.Phase)
	}
	if pending := coordinator.Pending(); len(pending) != 0 {
		t.Fatalf("operation still pending after recovery: %#v", pending)
	}
	// The failed phase must be durable — a restarted service must not
	// resurrect the pass as runnable.
	reloaded, err := passsvc.NewService(paths, nil, audit.NewTrail(store.NewAppendLog(paths)))
	if err != nil {
		t.Fatal(err)
	}
	current, found := reloaded.Get(value.ID)
	if !found || current.Phase != passsvc.Failed {
		t.Fatalf("pass resurrected after reload: %#v", current)
	}
}

func TestRecoverResolvesOperationWhosePassIsGone(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	passes, err := passsvc.NewService(paths, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	// Seed an interrupted operation as if a crash happened right after the
	// hardware commits for a pass that no longer exists.
	operations := []weather.StowOperation{{ID: "op-orphan", PassID: "pass-gone", AntennaID: "ANT-01", ChainID: "RF-01", Inhibited: true, Stowed: true, UpdatedAt: time.Now().UTC()}}
	if err := store.WriteJSON(paths.Snapshot("weather-stow"), operations); err != nil {
		t.Fatal(err)
	}
	radio := &fakeRF{inhibited: map[string]string{}}
	coordinator, err := weather.NewCoordinator(paths, 20, radio, &fakeStower{stowed: map[string]bool{}}, passes, trail)
	if err != nil {
		t.Fatal(err)
	}
	if pending := coordinator.Pending(); len(pending) != 1 {
		t.Fatalf("interrupted operation was not loaded: %#v", pending)
	}
	if err := coordinator.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pending := coordinator.Pending(); len(pending) != 0 {
		t.Fatalf("orphaned operation was not resolved: %#v", pending)
	}
	// Recovery after a restart must re-confirm the inhibit for the operation.
	if !radio.VerifyInhibit("RF-01", "op-orphan") {
		t.Fatal("recovery did not re-confirm the RF inhibit")
	}
}

func gust(metersPerSecond float64) weather.WindSample {
	return weather.WindSample{StationID: "station", GustMetersPerSecond: metersPerSecond, ObservedAt: time.Now().UTC()}
}

func newStormRig(t *testing.T) (*weather.Coordinator, *passsvc.Service, *fakeRF, *fakeStower, store.Paths) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	passes, err := passsvc.NewService(paths, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	radio := &fakeRF{inhibited: map[string]string{}}
	stower := &fakeStower{stowed: map[string]bool{}}
	coordinator, err := weather.NewCoordinator(paths, 20, radio, stower, passes, trail)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, passes, radio, stower, paths
}

func createPass(t *testing.T, passes *passsvc.Service) passsvc.Pass {
	t.Helper()
	value, err := passes.Create(passsvc.CreateRequest{
		SatelliteID: "SAT-STORM", AntennaID: "ANT-01",
		RFChains: []string{"RF-01"}, RequiredReceiverChains: []string{"RF-01"},
		AOS: time.Now().UTC().Add(time.Hour), LOS: time.Now().UTC().Add(2 * time.Hour),
		Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// sabotagePassSnapshot makes the pass snapshot unwritable: renaming a file
// onto an existing directory fails, so every flush returns an error.
func sabotagePassSnapshot(t *testing.T, paths store.Paths) {
	t.Helper()
	snapshot := paths.Snapshot("passes")
	if err := os.Remove(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(snapshot, 0o755); err != nil {
		t.Fatal(err)
	}
}

func repairPassSnapshot(t *testing.T, paths store.Paths) {
	t.Helper()
	if err := os.Remove(paths.Snapshot("passes")); err != nil {
		t.Fatal(err)
	}
}
