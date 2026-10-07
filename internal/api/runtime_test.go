package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"orbitlink/internal/antenna"
	stationclock "orbitlink/internal/clock"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/rf"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/weather"
)

func TestOperationsPagesAndAPIsUseRuntimeState(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	for _, target := range []string{"/passes", "/antennas", "/rf-chains", "/recordings", "/healthz", "/api/passes", "/api/antennas", "/api/rf-chains", "/api/recordings"} {
		response, err := server.Client().Get(server.URL + target)
		if err != nil {
			t.Fatalf("GET %s: %v", target, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s returned %d", target, response.StatusCode)
		}
		_ = response.Body.Close()
	}
}

func TestPassRevisionAcquisitionAndRecordingFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-01", CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-02", CenterHz: 2_245_000_000, DopplerHz: 12_050, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	decision, err := runtime.Passes.AcceptDiversity(value.ID, results)
	if err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	session, err := runtime.NewDecoderSession(value.ID, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if written, err := session.Feed(attempt.ID, make([]byte, 128)); err != nil || written != 2 {
		t.Fatalf("decoder feed wrote %d frames: %v", written, err)
	}
	if _, err := runtime.Recorder.Drain(context.Background(), active.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Recorder.Seal(active.ID, "integration complete"); err != nil {
		t.Fatal(err)
	}
}

func TestEphemerisClockAndWindSafetyFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	previous, _ := runtime.Ephemerides.Current(value.SatelliteID)
	if _, err := runtime.Ephemerides.Import(satellite.Ephemeris{SatelliteID: value.SatelliteID, Revision: previous.Revision + 1, Epoch: time.Now().UTC(), Line1: "revised orbital element one", Line2: "revised orbital element two"}); err != nil {
		t.Fatal(err)
	}
	commands := runtime.Commands.List(value.ID)
	if len(commands) == 0 || commands[len(commands)-1].Revision != previous.Revision+1 {
		t.Fatal("revised pointing commands were not queued")
	}
	if _, err := runtime.Clock.Apply(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 2 * time.Second, Source: "gps", Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	if len(runtime.Clock.Corrections()) != 1 {
		t.Fatal("clock correction was not persisted")
	}
	if err := runtime.RF.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: time.Now().UTC()}, value.ID, value.AntennaID, "RF-01")
	if err != nil || !result.InhibitConfirmed || !result.Stowed {
		t.Fatalf("wind safety did not converge: %#v %v", result, err)
	}
	if !result.PassFailed {
		t.Fatalf("pass should be marked failed after a committed storm stow: %#v", result)
	}
	assertPassFenced(t, runtime, value.ID, value.AntennaID)
	if len(runtime.Weather.PendingRecoveries()) != 0 {
		t.Fatal("committed storm stow must not leave a pending recovery")
	}
}

func TestRouterRejectsOverlappingReservation(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.AOS.Add(time.Minute), LOS: first.Window.LOS.Add(time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err == nil {
		t.Fatal("overlapping reservation unexpectedly succeeded")
	}
}

// A storm stow for a pass that no longer exists must still fence whatever
// scheduler artifacts reference it, and must not report a fake failure: the
// hardware is safe and there is no pass record left to lie about.
func TestStormStowWithMissingPassFencesSchedulerArtifacts(t *testing.T) {
	runtime := newTestRuntime(t)
	now := time.Now().UTC()
	commands, err := antenna.BuildCommands("ghost-pass", "ANT-02", 1, []antenna.TrackPoint{
		{At: now.Add(time.Minute), Azimuth: 100, Elevation: 10},
		{At: now.Add(2 * time.Minute), Azimuth: 120, Elevation: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Commands.Replace("ghost-pass", 1, commands); err != nil {
		t.Fatal(err)
	}
	reservation, err := runtime.Timeline.Reserve("ghost-pass", "ANT-02", now.Add(-time.Minute), now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Antennas.Claim(reservation); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: now}, "ghost-pass", "ANT-02", "RF-02")
	if err != nil {
		t.Fatalf("missing pass must not fail the committed stow: %v", err)
	}
	if !result.Stowed || !result.InhibitConfirmed || !result.PassMissing || result.PassFailed {
		t.Fatalf("unexpected stow result for missing pass: %#v", result)
	}
	assertPassFenced(t, runtime, "ghost-pass", "ANT-02")
	if state, _ := runtime.Antennas.State("ANT-02"); state.Mode != antenna.Stowed {
		t.Fatalf("stowed antenna must stay stowed, got %s", state.Mode)
	}
	if pending := runtime.Weather.PendingRecoveries(); len(pending) != 0 {
		t.Fatalf("missing pass bookkeeping is vacuous, expected no pending recovery: %#v", pending)
	}
}

// When the pass snapshot cannot be persisted, the committed stow must park
// the pass bookkeeping in the recovery log instead of leaving a pass that
// still looks schedulable.
func TestStormStowPassPersistFailureParksRecovery(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	value := runtime.Passes.List()[0]
	restore := obstructPassSnapshot(t, runtime.StateRoot)

	response := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/antennas/"+value.AntennaID+"/stow", `{"pass_id":"`+value.ID+`","chain_id":"RF-01","gust_meters_per_second":25}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("partial storm stow should report conflict, got %d", response.StatusCode)
	}
	var result weather.StowResult
	decodeBody(t, response, &result)
	if !result.Stowed || !result.InhibitConfirmed || result.PassFailed || result.RecoveryID == "" {
		t.Fatalf("partial stow result must show committed hardware and a recovery: %#v", result)
	}
	if pass, _ := runtime.Passes.Get(value.ID); pass.Phase != passsvc.Failed {
		t.Fatalf("in-memory pass must fail closed, got %s", pass.Phase)
	}
	if pass := readPersistedPass(t, filepath.Join(runtime.StateRoot, "snapshots", "passes.json.bak"), value.ID); pass.Phase != passsvc.Planned {
		t.Fatalf("stale snapshot should still show the pre-stow phase, got %s", pass.Phase)
	}
	assertPassFenced(t, runtime, value.ID, value.AntennaID)
	if pending := runtime.Weather.PendingRecoveries(); len(pending) != 1 || pending[0].PassID != value.ID {
		t.Fatalf("expected one pending recovery for the pass: %#v", pending)
	}
	if health := runtime.Health(); health.Components["weather"] != "storm-recovery-pending:1" {
		t.Fatalf("health should surface the pending recovery: %#v", health.Components)
	}

	restore()
	report := runtime.Weather.ReconcilePending()
	if report.Resolved != 1 || report.Pending != 0 {
		t.Fatalf("reconcile should resolve the parked recovery: %#v", report)
	}
	if pass := readPersistedPass(t, filepath.Join(runtime.StateRoot, "snapshots", "passes.json"), value.ID); pass.Phase != passsvc.Failed {
		t.Fatalf("reconciled pass must be failed on disk, got %s", pass.Phase)
	}
}

// A restart with a stale pass snapshot must not resurrect a schedulable pass:
// the pending recovery log replays the bookkeeping at startup.
func TestStormRecoveryReconcilesAfterRestart(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	restore := obstructPassSnapshot(t, runtime.StateRoot)
	_, handleErr := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: time.Now().UTC()}, value.ID, value.AntennaID, "RF-01")
	var partial *weather.PartialFailureError
	if !errors.As(handleErr, &partial) || partial.RecoveryID == "" {
		t.Fatalf("expected a partial failure naming the parked recovery, got %v", handleErr)
	}
	restore()

	restarted, err := NewRuntime(runtime.StateRoot)
	if err != nil {
		t.Fatal(err)
	}
	if pass, _ := restarted.Passes.Get(value.ID); pass.Phase != passsvc.Failed {
		t.Fatalf("startup reconcile must fail the resurrected pass, got %s", pass.Phase)
	}
	if pending := restarted.Weather.PendingRecoveries(); len(pending) != 0 {
		t.Fatalf("startup reconcile should drain the recovery log: %#v", pending)
	}
	assertPassFenced(t, restarted, value.ID, value.AntennaID)
}

// obstructPassSnapshot makes the pass snapshot unwritable and returns a
// restore function that puts the stale pre-stow snapshot back.
func obstructPassSnapshot(t *testing.T, stateRoot string) func() {
	t.Helper()
	path := filepath.Join(stateRoot, "snapshots", "passes.json")
	if err := os.Rename(path, path+".bak"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".bak", path); err != nil {
			t.Fatal(err)
		}
	}
}

// assertPassFenced verifies that nothing in the scheduler can still drive
// hardware for the pass: no live pointing commands, no live reservation, no
// antenna ownership.
func assertPassFenced(t *testing.T, runtime *Runtime, passID, antennaID string) {
	t.Helper()
	for _, command := range runtime.Commands.List(passID) {
		if !command.Retired {
			t.Fatalf("pointing command %s for pass %s is still live", command.ID, passID)
		}
	}
	for _, command := range runtime.Commands.Due(time.Now().Add(24 * time.Hour)) {
		if command.PassID == passID {
			t.Fatalf("fenced pass %s still has due pointing command %s", passID, command.ID)
		}
	}
	for _, interval := range runtime.Timeline.List(antennaID) {
		if interval.PassID == passID && interval.Status != scheduler.Released {
			t.Fatalf("reservation %s for pass %s is still %s", interval.ReservationID, passID, interval.Status)
		}
	}
	if state, found := runtime.Antennas.State(antennaID); found && state.OwnerPassID == passID {
		t.Fatalf("antenna %s is still owned by fenced pass %s", antennaID, passID)
	}
}

func readPersistedPass(t *testing.T, path string, passID string) passsvc.Pass {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var passes []passsvc.Pass
	if err := json.Unmarshal(data, &passes); err != nil {
		t.Fatal(err)
	}
	for _, value := range passes {
		if value.ID == passID {
			return value
		}
	}
	t.Fatalf("pass %s not found in %s", passID, path)
	return passsvc.Pass{}
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func decodeBody(t *testing.T, response *http.Response, value any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, target, payload string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, target, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
