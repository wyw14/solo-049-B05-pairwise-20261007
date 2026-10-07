package antenna

import (
	"fmt"
	"sync"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

type Fleet struct {
	mu       sync.RWMutex
	path     string
	states   map[string]State
	timeline *scheduler.Timeline
	audit    *audit.Trail
}

func NewFleet(paths store.Paths, timeline *scheduler.Timeline, trail *audit.Trail, ids []string) (*Fleet, error) {
	fleet := &Fleet{path: paths.Snapshot("antennas"), states: make(map[string]State), timeline: timeline, audit: trail}
	var states []State
	found, err := store.ReadJSON(fleet.path, &states)
	if err != nil {
		return nil, err
	}
	if found {
		for _, state := range states {
			fleet.states[state.ID] = state
		}
	}
	for _, id := range ids {
		if _, exists := fleet.states[id]; !exists {
			fleet.states[id] = State{ID: id, Mode: Idle, Azimuth: 180, Elevation: 15}
		}
	}
	if err := fleet.flushLocked(); err != nil {
		return nil, err
	}
	return fleet, nil
}

func (f *Fleet) Claim(reservation scheduler.Interval) (State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, found := f.states[reservation.AntennaID]
	if !found {
		return State{}, fmt.Errorf("antenna %s not found", reservation.AntennaID)
	}
	if state.OwnerPassID != "" && state.OwnerPassID != reservation.PassID {
		return State{}, fmt.Errorf("antenna %s is owned by pass %s", state.ID, state.OwnerPassID)
	}
	state.OwnerPassID = reservation.PassID
	state.OwnerGeneration = reservation.Generation
	f.states[state.ID] = state
	if err := f.flushLocked(); err != nil {
		return State{}, err
	}
	_, _ = f.audit.Record(audit.Event{Component: "antenna", Action: "claimed", Subject: state.ID, Fields: map[string]any{"pass_id": state.OwnerPassID, "generation": state.OwnerGeneration}})
	return state, nil
}

func (f *Fleet) Release(antennaID, passID string, generation int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, found := f.states[antennaID]
	if !found {
		return fmt.Errorf("antenna %s not found", antennaID)
	}
	if state.OwnerPassID != passID || state.OwnerGeneration != generation {
		return fmt.Errorf("antenna owner generation changed")
	}
	state.OwnerPassID = ""
	state.OwnerGeneration = 0
	state.Mode = Idle
	f.states[antennaID] = state
	return f.flushLocked()
}

// ReleaseOwner clears ownership held by a pass without touching the antenna
// mode. Unlike Release it requires no generation match and never flips the
// antenna back to Idle: a storm-stowed antenna must stay stowed, it just must
// no longer accept pointing commands from the fenced pass.
func (f *Fleet) ReleaseOwner(antennaID, passID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, found := f.states[antennaID]
	if !found {
		return fmt.Errorf("antenna %s not found", antennaID)
	}
	if state.OwnerPassID != passID {
		return nil
	}
	state.OwnerPassID = ""
	state.OwnerGeneration = 0
	f.states[antennaID] = state
	return f.flushLocked()
}

func (f *Fleet) State(id string) (State, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state, found := f.states[id]
	return state, found
}

func (f *Fleet) List() []State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	values := make([]State, 0, len(f.states))
	for _, state := range f.states {
		values = append(values, state)
	}
	return values
}

func (f *Fleet) update(state State) error {
	f.states[state.ID] = state
	return f.flushLocked()
}

func (f *Fleet) flushLocked() error {
	values := make([]State, 0, len(f.states))
	for _, state := range f.states {
		values = append(values, state)
	}
	return store.WriteJSON(f.path, values)
}
