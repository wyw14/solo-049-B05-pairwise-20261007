package weather

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"orbitlink/internal/store"
)

// StowOperation is the durable record of one storm-handling saga. Every step
// that commits a side effect in another module is marked here so an
// interrupted operation can be driven to completion instead of leaving the
// antenna, RF chain and pass in divergent states.
type StowOperation struct {
	ID         string    `json:"id"`
	PassID     string    `json:"pass_id,omitempty"`
	AntennaID  string    `json:"antenna_id"`
	ChainID    string    `json:"chain_id"`
	GustMPS    float64   `json:"gust_mps"`
	Inhibited  bool      `json:"inhibited"`
	Stowed     bool      `json:"stowed"`
	PassFailed bool      `json:"pass_failed"`
	Resolved   bool      `json:"resolved"`
	Error      string    `json:"error,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// journal is the write-ahead record for storm operations. It follows the same
// check-and-publish discipline as scheduler.Timeline: an in-memory change is
// kept only when the snapshot write commits.
type journal struct {
	mu         sync.Mutex
	path       string
	operations map[string]StowOperation
}

func openJournal(path string) (*journal, error) {
	journal := &journal{path: path, operations: make(map[string]StowOperation)}
	var values []StowOperation
	found, err := store.ReadJSON(path, &values)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range values {
			journal.operations[value.ID] = value
		}
	}
	return journal, nil
}

func (j *journal) begin(operation StowOperation) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.operations[operation.ID]; exists {
		return fmt.Errorf("stow operation %s already exists", operation.ID)
	}
	operation.UpdatedAt = time.Now().UTC()
	j.operations[operation.ID] = operation
	if err := j.flushLocked(); err != nil {
		delete(j.operations, operation.ID)
		return err
	}
	return nil
}

func (j *journal) mark(id string, apply func(*StowOperation)) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	original, found := j.operations[id]
	if !found {
		return fmt.Errorf("stow operation %s not found", id)
	}
	updated := original
	apply(&updated)
	updated.UpdatedAt = time.Now().UTC()
	j.operations[id] = updated
	if err := j.flushLocked(); err != nil {
		j.operations[id] = original
		return err
	}
	return nil
}

func (j *journal) unresolved() []StowOperation {
	j.mu.Lock()
	defer j.mu.Unlock()
	values := make([]StowOperation, 0, len(j.operations))
	for _, value := range j.operations {
		if !value.Resolved {
			values = append(values, value)
		}
	}
	sort.Slice(values, func(a, b int) bool {
		if values[a].UpdatedAt.Equal(values[b].UpdatedAt) {
			return values[a].ID < values[b].ID
		}
		return values[a].UpdatedAt.Before(values[b].UpdatedAt)
	})
	return values
}

func (j *journal) flushLocked() error {
	values := make([]StowOperation, 0, len(j.operations))
	for _, value := range j.operations {
		values = append(values, value)
	}
	return store.WriteJSON(j.path, values)
}
