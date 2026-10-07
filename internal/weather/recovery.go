package weather

import (
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/store"
)

// Recovery is the durable record of a storm stow whose hardware committed
// (RF inhibited, antenna stowed) while the pass bookkeeping has not. It is
// the recoverable state a partial failure parks in: inspectable, replayable,
// and cleared only once the pass can no longer be scheduled.
type Recovery struct {
	ID          string    `json:"id"`
	OperationID string    `json:"operation_id"`
	PassID      string    `json:"pass_id"`
	AntennaID   string    `json:"antenna_id"`
	ChainID     string    `json:"chain_id"`
	Reason      string    `json:"reason"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ResolvedAt  time.Time `json:"resolved_at,omitempty"`
	Resolved    bool      `json:"resolved"`
}

// ReconcileReport summarizes one replay of the pending recovery log.
type ReconcileReport struct {
	Resolved int      `json:"resolved"`
	Pending  int      `json:"pending"`
	Errors   []string `json:"errors,omitempty"`
}

// RecoveryStore persists recovery records on their own snapshot, independent
// of the pass store whose failure creates them. Records stay in memory when a
// flush fails so the pending state is never silently dropped.
type RecoveryStore struct {
	mu      sync.RWMutex
	path    string
	records map[string]Recovery
}

func NewRecoveryStore(paths store.Paths) (*RecoveryStore, error) {
	recoveryStore := &RecoveryStore{path: paths.Snapshot("storm-recoveries"), records: make(map[string]Recovery)}
	var values []Recovery
	found, err := store.ReadJSON(recoveryStore.path, &values)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range values {
			recoveryStore.records[value.ID] = value
		}
	}
	return recoveryStore, nil
}

// Begin writes the recovery intent ahead of the bookkeeping attempt, so a
// crash between the hardware commit and the pass update still leaves a
// replayable record.
func (s *RecoveryStore) Begin(record Recovery) (Recovery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.ID] = record
	return record, s.flushLocked()
}

// MarkAttempt records a failed bookkeeping attempt against a pending record.
func (s *RecoveryStore) MarkAttempt(id string, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[id]
	if !found {
		return fmt.Errorf("recovery %s not found", id)
	}
	record.Attempts++
	record.LastError = cause.Error()
	record.UpdatedAt = time.Now().UTC()
	s.records[id] = record
	return s.flushLocked()
}

// Resolve closes a record once its bookkeeping has converged.
func (s *RecoveryStore) Resolve(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[id]
	if !found {
		return fmt.Errorf("recovery %s not found", id)
	}
	record.Resolved = true
	record.ResolvedAt = time.Now().UTC()
	record.UpdatedAt = record.ResolvedAt
	s.records[id] = record
	return s.flushLocked()
}

func (s *RecoveryStore) Pending() []Recovery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var values []Recovery
	for _, record := range s.records {
		if !record.Resolved {
			values = append(values, record)
		}
	}
	return values
}

func (s *RecoveryStore) List() []Recovery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]Recovery, 0, len(s.records))
	for _, record := range s.records {
		values = append(values, record)
	}
	return values
}

func (s *RecoveryStore) flushLocked() error {
	values := make([]Recovery, 0, len(s.records))
	for _, record := range s.records {
		values = append(values, record)
	}
	return store.WriteJSON(s.path, values)
}
