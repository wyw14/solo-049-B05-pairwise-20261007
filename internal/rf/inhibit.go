package rf

import (
	"context"
	"fmt"
	"time"

	"orbitlink/internal/audit"
)

type InhibitProof struct {
	ChainID     string    `json:"chain_id"`
	Operation   string    `json:"operation"`
	Confirmed   bool      `json:"confirmed"`
	ConfirmedAt time.Time `json:"confirmed_at"`
}

func (c *Controller) Inhibit(ctx context.Context, chainID, operationID string) (InhibitProof, error) {
	if operationID == "" {
		return InhibitProof{}, fmt.Errorf("inhibit operation is required")
	}
	select {
	case <-ctx.Done():
		return InhibitProof{}, fmt.Errorf("RF inhibit canceled: %w", ctx.Err())
	default:
	}
	c.mu.Lock()
	state, found := c.states[chainID]
	if !found {
		c.mu.Unlock()
		return InhibitProof{}, fmt.Errorf("RF chain %s not found", chainID)
	}
	original := state
	state.TransmitEnabled = false
	state.InhibitOperation = operationID
	state.UpdatedAt = time.Now().UTC()
	c.states[chainID] = state
	err := c.flushLocked()
	if err != nil {
		c.states[chainID] = original
		c.mu.Unlock()
		return InhibitProof{}, err
	}
	c.mu.Unlock()
	proof := InhibitProof{ChainID: chainID, Operation: operationID, Confirmed: true, ConfirmedAt: state.UpdatedAt}
	_, _ = c.audit.Record(audit.Event{Component: "rf", Action: "transmit.inhibited", Subject: chainID, Operation: operationID})
	return proof, nil
}

func (c *Controller) VerifyInhibit(chainID, operationID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	state, found := c.states[chainID]
	return found && !state.TransmitEnabled && state.InhibitOperation == operationID
}
