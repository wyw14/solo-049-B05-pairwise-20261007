package weather

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/rf"
	"orbitlink/internal/store"
)

type RFInhibiter interface {
	Inhibit(context.Context, string, string) (rf.InhibitProof, error)
	VerifyInhibit(string, string) bool
}

type AntennaStower interface {
	Stow(string, string, bool) error
}

type Coordinator struct {
	threshold float64
	rf        RFInhibiter
	antenna   AntennaStower
	passes    *passsvc.Service
	audit     *audit.Trail
	journal   *journal
}

func NewCoordinator(paths store.Paths, threshold float64, controller RFInhibiter, stower AntennaStower, passes *passsvc.Service, trail *audit.Trail) (*Coordinator, error) {
	journal, err := openJournal(paths.Snapshot("weather-stow"))
	if err != nil {
		return nil, err
	}
	return &Coordinator{threshold: threshold, rf: controller, antenna: stower, passes: passes, audit: trail, journal: journal}, nil
}

// Handle runs storm handling as one commit boundary across the rf, antenna and
// pass modules: validate every precondition before the first side effect,
// record durable intent, then commit step by step. A failure after any commit
// leaves an unresolved operation in the journal — the recoverable state that
// Recover drives to completion — instead of a pass that still looks runnable
// against stowed hardware.
func (c *Coordinator) Handle(ctx context.Context, sample WindSample, passID, antennaID, chainID string) (StowResult, error) {
	result := StowResult{OperationID: uuid.NewString(), AntennaID: antennaID, ChainID: chainID}
	// Finish previously interrupted operations before starting a new one.
	_ = c.Recover(ctx)
	if sample.GustMetersPerSecond < c.threshold {
		return result, nil
	}

	// Validate the bookkeeping target before any hardware side effect, so an
	// unknown pass fails the request without stowing anything.
	failNeeded := false
	if passID != "" {
		value, found := c.passes.Get(passID)
		if !found {
			result.Error = fmt.Sprintf("pass %s not found", passID)
			_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.rejected", Subject: antennaID, Severity: audit.Warning, Operation: result.OperationID, Fields: map[string]any{"error": result.Error}})
			return result, fmt.Errorf("%s", result.Error)
		}
		failNeeded = !value.Phase.Terminal()
	}

	operation := StowOperation{ID: result.OperationID, PassID: passID, AntennaID: antennaID, ChainID: chainID, GustMPS: sample.GustMetersPerSecond}
	if err := c.journal.begin(operation); err != nil {
		result.Error = err.Error()
		return result, fmt.Errorf("cannot record stow operation: %w", err)
	}

	proof, err := c.rf.Inhibit(ctx, chainID, result.OperationID)
	if err != nil {
		result.Error = err.Error()
		// The inhibit did not commit, so nothing is left to recover.
		_ = c.journal.mark(operation.ID, func(op *StowOperation) { op.Resolved = true; op.Error = err.Error() })
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.inhibit-failed", Subject: antennaID, Severity: audit.Critical, Operation: result.OperationID, Fields: map[string]any{"error": err.Error()}})
		return result, fmt.Errorf("cannot confirm RF inhibit: %w", err)
	}
	result.InhibitConfirmed = proof.Confirmed && c.rf.VerifyInhibit(chainID, result.OperationID)
	if !result.InhibitConfirmed {
		result.Error = "RF inhibit proof does not match operation"
		c.notePartial(operation.ID, antennaID, fmt.Errorf("%s", result.Error))
		return result, fmt.Errorf("%s", result.Error)
	}
	if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.Inhibited = true }); err != nil {
		result.Error = err.Error()
		return result, fmt.Errorf("cannot record RF inhibit: %w", err)
	}
	if err := c.antenna.Stow(antennaID, result.OperationID, true); err != nil {
		result.Error = err.Error()
		c.notePartial(operation.ID, antennaID, err)
		return result, err
	}
	result.Stowed = true
	if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.Stowed = true }); err != nil {
		result.Error = err.Error()
		return result, fmt.Errorf("cannot record antenna stow: %w", err)
	}
	if failNeeded {
		if err := c.passes.Fail(passID, "high wind safety stow"); err != nil {
			result.Error = err.Error()
			c.notePartial(operation.ID, antennaID, err)
			return result, fmt.Errorf("pass fail did not commit; stow operation %s stays recoverable: %w", operation.ID, err)
		}
		result.PassFailed = true
		if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.PassFailed = true }); err != nil {
			result.Error = err.Error()
			c.notePartial(operation.ID, antennaID, err)
			return result, fmt.Errorf("cannot record pass failure; stow operation %s stays recoverable: %w", operation.ID, err)
		}
	}
	if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.Resolved = true; op.Error = "" }); err != nil {
		result.Error = err.Error()
		return result, fmt.Errorf("cannot close stow operation %s: %w", operation.ID, err)
	}
	_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.completed", Subject: antennaID, Operation: result.OperationID, Fields: map[string]any{"gust_mps": sample.GustMetersPerSecond}})
	return result, nil
}

// Pending returns the storm operations still waiting to be driven to completion.
func (c *Coordinator) Pending() []StowOperation {
	return c.journal.unresolved()
}

// Recover drives every unresolved stow operation to completion. Each step is
// confirmed before it is re-applied, so recovery is idempotent and safe to run
// at startup and before every new storm operation.
func (c *Coordinator) Recover(ctx context.Context) error {
	for _, operation := range c.journal.unresolved() {
		if err := c.recoverOperation(ctx, operation); err != nil {
			_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.recover-failed", Subject: operation.AntennaID, Severity: audit.Critical, Operation: operation.ID, Fields: map[string]any{"error": err.Error()}})
			return err
		}
	}
	return nil
}

func (c *Coordinator) recoverOperation(ctx context.Context, operation StowOperation) error {
	if !c.rf.VerifyInhibit(operation.ChainID, operation.ID) {
		if _, err := c.rf.Inhibit(ctx, operation.ChainID, operation.ID); err != nil {
			return fmt.Errorf("recover RF inhibit: %w", err)
		}
	}
	if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.Inhibited = true }); err != nil {
		return err
	}
	if !operation.Stowed {
		if err := c.antenna.Stow(operation.AntennaID, operation.ID, true); err != nil {
			return fmt.Errorf("recover antenna stow: %w", err)
		}
		if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.Stowed = true }); err != nil {
			return err
		}
	}
	if operation.PassID != "" && !operation.PassFailed {
		// A missing or already terminal pass can no longer be failed — and can
		// no longer be scheduled either — so there is nothing left to recover.
		if value, found := c.passes.Get(operation.PassID); found && !value.Phase.Terminal() {
			if err := c.passes.Fail(operation.PassID, "high wind safety stow (recovered)"); err != nil {
				return fmt.Errorf("recover pass fail: %w", err)
			}
			if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.PassFailed = true }); err != nil {
				return err
			}
		}
	}
	if err := c.journal.mark(operation.ID, func(op *StowOperation) { op.Resolved = true; op.Error = "" }); err != nil {
		return err
	}
	_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.recovered", Subject: operation.AntennaID, Operation: operation.ID, Fields: map[string]any{"pass_id": operation.PassID}})
	return nil
}

// notePartial records a failure that happened after at least one side effect
// committed. The operation stays unresolved so Recover can finish the saga.
func (c *Coordinator) notePartial(operationID, antennaID string, cause error) {
	_ = c.journal.mark(operationID, func(op *StowOperation) { op.Error = cause.Error() })
	_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.partial", Subject: antennaID, Severity: audit.Critical, Operation: operationID, Fields: map[string]any{"error": cause.Error()}})
}

var _ AntennaStower = (*antenna.StowController)(nil)
