package weather

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/rf"
)

type RFInhibiter interface {
	Inhibit(context.Context, string, string) (rf.InhibitProof, error)
	VerifyInhibit(string, string) bool
}

type AntennaStower interface {
	Stow(string, string, bool) error
}

// PartialFailureError reports a storm stow whose hardware committed while the
// pass bookkeeping did not. The referenced recovery record holds the pending
// work; the pass stays fenced until ReconcilePending converges it.
type PartialFailureError struct {
	RecoveryID string
	Err        error
}

func (e *PartialFailureError) Error() string {
	return fmt.Sprintf("storm stow committed but pass bookkeeping is pending recovery %s: %v", e.RecoveryID, e.Err)
}

func (e *PartialFailureError) Unwrap() error { return e.Err }

type Coordinator struct {
	threshold  float64
	rf         RFInhibiter
	antenna    AntennaStower
	passes     *passsvc.Service
	fencer     PassFencer
	recoveries *RecoveryStore
	audit      *audit.Trail
}

func NewCoordinator(threshold float64, controller RFInhibiter, stower AntennaStower, passes *passsvc.Service, fencer PassFencer, recoveries *RecoveryStore, trail *audit.Trail) *Coordinator {
	return &Coordinator{threshold: threshold, rf: controller, antenna: stower, passes: passes, fencer: fencer, recoveries: recoveries, audit: trail}
}

// Handle commits a storm stow across RF, antenna and pass bookkeeping in
// safety order: inhibit first, stow second, bookkeeping last. The pass
// bookkeeping is guarded by a write-ahead recovery record, so a failure at
// the last step parks the operation in a recoverable state instead of
// leaving a pass that still looks schedulable behind.
func (c *Coordinator) Handle(ctx context.Context, sample WindSample, passID, antennaID, chainID string) (StowResult, error) {
	result := StowResult{OperationID: uuid.NewString(), AntennaID: antennaID, ChainID: chainID}
	if sample.GustMetersPerSecond < c.threshold {
		return result, nil
	}
	proof, err := c.rf.Inhibit(ctx, chainID, result.OperationID)
	if err != nil {
		result.Error = err.Error()
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.inhibit-failed", Subject: antennaID, Severity: audit.Critical, Operation: result.OperationID, Fields: map[string]any{"error": err.Error()}})
		return result, fmt.Errorf("cannot confirm RF inhibit: %w", err)
	}
	result.InhibitConfirmed = proof.Confirmed && c.rf.VerifyInhibit(chainID, result.OperationID)
	if !result.InhibitConfirmed {
		result.Error = "RF inhibit proof does not match operation"
		return result, fmt.Errorf("%s", result.Error)
	}
	if err := c.antenna.Stow(antennaID, result.OperationID, true); err != nil {
		result.Error = err.Error()
		return result, err
	}
	result.Stowed = true
	if passID == "" {
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.completed", Subject: antennaID, Operation: result.OperationID, Fields: map[string]any{"gust_mps": sample.GustMetersPerSecond}})
		return result, nil
	}
	recovery, err := c.recoveries.Begin(Recovery{
		ID: uuid.NewString(), OperationID: result.OperationID, PassID: passID,
		AntennaID: antennaID, ChainID: chainID, Reason: "high wind safety stow",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.recovery-persist-failed", Subject: passID, Severity: audit.Critical, Operation: result.OperationID, Fields: map[string]any{"error": err.Error()}})
	}
	missing, err := c.completeBookkeeping(recovery)
	result.PassMissing = missing
	if err != nil {
		_ = c.recoveries.MarkAttempt(recovery.ID, err)
		result.RecoveryID = recovery.ID
		result.Error = err.Error()
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.bookkeeping-pending", Subject: passID, Severity: audit.Critical, Operation: result.OperationID, Fields: map[string]any{"recovery_id": recovery.ID, "error": err.Error()}})
		return result, &PartialFailureError{RecoveryID: recovery.ID, Err: err}
	}
	if err := c.recoveries.Resolve(recovery.ID); err != nil {
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.recovery-persist-failed", Subject: passID, Severity: audit.Critical, Operation: result.OperationID, Fields: map[string]any{"error": err.Error()}})
	}
	result.PassFailed = !missing
	_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.completed", Subject: antennaID, Operation: result.OperationID, Fields: map[string]any{"gust_mps": sample.GustMetersPerSecond}})
	return result, nil
}

// ReconcilePending replays every pending recovery record until its pass can
// no longer be scheduled. It runs at startup and on operator demand; every
// bookkeeping step is idempotent, so replay is always safe.
func (c *Coordinator) ReconcilePending() ReconcileReport {
	report := ReconcileReport{}
	for _, recovery := range c.recoveries.Pending() {
		if _, err := c.completeBookkeeping(recovery); err != nil {
			_ = c.recoveries.MarkAttempt(recovery.ID, err)
			report.Pending++
			report.Errors = append(report.Errors, fmt.Sprintf("recovery %s pass %s: %v", recovery.ID, recovery.PassID, err))
			_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.recovery-pending", Subject: recovery.PassID, Severity: audit.Warning, Operation: recovery.OperationID, Fields: map[string]any{"recovery_id": recovery.ID, "error": err.Error()}})
			continue
		}
		if err := c.recoveries.Resolve(recovery.ID); err != nil {
			report.Pending++
			report.Errors = append(report.Errors, fmt.Sprintf("recovery %s resolve: %v", recovery.ID, err))
			continue
		}
		report.Resolved++
		_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.recovered", Subject: recovery.PassID, Operation: recovery.OperationID, Fields: map[string]any{"recovery_id": recovery.ID}})
	}
	return report
}

func (c *Coordinator) PendingRecoveries() []Recovery {
	return c.recoveries.Pending()
}

func (c *Coordinator) ListRecoveries() []Recovery {
	return c.recoveries.List()
}

// completeBookkeeping is the commit boundary for the pass side of a storm
// stow: the pass is marked failed and fenced out of every scheduler structure
// that could keep driving hardware for it. A missing pass is vacuously
// complete — the pass store holds no fake running task — but its scheduler
// artifacts are still fenced.
func (c *Coordinator) completeBookkeeping(recovery Recovery) (passMissing bool, err error) {
	var errs []error
	if failErr := c.passes.Fail(recovery.PassID, recovery.Reason); failErr != nil {
		if errors.Is(failErr, passsvc.ErrNotFound) {
			passMissing = true
			_, _ = c.audit.Record(audit.Event{Component: "weather", Action: "stow.pass-missing", Subject: recovery.PassID, Severity: audit.Warning, Operation: recovery.OperationID})
		} else {
			errs = append(errs, fmt.Errorf("mark pass failed: %w", failErr))
		}
	}
	if c.fencer != nil {
		if fenceErr := c.fencer.FencePass(recovery.PassID, recovery.AntennaID, "storm stow "+recovery.OperationID); fenceErr != nil {
			errs = append(errs, fenceErr)
		}
	}
	return passMissing, errors.Join(errs...)
}

var _ AntennaStower = (*antenna.StowController)(nil)
