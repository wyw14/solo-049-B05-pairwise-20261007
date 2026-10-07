package weather

import (
	"errors"
	"fmt"

	"orbitlink/internal/antenna"
	"orbitlink/internal/scheduler"
)

// PassFencer removes every scheduler artifact that would let a pass keep
// driving hardware after a storm stow.
type PassFencer interface {
	FencePass(passID, antennaID, cause string) error
}

// SchedulerFencer fences a pass across the command queue, the antenna
// timeline and fleet ownership. Every step is idempotent so the storm
// recovery log can replay fencing until it converges.
type SchedulerFencer struct {
	commands *scheduler.CommandQueue
	timeline *scheduler.Timeline
	fleet    *antenna.Fleet
}

func NewSchedulerFencer(commands *scheduler.CommandQueue, timeline *scheduler.Timeline, fleet *antenna.Fleet) *SchedulerFencer {
	return &SchedulerFencer{commands: commands, timeline: timeline, fleet: fleet}
}

// FencePass attempts all fencing steps and aggregates the failures instead of
// stopping at the first one: a partially fenced pass is still safer, and the
// recovery log tracks what is left.
func (f *SchedulerFencer) FencePass(passID, antennaID, cause string) error {
	var errs []error
	if _, err := f.commands.RetireForPass(passID, cause); err != nil {
		errs = append(errs, fmt.Errorf("retire pointing commands: %w", err))
	}
	if _, err := f.timeline.ReleasePass(passID); err != nil {
		errs = append(errs, fmt.Errorf("release reservations: %w", err))
	}
	if err := f.fleet.ReleaseOwner(antennaID, passID); err != nil {
		errs = append(errs, fmt.Errorf("release antenna ownership: %w", err))
	}
	return errors.Join(errs...)
}
