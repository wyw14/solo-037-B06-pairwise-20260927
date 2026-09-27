package wave

import (
    "context"
    "sync"
    "time"

    "github.com/wyw14/solo-037/internal/model"
)

// loopSignals carries the stop request for a single background loop generation.
// The once guard makes repeated stops idempotent: closing an already-closed
// channel would panic, so every stop path must go through requestStop.
type loopSignals struct {
    stop chan struct{}
    once sync.Once
}

func newLoopSignals() *loopSignals {
    return &loopSignals{stop: make(chan struct{})}
}

func (s *loopSignals) requestStop() {
    s.once.Do(func() { close(s.stop) })
}

type Controller struct {
    mu sync.Mutex
    run model.RunID
    commands []model.DeviceCommand
    active bool
    signals *loopSignals
    done chan struct{}
}

func NewController() *Controller {
    return &Controller{signals: newLoopSignals(), done: make(chan struct{})}
}

func (c *Controller) Start(ctx context.Context, run model.RunID, profile Profile, epoch uint64, at time.Time) {
    c.mu.Lock()
    signals := newLoopSignals()
    done := make(chan struct{})
    c.run = run
    c.active = true
    c.signals = signals
    c.done = done
    c.mu.Unlock()
    go c.loop(ctx, profile, epoch, at, signals, done)
}

func (c *Controller) loop(ctx context.Context, profile Profile, epoch uint64, at time.Time, signals *loopSignals, done chan struct{}) {
    ticker := time.NewTicker(5 * time.Millisecond)
    defer ticker.Stop()
    // Both context cancellation and an explicit stop converge on this single
    // teardown, so WaitStopped observes the same finished state no matter which
    // signal ended the loop.
    defer c.finish(done)
    index := 0
    samples := profile.Samples(profile.Duration / 3)
    for {
        select {
        case <-ctx.Done():
            return
        case <-signals.stop:
            return
        case now := <-ticker.C:
            c.mu.Lock()
            if !c.active {
                c.mu.Unlock()
                return
            }
            value := samples[index%len(samples)]
            c.commands = append(c.commands, model.DeviceCommand{ID: string(model.NewEventID()), Run: c.run, Epoch: epoch, Setpoint: value, IssuedAt: now.UTC()})
            index++
            c.mu.Unlock()
            _ = at
        }
    }
}

// finish marks the loop generation as finished. It only touches controller
// state when this loop is still the current generation, so a loop from a
// superseded Start cannot clobber a newer run or close its done channel.
func (c *Controller) finish(done chan struct{}) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.done != done {
        return
    }
    c.active = false
    close(done)
}

func (c *Controller) Stop() {
    c.mu.Lock()
    c.active = false
    signals := c.signals
    c.mu.Unlock()
    signals.requestStop()
}

func (c *Controller) Active() bool {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.active
}

func (c *Controller) Commands() []model.DeviceCommand {
    c.mu.Lock()
    defer c.mu.Unlock()
    result := make([]model.DeviceCommand, len(c.commands))
    copy(result, c.commands)
    return result
}

func (c *Controller) WaitStopped(timeout time.Duration) bool {
    c.mu.Lock()
    done := c.done
    c.mu.Unlock()
    select {
    case <-done:
        return true
    case <-time.After(timeout):
        return false
    }
}
