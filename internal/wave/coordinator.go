package wave

import (
    "context"
    "sync"
    "time"

    "github.com/wyw14/solo-037/internal/model"
)

type Controller struct {
    mu sync.Mutex
    run model.RunID
    commands []model.DeviceCommand
    active bool
    cancel context.CancelFunc
    done chan struct{}
}

func NewController() *Controller { return &Controller{done: make(chan struct{})} }

func (c *Controller) Start(ctx context.Context, run model.RunID, profile Profile, epoch uint64, at time.Time) {
    c.mu.Lock()
    if c.cancel != nil {
        c.cancel()
    }
    c.run = run
    c.active = true
    ctx, cancel := context.WithCancel(ctx)
    c.cancel = cancel
    done := make(chan struct{})
    c.done = done
    c.mu.Unlock()
    go c.loop(ctx, profile, epoch, at, done)
}

func (c *Controller) loop(ctx context.Context, profile Profile, epoch uint64, at time.Time, done chan struct{}) {
    ticker := time.NewTicker(5 * time.Millisecond)
    defer func() {
        ticker.Stop()
        c.mu.Lock()
        c.active = false
        c.mu.Unlock()
        close(done)
    }()
    index := 0
    samples := profile.Samples(profile.Duration / 3)
    for {
        select {
        case <-ctx.Done():
            return
        case now := <-ticker.C:
            c.mu.Lock()
            value := samples[index%len(samples)]
            c.commands = append(c.commands, model.DeviceCommand{ID: string(model.NewEventID()), Run: c.run, Epoch: epoch, Setpoint: value, IssuedAt: now.UTC()})
            index++
            c.mu.Unlock()
            _ = at
        }
    }
}

// Stop signals the background loop to finish. It is idempotent: stopping an
// already stopped controller, or racing with context cancellation, completes
// the same shutdown observable through WaitStopped without closing any
// channel twice.
func (c *Controller) Stop() {
    c.mu.Lock()
    c.active = false
    cancel := c.cancel
    c.mu.Unlock()
    if cancel != nil {
        cancel()
    }
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
