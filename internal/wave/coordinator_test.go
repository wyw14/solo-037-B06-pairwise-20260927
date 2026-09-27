package wave

import (
    "context"
    "testing"
    "time"

    "github.com/wyw14/solo-037/internal/model"
)

func newTestController() *Controller {
    return NewController()
}

func TestStopCompletesWithoutWaitingForContextCancel(t *testing.T) {
    controller := newTestController()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    controller.Start(ctx, model.RunID("run-1"), NewProfile(0.8, 0.15, 80*time.Millisecond), 1, time.Now())

    controller.Stop()
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out after explicit Stop")
    }
    if controller.Active() {
        t.Fatal("controller reports active after Stop completed")
    }
}

func TestContextCancelCompletesSameShutdown(t *testing.T) {
    controller := newTestController()
    ctx, cancel := context.WithCancel(context.Background())
    controller.Start(ctx, model.RunID("run-1"), NewProfile(0.8, 0.15, 80*time.Millisecond), 1, time.Now())

    cancel()
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out after context cancellation")
    }
    if controller.Active() {
        t.Fatal("controller reports active after context cancellation completed")
    }
}

func TestRepeatedStopDoesNotPanic(t *testing.T) {
    controller := newTestController()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    controller.Start(ctx, model.RunID("run-1"), NewProfile(0.8, 0.15, 80*time.Millisecond), 1, time.Now())

    controller.Stop()
    controller.Stop()
    controller.Stop()
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out after repeated Stop calls")
    }
}

func TestStopAndCancelInterleave(t *testing.T) {
    controller := newTestController()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    controller.Start(ctx, model.RunID("run-1"), NewProfile(0.8, 0.15, 80*time.Millisecond), 1, time.Now())

    done := make(chan bool, 1)
    go func() {
        controller.Stop()
        done <- true
    }()
    cancel()
    select {
    case <-done:
    case <-time.After(time.Second):
        t.Fatal("concurrent Stop did not return")
    }
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out when Stop and cancel interleaved")
    }
}
