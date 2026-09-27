package wave

import (
    "context"
    "testing"
    "time"

    "github.com/wyw14/solo-037/internal/model"
)

func testProfile() Profile {
    return NewProfile(0.8, 0.15, 80*time.Millisecond)
}

func TestStopClosesDone(t *testing.T) {
    controller := NewController()
    controller.Start(context.Background(), model.RunID("run-1"), testProfile(), 1, time.Now())

    controller.Stop()
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out after explicit Stop")
    }
    if controller.Active() {
        t.Fatal("controller still active after stop confirmed")
    }
}

func TestRepeatedStopDoesNotPanic(t *testing.T) {
    controller := NewController()
    controller.Start(context.Background(), model.RunID("run-1"), testProfile(), 1, time.Now())

    controller.Stop()
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out after first Stop")
    }
    controller.Stop()
    controller.Stop()
}

func TestContextCancelConvergesOnDone(t *testing.T) {
    controller := NewController()
    ctx, cancel := context.WithCancel(context.Background())
    controller.Start(ctx, model.RunID("run-1"), testProfile(), 1, time.Now())

    cancel()
    if !controller.WaitStopped(time.Second) {
        t.Fatal("WaitStopped timed out after context cancellation")
    }
    // Stop after cancellation must remain idempotent.
    controller.Stop()
}

func TestRestartAfterStop(t *testing.T) {
    controller := NewController()
    for attempt := 0; attempt < 3; attempt++ {
        controller.Start(context.Background(), model.RunID("run-1"), testProfile(), 1, time.Now())
        controller.Stop()
        if !controller.WaitStopped(time.Second) {
            t.Fatalf("attempt %d: WaitStopped timed out", attempt)
        }
    }
}
