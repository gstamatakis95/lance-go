package lance

// Tests for context cancellation of FFI calls (ffiCall's cancel-token
// plumbing in errors.go). They drive the native test export
// lance_test_cancellable_sleep through the unexported wrapper
// testCancellableSleep (bottom of errors.go; _test.go files cannot use cgo).

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestFFICallPreCancelledContext verifies a context that is already done
// fails fast with context.Canceled, without entering the native call.
func TestFFICallPreCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	// A 30s sleep: if the pre-check were broken this would block the test.
	err := testCancellableSleep(ctx, 30_000)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("pre-cancelled call took %v, expected immediate return", elapsed)
	}
}

// TestFFICallMidCallCancel verifies cancelling the context while the native
// call is blocked aborts it promptly and surfaces context.Canceled.
func TestFFICallMidCallCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	start := time.Now()
	go func() { errCh <- testCancellableSleep(ctx, 30_000) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("cancelled call returned after %v, want prompt abort", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled FFI call did not return within 10s")
	}
}

// TestFFICallCancelDeadlineExceeded verifies a context deadline aborts the
// native call and surfaces context.DeadlineExceeded.
func TestFFICallCancelDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := testCancellableSleep(ctx, 30_000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("deadline-bound call returned after %v, want prompt abort", elapsed)
	}
}

// TestFFICallNoCancelBackgroundCompletes verifies a non-cancellable context
// (Done() == nil) skips the token machinery and the call completes normally.
func TestFFICallNoCancelBackgroundCompletes(t *testing.T) {
	if err := testCancellableSleep(context.Background(), 10); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

// TestFFICallCancelStress shakes the watcher-goroutine / token-free teardown
// ordering under -race: many iterations of tiny sleeps racing tiny
// cancels/timeouts, so completion, cancellation, and deadline paths all
// interleave with the teardown.
func TestFFICallCancelStress(t *testing.T) {
	for i := 0; i < 50; i++ {
		var ctx context.Context
		var cancel context.CancelFunc
		if i%2 == 0 {
			ctx, cancel = context.WithCancel(context.Background())
			go func() {
				time.Sleep(time.Duration(i%5) * time.Millisecond)
				cancel()
			}()
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), time.Duration(i%7)*time.Millisecond)
		}

		err := testCancellableSleep(ctx, uint64(i%4)) // 0-3ms sleeps
		if err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, ErrCanceled) {
			t.Fatalf("iteration %d: unexpected error %v", i, err)
		}
		cancel()
	}
}
