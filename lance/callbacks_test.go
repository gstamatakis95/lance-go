// White-box tests for the callback/plugin bridge. In package lance (not
// lance_test) because the bridge is package-internal, and the cgo-touching
// pieces live in callbacks.go: cgo is not permitted in _test.go files.
package lance

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// funcPlugin adapts a function to the plugin interface.
type funcPlugin func(method int32, payload []byte) ([]byte, error)

func (f funcPlugin) Invoke(method int32, payload []byte) ([]byte, error) {
	return f(method, payload)
}

// echoPlugin responds with "m<method>:<payload>".
func echoPlugin(method int32, payload []byte) ([]byte, error) {
	return append(fmt.Appendf(nil, "m%d:", method), payload...), nil
}

// registerTestPlugin registers f and releases it at test cleanup.
func registerTestPlugin(t *testing.T, f funcPlugin) uintptr {
	t.Helper()
	h := registerPlugin(f)
	t.Cleanup(func() { releasePlugin(h) })
	return h
}

func TestCallbackRoundtripEcho(t *testing.T) {
	h := registerTestPlugin(t, echoPlugin)
	out, err := testCallbackRoundtrip(h, 7, []byte("hello"), false)
	if err != nil {
		t.Fatalf("roundtrip failed: %v", err)
	}
	if want := []byte("m7:hello"); !bytes.Equal(out, want) {
		t.Fatalf("roundtrip = %q, want %q", out, want)
	}
}

func TestCallbackAsyncRoundtrip(t *testing.T) {
	h := registerTestPlugin(t, echoPlugin)
	out, err := testCallbackRoundtrip(h, 9, []byte("async"), true)
	if err != nil {
		t.Fatalf("async roundtrip failed: %v", err)
	}
	if want := []byte("m9:async"); !bytes.Equal(out, want) {
		t.Fatalf("async roundtrip = %q, want %q", out, want)
	}
}

func TestCallbackEmptyPayloadAndResponse(t *testing.T) {
	gotLen := -1
	h := registerTestPlugin(t, func(method int32, payload []byte) ([]byte, error) {
		gotLen = len(payload)
		return nil, nil
	})
	out, err := testCallbackRoundtrip(h, 1, nil, false)
	if err != nil {
		t.Fatalf("roundtrip failed: %v", err)
	}
	if out != nil {
		t.Fatalf("empty response = %q, want nil", out)
	}
	if gotLen != 0 {
		t.Fatalf("plugin saw payload of length %d, want 0", gotLen)
	}
}

func TestCallbackMiss(t *testing.T) {
	h := registerTestPlugin(t, func(method int32, payload []byte) ([]byte, error) {
		// Wrapped, to prove the shim classifies with errors.Is.
		return nil, fmt.Errorf("key %q: %w", payload, errPluginMiss)
	})
	out, err := testCallbackRoundtrip(h, 2, []byte("absent-key"), false)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("miss error = %v, want ErrNotFound", err)
	}
	if out != nil {
		t.Fatalf("miss returned payload %q, want nil", out)
	}
}

func TestCallbackErrorMessageCrosses(t *testing.T) {
	h := registerTestPlugin(t, func(method int32, payload []byte) ([]byte, error) {
		return nil, errors.New("kaboom: disk on fire")
	})
	for _, async := range []bool{false, true} {
		_, err := testCallbackRoundtrip(h, 3, []byte("x"), async)
		if !errors.Is(err, ErrInternal) {
			t.Fatalf("async=%v: error = %v, want ErrInternal", async, err)
		}
		if !strings.Contains(err.Error(), "kaboom: disk on fire") {
			t.Fatalf("async=%v: error %q does not carry the plugin message", async, err)
		}
	}
}

func TestCallbackPanicContainment(t *testing.T) {
	h := registerTestPlugin(t, func(method int32, payload []byte) ([]byte, error) {
		panic("plugin exploded")
	})
	for _, async := range []bool{false, true} {
		_, err := testCallbackRoundtrip(h, 4, []byte("x"), async)
		if !errors.Is(err, ErrInternal) {
			t.Fatalf("async=%v: panic surfaced as %v, want ErrInternal", async, err)
		}
		if !strings.Contains(err.Error(), "plugin exploded") {
			t.Fatalf("async=%v: error %q does not carry the panic message", async, err)
		}
	}
	// The process survived and the bridge still works.
	h2 := registerTestPlugin(t, echoPlugin)
	out, err := testCallbackRoundtrip(h2, 5, []byte("still alive"), false)
	if err != nil || !bytes.Equal(out, []byte("m5:still alive")) {
		t.Fatalf("bridge broken after panic: out=%q err=%v", out, err)
	}
}

func TestCallbackHandlesAreIndependent(t *testing.T) {
	h1 := registerTestPlugin(t, func(method int32, payload []byte) ([]byte, error) {
		return []byte("one"), nil
	})
	h2 := registerTestPlugin(t, func(method int32, payload []byte) ([]byte, error) {
		return []byte("two"), nil
	})
	if h1 == h2 {
		t.Fatalf("registry issued duplicate handles: %d", h1)
	}
	if out, err := testCallbackRoundtrip(h1, 0, nil, false); err != nil || string(out) != "one" {
		t.Fatalf("h1 dispatch: out=%q err=%v", out, err)
	}
	if out, err := testCallbackRoundtrip(h2, 0, nil, false); err != nil || string(out) != "two" {
		t.Fatalf("h2 dispatch: out=%q err=%v", out, err)
	}
}

func TestCallbackReleasedHandleFailsCleanly(t *testing.T) {
	h := registerPlugin(funcPlugin(echoPlugin))
	if _, err := testCallbackRoundtrip(h, 1, []byte("x"), false); err != nil {
		t.Fatalf("call before release failed: %v", err)
	}
	releasePlugin(h)
	for _, async := range []bool{false, true} {
		_, err := testCallbackRoundtrip(h, 1, []byte("x"), async)
		if !errors.Is(err, ErrInternal) {
			t.Fatalf("async=%v: stale-handle error = %v, want ErrInternal", async, err)
		}
		if !strings.Contains(err.Error(), "no plugin registered") {
			t.Fatalf("async=%v: stale-handle error %q lacks the registry message", async, err)
		}
	}
	// Double release is a no-op.
	releasePlugin(h)
}

func TestCallbackUnknownHandle(t *testing.T) {
	_, err := testCallbackRoundtrip(0xDEADBEEF, 1, nil, false)
	if !errors.Is(err, ErrInternal) || !strings.Contains(err.Error(), "no plugin registered") {
		t.Fatalf("unknown-handle error = %v, want ErrInternal with registry message", err)
	}
}

func TestCallbackNativeRelease(t *testing.T) {
	h := registerPlugin(funcPlugin(echoPlugin))
	// Rust GoPlugin::release → vtable release slot → go_handle_release.
	testCallbackReleaseFromNative(h)
	if _, ok := lookupPlugin(h); ok {
		t.Fatal("native release did not remove the plugin from the registry")
	}
	if _, err := testCallbackRoundtrip(h, 1, nil, false); err == nil {
		t.Fatal("call after native release succeeded, want error")
	}
	// Releasing again from either side stays a no-op.
	testCallbackReleaseFromNative(h)
	releasePlugin(h)
}

func TestCallbackConcurrency(t *testing.T) {
	const goroutines, calls = 100, 100
	h := registerTestPlugin(t, echoPlugin)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range calls {
				method := int32(i % 17)
				payload := fmt.Appendf(nil, "g%d-i%d", g, i)
				async := (g+i)%4 == 0 // mix in the spawn_blocking path
				out, err := testCallbackRoundtrip(h, method, payload, async)
				if err != nil {
					t.Errorf("g=%d i=%d: roundtrip failed: %v", g, i, err)
					return
				}
				want := append(fmt.Appendf(nil, "m%d:", method), payload...)
				if !bytes.Equal(out, want) {
					t.Errorf("g=%d i=%d: got %q, want %q", g, i, out, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}
