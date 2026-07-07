package ws

import (
	"sync"
	"testing"
	"time"

	"github.com/tent-of-trials/market/types"
	"go.uber.org/zap"
)

func testLogger() *zap.Logger {
	l, _ := zap.NewDevelopment()
	return l
}

func newTestClient(h *Hub, remote string, buf int) *Client {
	return &Client{
		hub:    h,
		send:   make(chan []byte, buf),
		subs:   make(map[types.Symbol]struct{}),
		remote: remote,
	}
}

func startHub(h *Hub) {
	go h.Run()
}

func waitUntil(t *testing.T, fn func() bool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func clientCount(h *Hub) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func TestHub_RegisterAndUnregisterLifecycle(t *testing.T) {
	h := NewHub(testLogger())
	startHub(h)

	c := newTestClient(h, "127.0.0.1:1001", 8)
	h.register <- c
	waitUntil(t, func() bool { return clientCount(h) == 1 }, 200*time.Millisecond)

	h.unregister <- c
	waitUntil(t, func() bool { return clientCount(h) == 0 }, 200*time.Millisecond)

	select {
	case _, ok := <-c.send:
		if ok {
			t.Fatal("send channel should be closed after unregister")
		}
	default:
		t.Fatal("expected closed send channel")
	}
}

func TestHub_BroadcastFanoutToConnectedClients(t *testing.T) {
	h := NewHub(testLogger())
	startHub(h)

	c1 := newTestClient(h, "127.0.0.1:2001", 8)
	c2 := newTestClient(h, "127.0.0.1:2002", 8)
	h.register <- c1
	h.register <- c2
	waitUntil(t, func() bool { return clientCount(h) == 2 }, 200*time.Millisecond)

	msg := []byte(`{"event":"trade"}`)
	h.broadcast <- msg

	received := 0
	deadline := time.After(300 * time.Millisecond)
loop:
	for {
		select {
		case got := <-c1.send:
			if string(got) != string(msg) {
				t.Fatalf("c1 payload=%q", got)
			}
			received++
		case got := <-c2.send:
			if string(got) != string(msg) {
				t.Fatalf("c2 payload=%q", got)
			}
			received++
		case <-deadline:
			break loop
		}
	}
	if received != 2 {
		t.Fatalf("broadcast delivered=%d want 2", received)
	}
}

func TestHub_BroadcastEvictsSlowClient(t *testing.T) {
	h := NewHub(testLogger())
	startHub(h)

	slow := newTestClient(h, "127.0.0.1:slow", 1)
	slow.send <- []byte("prefill")
	fast := newTestClient(h, "127.0.0.1:fast", 8)

	h.register <- slow
	h.register <- fast
	waitUntil(t, func() bool { return clientCount(h) == 2 }, 200*time.Millisecond)

	h.broadcast <- []byte("pressure")
	time.Sleep(50 * time.Millisecond)

	h.mu.RLock()
	_, slowPresent := h.clients[slow]
	_, fastPresent := h.clients[fast]
	h.mu.RUnlock()

	if slowPresent {
		t.Fatal("slow client must be removed during broadcast cleanup")
	}
	if !fastPresent {
		t.Fatal("fast client must remain connected")
	}

	select {
	case _, ok := <-slow.send:
		if ok {
			t.Fatal("slow client send channel should be closed")
		}
	default:
		t.Fatal("expected closed slow send channel")
	}
}

func TestHub_UnregisterUnknownClientIsSafe(t *testing.T) {
	h := NewHub(testLogger())
	startHub(h)

	ghost := newTestClient(h, "127.0.0.1:ghost", 4)
	h.unregister <- ghost
	time.Sleep(30 * time.Millisecond)
	if clientCount(h) != 0 {
		t.Fatalf("ghost unregister mutated client set: count=%d", clientCount(h))
	}
}

func TestHub_ConcurrentRegisterUnregister(t *testing.T) {
	h := NewHub(testLogger())
	startHub(h)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := newTestClient(h, "127.0.0.1:batch", 4)
			h.register <- c
			time.Sleep(5 * time.Millisecond)
			h.unregister <- c
		}(i)
	}
	wg.Wait()
	time.Sleep(80 * time.Millisecond)
	if n := clientCount(h); n != 0 {
		t.Fatalf("expected empty hub after churn, got %d", n)
	}
}